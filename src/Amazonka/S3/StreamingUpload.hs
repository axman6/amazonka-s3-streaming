{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ParallelListComp #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE NamedFieldPuns #-}


module Amazonka.S3.StreamingUpload
  ( streamUpload
  , ChunkSize
  , minimumChunkSize
  , NumThreads
  , concurrentUpload
  , UploadLocation(..)
  , abortAllUploads
  , module Amazonka.S3.CreateMultipartUpload
  , module Amazonka.S3.CompleteMultipartUpload
  ) where

import Amazonka
       ( LogLevel(..), HashedBody(..), getFileSize, hashedFileRange, toBody, send )
import Amazonka.Env (Env, Env'(envLogger, envManager))
import Amazonka.Crypto ( hash )

import Amazonka.S3.AbortMultipartUpload    ( AbortMultipartUploadResponse, newAbortMultipartUpload )
import Amazonka.S3.CompleteMultipartUpload
import Amazonka.S3.CreateMultipartUpload
import Amazonka.S3.ListMultipartUploads    ( newListMultipartUploads, uploads )
import Amazonka.S3.Types
       ( BucketName, CompletedPart, MultipartUpload(..), newCompletedPart)
import Amazonka.S3.Types.CompletedMultipartUpload
import Amazonka.S3.UploadPart ( UploadPartResponse(..), newUploadPart )

import Network.HTTP.Client ( managerConnCount, newManager )
import Network.HTTP.Client.TLS ( tlsManagerSettings )

import Control.Monad              ( forM_ )
import Control.Monad.Catch        ( Exception, onException, MonadCatch )
import Control.Monad.IO.Class     ( MonadIO, liftIO )
import Control.Monad.Trans.Class  ( lift )
import Control.Monad.Trans.Resource (MonadResource, runResourceT)

import Conduit                  ( MonadUnliftIO(..) )
import Data.Conduit             ( ConduitT, Void, await, handleC, yield, (.|) )
import Data.Conduit.Combinators ( sinkList )
import qualified Data.Conduit.Combinators as CC

import Data.ByteString               ( ByteString )
import qualified Data.ByteString as BS
import Data.ByteString.Builder       ( Builder, stringUtf8 )
import Data.ByteString.Builder.Extra ( Next(..), byteStringCopy, runBuilder )
import Data.List                     ( unfoldr )
import Data.List.NonEmpty            ( fromList, nonEmpty )
import Data.Text                     ( Text )

import Control.Concurrent       ( newQSem, signalQSem, waitQSem )
import Control.Concurrent.Async ( forConcurrently )
import Control.Exception.Base   ( SomeException, bracket_ )

import qualified Data.ByteString as B
import Data.ByteString.Internal  ( ByteString(PS) )
import Foreign.ForeignPtr        ( mallocForeignPtrBytes )
import Foreign.ForeignPtr.Unsafe ( unsafeForeignPtrToPtr )
import GHC.ForeignPtr            ( finalizeForeignPtr )

import Control.DeepSeq         ( rwhnf, (<$!!>) )
import Data.Typeable           ( Typeable )


type ChunkSize = Int
type NumThreads = Int

-- | Minimum size of data which will be sent in a single part, currently 6MB
minimumChunkSize :: ChunkSize
minimumChunkSize = 6*1024*1024 -- Making this 5MB+1 seemed to cause AWS to complain


data StreamingError
    = UnableToCreateMultipartUpload CreateMultipartUploadResponse
    | FailedToUploadPiece UploadPartResponse
    | Other String
  deriving stock (Show, Eq, Typeable)

instance Exception StreamingError



{- |
Given a 'CreateMultipartUpload', creates a 'Sink' which will sequentially
upload the data streamed in in chunks of at least 'minimumChunkSize' and return either
the 'CompleteMultipartUploadResponse', or if an exception is thrown,
`AbortMultipartUploadResponse` and the exception as `SomeException`. If aborting
the upload also fails then the exception caused by the call to abort will be thrown.

'Network.AWS.S3.ListMultipartUploads' can be used to list any pending
uploads - it is important to abort multipart uploads because you will
be charged for storage of the parts until it is completed or aborted.
See the AWS documentation for more details.

May throw 'Network.AWS.Error'
-}
streamUpload :: forall m. (MonadUnliftIO m, MonadResource m)
             => Env
             -> Maybe ChunkSize -- ^ Optional chunk size
             -> CreateMultipartUpload -- ^ Upload location
             -> ConduitT ByteString Void m (Either (AbortMultipartUploadResponse, SomeException) CompleteMultipartUploadResponse)
streamUpload env mChunkSize multiPartUploadDesc@CreateMultipartUpload'{bucket = buck, key = k} =
  processAndChunkOutputRaw chunkSize
  .| enumerateConduit
  .| startUpload
  where
    chunkSize :: ChunkSize
    chunkSize = maybe minimumChunkSize (max minimumChunkSize) mChunkSize

    logStr :: String -> m ()
    logStr msg  = do
      let logger = envLogger env
      liftIO $ logger Debug $ stringUtf8 msg

    startUpload :: ConduitT (Int, S) Void m
                    (Either (AbortMultipartUploadResponse, SomeException)
                    CompleteMultipartUploadResponse)
    startUpload = do
      CreateMultipartUploadResponse'{uploadId = upId} <- lift $ send env multiPartUploadDesc
      lift $ logStr "\n**** Created upload\n"

      handleC (cancelMultiUploadConduit upId) $
        CC.mapM (multiUpload upId)
        .| finishMultiUploadConduit upId

    multiUpload :: Text -> (Int, S) -> m (Maybe CompletedPart)
    multiUpload upId (partnum, s) = do
      buffer@(PS fptr _ _) <- liftIO $ finaliseS s
      UploadPartResponse'{eTag} <- send env $! newUploadPart buck k partnum upId $! toBody $! (HashedBytes $! hash buffer) buffer
      let !_ = rwhnf eTag
      liftIO $ finalizeForeignPtr fptr
      logStr $ "\n**** Uploaded part " <> show partnum
      return $! newCompletedPart partnum <$!!> eTag

    -- collect all the parts
    finishMultiUploadConduit :: Text
                             -> ConduitT (Maybe CompletedPart) Void m
                                  (Either (AbortMultipartUploadResponse, SomeException) CompleteMultipartUploadResponse)
    finishMultiUploadConduit upId = do
      parts <- sinkList
      res <- lift $ send env $ (newCompleteMultipartUpload buck k upId)
               { multipartUpload =
                  Just $ newCompletedMultipartUpload {parts = sequenceA $ fromList parts}
               }

      return $ Right res

    -- in case of an exception, return Left
    cancelMultiUploadConduit :: Text -> SomeException
                             -> ConduitT i Void m
                                  (Either (AbortMultipartUploadResponse, SomeException) CompleteMultipartUploadResponse)
    cancelMultiUploadConduit upId exc = do
      res <- lift $ send env $ newAbortMultipartUpload buck k upId
      return $ Left (res, exc)

    -- count from 1
    enumerateConduit :: ConduitT a (Int, a) m ()
    enumerateConduit = loop 1
      where
        loop i = await >>= maybe (return ()) (go i)
        go i x = do
          yield (i, x)
          loop (i + 1)
    {-# INLINE enumerateConduit #-}

-- | Specifies whether to upload a file or 'ByteString
data UploadLocation
    = FP FilePath -- ^ A file to be uploaded
    | BS ByteString -- ^ A strict 'ByteString'

{-|
Allows a file or 'ByteString' to be uploaded concurrently, using the
async library.  The chunk size may optionally be specified, but will be at least
`minimumChunkSize`, and may be made larger than if the `ByteString` or file
is larger enough to cause more than 10,000 chunks.

Files are mmapped into 'chunkSize' chunks and each chunk is uploaded in parallel.
This considerably reduces the memory necessary compared to reading the contents
into memory as a strict 'ByteString'. The usual caveats about mmaped files apply:
if the file is modified during this operation, the data may become corrupt.

May throw `Network.AWS.Error`, or `IOError`; an attempt is made to cancel the
multipart upload on any error, but this may also fail if, for example, the network
connection has been broken. See `abortAllUploads` for a crude cleanup method.
-}
concurrentUpload :: forall m.
  (MonadResource m, MonadCatch m)
  => Env
  -> Maybe ChunkSize -- ^ Optional chunk size
  -> Maybe NumThreads -- ^ Optional number of threads to upload with
  -> UploadLocation -- ^ Whether to upload a file on disk or a `ByteString` that's already in memory.
  -> CreateMultipartUpload -- ^ Description of where to upload.
  -> m CompleteMultipartUploadResponse
concurrentUpload env' mChunkSize mNumThreads uploadLoc
                 multiPartUploadDesc@CreateMultipartUpload'{bucket = buck, key = k}
  = do
  CreateMultipartUploadResponse'{uploadId = upId} <- send env' multiPartUploadDesc

  let logStr :: MonadIO n => String -> n ()
      logStr = liftIO . envLogger env' Info . stringUtf8

      calculateChunkSize :: Int -> Int
      calculateChunkSize len =
          let chunkSize' = maybe minimumChunkSize (max minimumChunkSize) mChunkSize
          in if len `div` chunkSize' >= 10000 then len `div` 9999 else chunkSize'

      mConnCount = managerConnCount tlsManagerSettings
      nThreads   = maybe mConnCount (max 1) mNumThreads

  env <- if maybe False (> mConnCount) mNumThreads
              then do
                  mgr' <- liftIO $ newManager tlsManagerSettings{managerConnCount = nThreads}
                  pure env'{envManager = mgr'}
              else pure env'
  flip onException (send env (newAbortMultipartUpload buck k upId)) $ do
      sem <- liftIO $ newQSem nThreads
      uploadResponses <- case uploadLoc of
          BS bytes ->
            let chunkSize = calculateChunkSize $ BS.length bytes
            in liftIO $ forConcurrently (zip [1..] $ chunksOf chunkSize bytes) $ \(partnum, chunk) ->
                bracket_ (waitQSem sem) (signalQSem sem) $ do
                  logStr $ "Starting part: " ++ show partnum
                  UploadPartResponse'{eTag} <- runResourceT $ send env . newUploadPart buck k partnum upId . toBody $ chunk
                  logStr $ "Finished part: " ++ show partnum
                  pure $ newCompletedPart partnum <$> eTag

          FP filePath -> do
            fsize <- liftIO $ getFileSize filePath
            let chunkSize = calculateChunkSize $ fromIntegral fsize
                (count,lst) = fromIntegral fsize `divMod` chunkSize
                params = [(partnum, chunkSize*offset, size)
                          | partnum <- [1..]
                          | offset  <- [0..count]
                          | size    <- (chunkSize <$ [0..count-1]) ++ [lst]
                          ]

            liftIO $ forConcurrently params $ \(partnum,off,size) ->
              bracket_ (waitQSem sem) (signalQSem sem) $ do
                logStr $ "Starting file part: " ++ show partnum
                chunkStream <- hashedFileRange filePath (fromIntegral off) (fromIntegral size)
                UploadPartResponse'{eTag} <- runResourceT $
                  send env . newUploadPart buck k partnum upId . toBody $ chunkStream
                logStr $ "Finished file part: " ++ show partnum
                pure $ newCompletedPart partnum <$> eTag

      let parts = nonEmpty =<< sequence uploadResponses
      send env $ (newCompleteMultipartUpload buck k upId)
                  { multipartUpload = Just $ newCompletedMultipartUpload { parts } }

-- | Aborts all uploads in a given bucket - useful for cleaning up.
abortAllUploads :: MonadResource m => Env -> BucketName -> m ()
abortAllUploads env buck = do
  rs <- send env (newListMultipartUploads buck)
  forM_ (uploads rs) $ \ups ->
    forM_ ups $ \MultipartUpload'{key,uploadId} -> do
      let mki = (,) <$> key <*> uploadId
      forM_ mki $ \(key',uid) -> send env (newAbortMultipartUpload buck key' uid)



-- http://stackoverflow.com/questions/32826539/chunksof-analog-for-bytestring
justWhen :: (a -> Bool) -> (a -> b) -> a -> Maybe b
justWhen f g a = if f a then Just (g a) else Nothing

nothingWhen :: (a -> Bool) -> (a -> b) -> a -> Maybe b
nothingWhen f = justWhen (not . f)

chunksOf :: Int -> BS.ByteString -> [BS.ByteString]
chunksOf x = unfoldr (nothingWhen BS.null (BS.splitAt x))

-- | A bytestring `Builder` stored with the size of buffer it needs to be fully evaluated.
data S = S !Builder {-# UNPACK #-} !Int

newS :: S
newS = S mempty 0

newSFrom :: ByteString -> S
newSFrom bs = S (byteStringCopy bs) (B.length bs)

appendS :: S -> ByteString -> S
appendS (S builder len) bs = S (builder <> byteStringCopy bs) (len + B.length bs)

finaliseS :: S -> IO ByteString
finaliseS (S builder builderLen) = do
  fptr <- mallocForeignPtrBytes builderLen
  let ptr = unsafeForeignPtrToPtr fptr
      bufWriter = runBuilder builder
  bufWriter ptr builderLen >>= \case
    (written, Done)
      | written == builderLen -> pure $! PS fptr 0 builderLen
      | otherwise ->
          error $ "finaliseS: bytes written didn't match, expected: " <> show builderLen <> " got: " <> show written
    (_written, _) -> error "Something went very wrong"

-- Right means the buffer needs more data to fill it
-- Left means the buffer is full
processChunk :: ChunkSize -> ByteString -> S -> IO (Either S S)
processChunk chunkSize input s@(S _ builderLen)
  | builderLen >= chunkSize = pure $! Left $! s
  | otherwise               = pure $! Right $! appendS s input

processAndChunkOutputRaw :: MonadIO m => ChunkSize -> ConduitT ByteString S m ()
processAndChunkOutputRaw chunkSize = loop newS where
  loop !s = await >>=
    maybe (yield s)
          (\bs -> liftIO (processChunk chunkSize bs s) >>= either (\s' -> yield s' >> loop (newSFrom bs)) loop)

