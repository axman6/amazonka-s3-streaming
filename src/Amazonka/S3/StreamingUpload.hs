{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ParallelListComp #-}
{-# LANGUAGE ScopedTypeVariables #-}

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

import Amazonka        ( HashedBody(..), LogLevel(..), getFileSize, hashedFileRange, send, toBody )
import Amazonka.Crypto ( hash )
import Amazonka.Env    ( Env, logger, manager )

import Amazonka.S3.AbortMultipartUpload    ( AbortMultipartUploadResponse, newAbortMultipartUpload )
import Amazonka.S3.CompleteMultipartUpload
       ( CompleteMultipartUpload(..), CompleteMultipartUploadResponse, newCompleteMultipartUpload )
import Amazonka.S3.CreateMultipartUpload
       ( CreateMultipartUpload(..), CreateMultipartUploadResponse(..) )
import Amazonka.S3.ListMultipartUploads
       ( ListMultipartUploadsResponse(..), newListMultipartUploads, uploads )
import Amazonka.S3.Types
       ( BucketName, CompletedMultipartUpload(..), CompletedPart, MultipartUpload(..),
       newCompletedMultipartUpload, newCompletedPart )
import Amazonka.S3.UploadPart              ( UploadPartResponse(..), newUploadPart )

import Network.HTTP.Client     ( managerConnCount, newManager )
import Network.HTTP.Client.TLS ( tlsManagerSettings )

import Control.Monad.Catch          ( Exception, MonadCatch, onException )
import Control.Monad.IO.Class       ( MonadIO, liftIO )
import Control.Monad.Trans.Class    ( lift )
import Control.Monad.Trans.Resource ( MonadResource, runResourceT )

import Conduit                  ( MonadUnliftIO(..) )
import Data.Conduit             ( ConduitT, Void, await, handleC, yield, (.|) )
import Data.Conduit.Combinators ( sinkList )
import Data.Conduit.Combinators qualified as CC

import Data.ByteString               qualified as BS
import Data.ByteString.Builder       ( stringUtf8 )
import Data.ByteString.Builder.Extra ( byteStringCopy, runBuilder )
import Data.ByteString.Internal      ( ByteString(PS) )

import Data.List          ( unfoldr )
import Data.List.NonEmpty ( fromList, nonEmpty )
import Data.Text          ( Text )

import Control.Concurrent       ( newQSem, signalQSem, waitQSem )
import Control.Concurrent.Async ( forConcurrently )
import Control.Exception.Base   ( SomeException, bracket_ )

import Foreign.ForeignPtr        ( ForeignPtr, mallocForeignPtrBytes, plusForeignPtr )
import Foreign.ForeignPtr.Unsafe ( unsafeForeignPtrToPtr )

import Control.DeepSeq ( rwhnf )
import Data.Foldable   ( for_, traverse_ )
import Data.Typeable   ( Typeable )
import Data.Word       ( Word8 )
import Control.Monad   ((>=>))


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

'Amazonka.S3.ListMultipartUploads' can be used to list any pending
uploads - it is important to abort multipart uploads because you will
be charged for storage of the parts until it is completed or aborted.
See the AWS documentation for more details.

Internally, a single @chunkSize@d buffer will be allocated and reused between
requests to avoid holding onto incoming @ByteString@s.

May throw 'Amazonka.Error'
-}
streamUpload :: forall m. (MonadUnliftIO m, MonadResource m)
             => Env
             -> Maybe ChunkSize -- ^ Optional chunk size
             -> CreateMultipartUpload -- ^ Upload location
             -> ConduitT ByteString Void m (Either (AbortMultipartUploadResponse, SomeException) CompleteMultipartUploadResponse)
streamUpload env mChunkSize multiPartUploadDesc@CreateMultipartUpload'{bucket = buck, key = k} = do
  buffer <- liftIO $ allocBuffer chunkSize
  unsafeWriteChunksToBuffer buffer
    .| enumerateConduit
    .| startUpload buffer
  where
    chunkSize :: ChunkSize
    chunkSize = maybe minimumChunkSize (max minimumChunkSize) mChunkSize

    logStr :: String -> m ()
    logStr msg  = do
      liftIO $ logger env Debug $ stringUtf8 msg

    startUpload :: Buffer
                -> ConduitT (Int, BufferResult) Void m
                    (Either (AbortMultipartUploadResponse, SomeException)
                    CompleteMultipartUploadResponse)
    startUpload buffer = do
      CreateMultipartUploadResponse'{uploadId = upId} <- lift $ send env multiPartUploadDesc
      lift $ logStr "\n**** Created upload\n"

      handleC (cancelMultiUploadConduit upId) $
        CC.mapM (multiUpload buffer upId)
        .| finishMultiUploadConduit upId

    multiUpload :: Buffer -> Text -> (Int, BufferResult) -> m (Maybe CompletedPart)
    multiUpload buffer upId (partnum, result) = do
      let !bs = bufferToByteString buffer result
          !bsHash = hash bs
      UploadPartResponse'{eTag} <- send env $! newUploadPart buck k partnum upId $! toBody $! HashedBytes bsHash bs
      let !_ = rwhnf eTag
      logStr $ "\n**** Uploaded part " <> show partnum
      return $! newCompletedPart partnum <$> eTag

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

-- The number of bytes remaining in a buffer, and the pointer that backs it.
data Buffer = Buffer {remaining :: !Int, _fptr :: !(ForeignPtr Word8)}

data PutResult
    = Ok Buffer         -- Didn't fill the buffer, updated buffer.
    | Full ByteString   -- Buffer is full, the unwritten remaining string.

data BufferResult = FullBuffer | Incomplete Int

-- Accepts @ByteString@s and writes them into @Buffer@. When the buffer is full,
-- @FullBuffer@ is emitted. If there is no more input, @Incomplete@ is emitted with
-- the number of bytes remaining in the buffer.
unsafeWriteChunksToBuffer :: MonadIO m => Buffer -> ConduitT ByteString BufferResult m ()
unsafeWriteChunksToBuffer buffer0 = awaitLoop buffer0 where
  awaitLoop buf =
    await >>= maybe (yield $ Incomplete $ remaining buf)
      (liftIO . putBuffer buf >=> \case
        Full next -> yield FullBuffer *> chunkLoop buffer0 next
        Ok buf'   -> awaitLoop buf'
      )
  -- Handle inputs which are larger than the chunkSize
  chunkLoop buf = liftIO . putBuffer buf >=> \case
    Full next -> yield FullBuffer *> chunkLoop buffer0 next
    Ok buf'   -> awaitLoop buf'

bufferToByteString :: Buffer -> BufferResult -> ByteString
bufferToByteString (Buffer bufSize fptr) FullBuffer             = PS fptr 0 bufSize
bufferToByteString (Buffer bufSize fptr) (Incomplete remaining) = PS fptr 0 (bufSize - remaining)

allocBuffer :: Int -> IO Buffer
allocBuffer chunkSize = Buffer chunkSize <$> mallocForeignPtrBytes chunkSize

putBuffer :: Buffer -> ByteString -> IO PutResult
putBuffer buffer bs
  | BS.length bs <= remaining buffer =
      Ok <$> unsafeWriteBuffer buffer bs
  | otherwise = do
      let (remainder,rest) = BS.splitAt (remaining buffer) bs
      Full rest <$ unsafeWriteBuffer buffer remainder

-- The length of the bytestring must be less than or equal to the number
-- of bytes remaining.
unsafeWriteBuffer :: Buffer -> ByteString -> IO Buffer
unsafeWriteBuffer (Buffer remaining fptr) bs = do
    let ptr = unsafeForeignPtrToPtr fptr
        len = BS.length bs
    _ <- runBuilder (byteStringCopy bs) ptr remaining
    pure $ Buffer (remaining - len) (plusForeignPtr fptr len)


-- | Specifies whether to upload a file or 'ByteString'.
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

May throw `Amazonka.Error`, or `IOError`; an attempt is made to cancel the
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
      logStr = liftIO . logger env' Info . stringUtf8

      calculateChunkSize :: Int -> Int
      calculateChunkSize len =
          let chunkSize' = maybe minimumChunkSize (max minimumChunkSize) mChunkSize
          in if len `div` chunkSize' >= 10000 then len `div` 9999 else chunkSize'

      mConnCount = managerConnCount tlsManagerSettings
      nThreads   = maybe mConnCount (max 1) mNumThreads

  env <- if maybe False (> mConnCount) mNumThreads
              then do
                  mgr' <- liftIO $ newManager tlsManagerSettings{managerConnCount = nThreads}
                  pure env'{manager = mgr'}
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
  ListMultipartUploadsResponse' {uploads} <- send env $ newListMultipartUploads buck
  flip (traverse_ . traverse_) uploads $ \MultipartUpload'{key, uploadId} -> do
    let mki = (,) <$> key <*> uploadId
    for_ mki $ \(key',uid) -> send env (newAbortMultipartUpload buck key' uid)



-- http://stackoverflow.com/questions/32826539/chunksof-analog-for-bytestring
justWhen :: (a -> Bool) -> (a -> b) -> a -> Maybe b
justWhen f g a = if f a then Just (g a) else Nothing

nothingWhen :: (a -> Bool) -> (a -> b) -> a -> Maybe b
nothingWhen f = justWhen (not . f)

chunksOf :: Int -> BS.ByteString -> [BS.ByteString]
chunksOf x = unfoldr (nothingWhen BS.null (BS.splitAt x))
