{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ParallelListComp #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module Network.AWS.S3.StreamingUpload
  ( streamUpload
  , ChunkSize
  , minimumChunkSize
  , NumThreads
  , concurrentUpload
  , UploadLocation(..)
  , abortAllUploads
  , module Network.AWS.S3.CreateMultipartUpload
  , module Network.AWS.S3.CompleteMultipartUpload
  ) where

import Network.AWS
       ( AWS, HasEnv(..), LogLevel(..), MonadAWS, getFileSize, hashedBody, hashedFileRange,
       liftAWS, runAWS, runResourceT, send, toBody )

import Network.AWS.Data.Crypto ( Digest, SHA256, hashFinalize, hashInit, hashUpdate )

import Network.AWS.S3.AbortMultipartUpload
import Network.AWS.S3.CompleteMultipartUpload
import Network.AWS.S3.CreateMultipartUpload
import Network.AWS.S3.ListMultipartUploads
import Network.AWS.S3.Types
       ( BucketName, cmuParts, completedMultipartUpload, completedPart, muKey, muUploadId )
import Network.AWS.S3.UploadPart

import Control.Applicative
import Control.Category           ( (>>>) )
import Control.Monad              ( forM_, when, (>=>) )
import Control.Monad.Fail         ( MonadFail )
import Control.Monad.IO.Class     ( MonadIO, liftIO )
import Control.Monad.Morph        ( lift )
import Control.Monad.Reader.Class ( local )

import Conduit           ( MonadUnliftIO(..) )
import Data.Conduit      ( ConduitT, Void, await, catchC )
import Data.Conduit.List ( sourceList )

import           Data.ByteString         ( ByteString )
import qualified Data.ByteString         as BS
import           Data.ByteString.Builder ( stringUtf8 )

import qualified Data.DList         as D
import           Data.List          ( unfoldr )
import           Data.List.NonEmpty ( nonEmpty )

import Control.Lens           ( set, view )
import Control.Lens.Operators

import Text.Printf ( printf )

import Control.Concurrent       ( newQSem, signalQSem, waitQSem )
import Control.Concurrent.Async ( forConcurrently )
import Control.Exception.Base   ( SomeException, bracket_ )
import Control.Monad.Catch      ( onException )
import System.Mem               ( performGC )

import Network.HTTP.Client ( defaultManagerSettings, managerConnCount, newManager )

type ChunkSize = Int
type NumThreads = Int

-- | Minimum size of data which will be sent in a single part, currently 6MB
minimumChunkSize :: ChunkSize
minimumChunkSize = 6*1024*1024 -- Making this 5MB+1 seemed to cause AWS to complain


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
streamUpload :: (MonadUnliftIO m, MonadAWS m, MonadFail m)
             => Maybe ChunkSize -- ^ Optional chunk size
             -> CreateMultipartUpload -- ^ Upload location
             -> ConduitT ByteString Void m (Either (AbortMultipartUploadResponse, SomeException) CompleteMultipartUploadResponse)
streamUpload mChunkSize multiPartUploadDesc = do
  logger <- lift $ liftAWS $ view envLogger
  let logStr :: MonadIO m => String -> m ()
      logStr = liftIO . logger Debug . stringUtf8
      chunkSize = maybe minimumChunkSize (max minimumChunkSize) mChunkSize

  multiPartUpload <- lift $ send multiPartUploadDesc
  when (multiPartUpload ^. cmursResponseStatus /= 200) $
    fail "Failed to create upload"

  logStr "\n**** Created upload\n"

  let Just upId = multiPartUpload ^. cmursUploadId
      bucket    = multiPartUploadDesc  ^. cmuBucket
      key       = multiPartUploadDesc  ^. cmuKey
      -- go :: DList ByteString -> Int -> Context SHA256 -> Int -> DList (Maybe CompletedPart) -> Sink ByteString m ()
      go !bss !bufsize !ctx !partnum !completed = await >>= \case
        Just bs
            | l <- BS.length bs, bufsize + l <= chunkSize
                -> go (D.snoc bss bs) (bufsize + l) (hashUpdate ctx bs) partnum completed
            | otherwise -> do
                rs <- lift $ performUpload partnum (bufsize + BS.length bs)
                                            (hashFinalize $ hashUpdate ctx bs)
                                            (D.snoc bss bs)

                logStr $ printf "\n**** Uploaded part %d size %d\n" partnum bufsize
                let !part = completedPart partnum <$> (rs ^. uprsETag)
                liftIO performGC
                go empty 0 hashInit (partnum+1) . D.snoc completed $! part

        Nothing -> lift $ do
            prts <- if bufsize > 0
                then do
                    rs <- performUpload partnum bufsize (hashFinalize ctx) bss
                    logStr $ printf "\n**** Uploaded (final) part %d size %d\n" partnum bufsize
                    let allParts = D.toList $ D.snoc completed $ completedPart partnum <$> (rs ^. uprsETag)
                    pure $ nonEmpty =<< sequence allParts
                else do
                    logStr $ printf "\n**** No final data to upload\n"
                    pure $ nonEmpty =<< sequence (D.toList completed)

            send $ completeMultipartUpload bucket key upId
                    & cMultipartUpload ?~ set cmuParts prts completedMultipartUpload


      performUpload :: (MonadAWS m, MonadFail m) => Int -> Int -> Digest SHA256 -> D.DList ByteString -> m UploadPartResponse
      performUpload pnum size digest =
        D.toList
        >>> sourceList
        >>> hashedBody digest (fromIntegral size)
        >>> toBody
        >>> uploadPart bucket key pnum upId
        >>> send
        >=> checkUpload

      checkUpload :: (Monad m, MonadFail m) => UploadPartResponse -> m UploadPartResponse
      checkUpload upr = do
        when (upr ^. uprsResponseStatus /= 200) $ fail "Failed to upload piece"
        return upr

  (Right <$> go D.empty 0 hashInit 1 D.empty) `catchC` \(except :: SomeException) ->
    Left . (,except) <$> lift (send (abortMultipartUpload bucket key upId))
      -- Whatever happens, we abort the upload and return the exception


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
concurrentUpload :: (MonadAWS m, MonadFail m)
                 => Maybe ChunkSize -- ^ Optional chunk size
                 -> Maybe NumThreads -- ^ Optional number of threads to upload with
                 -> UploadLocation -- ^ Whether to upload a file on disk or a `ByteString` that's already in memory.
                 -> CreateMultipartUpload -- ^ Description of where to upload.
                 -> m CompleteMultipartUploadResponse
concurrentUpload mChunkSize mNumThreads uploadLoc multiPartUploadDesc = do
  env <- liftAWS $ view environment
  cmur <- send multiPartUploadDesc
  when (cmur ^. cmursResponseStatus /= 200) $
      fail "Failed to create upload"

  let logStr :: MonadIO m => String -> m ()
      logStr    = liftIO . (env ^. envLogger) Info . stringUtf8
      bucket    = multiPartUploadDesc  ^. cmuBucket
      key       = multiPartUploadDesc  ^. cmuKey
      Just upId = cmur ^. cmursUploadId

      calculateChunkSize :: Int -> Int
      calculateChunkSize len =
          let chunkSize' = maybe minimumChunkSize (max minimumChunkSize) mChunkSize
          in if len `div` chunkSize' >= 10000 then len `div` 9999 else chunkSize'

      mConnCount = managerConnCount defaultManagerSettings
      nThreads   = maybe mConnCount (max 1) mNumThreads

      exec :: MonadAWS m => AWS a -> m a
      exec act = if maybe False (> mConnCount) mNumThreads
              then do
                  mgr' <- liftIO $ newManager  defaultManagerSettings{managerConnCount = nThreads}
                  liftAWS $ local (envManager .~ mgr') act
              else liftAWS act
  exec $ flip onException (send (abortMultipartUpload bucket key upId)) $ do
      sem <- liftIO $ newQSem nThreads
      uploadResponses <- case uploadLoc of
          BS bytes ->
            let chunkSize = calculateChunkSize $ BS.length bytes
            in liftIO $ forConcurrently (zip [1..] $ chunksOf chunkSize bytes) $ \(partnum, chunk) ->
                bracket_ (waitQSem sem) (signalQSem sem) $ do
                  logStr $ "Starting part: " ++ show partnum
                  umr <- runResourceT $ runAWS env $ send . uploadPart bucket key partnum upId . toBody $ chunk
                  logStr $ "Finished part: " ++ show partnum
                  pure $ completedPart partnum <$> (umr ^. uprsETag)

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
                uploadResp <- runResourceT $ runAWS env $
                  send . uploadPart bucket key partnum upId . toBody $ chunkStream
                logStr $ "Finished file part: " ++ show partnum
                pure $ completedPart partnum <$> (uploadResp ^. uprsETag)

      let parts = nonEmpty =<< sequence uploadResponses
      send $ completeMultipartUpload bucket key upId
              & cMultipartUpload ?~ set cmuParts parts completedMultipartUpload

-- | Aborts all uploads in a given bucket - useful for cleaning up.
abortAllUploads :: (MonadAWS m) => BucketName -> m ()
abortAllUploads bucket = do
  rs <- send (listMultipartUploads bucket)
  forM_ (rs ^. lmursUploads) $ \mu -> do
    let mki = (,) <$> mu ^. muKey <*> mu ^. muUploadId
    forM_ mki $ \(key,uid) -> send (abortMultipartUpload bucket key uid)



-- http://stackoverflow.com/questions/32826539/chunksof-analog-for-bytestring
justWhen :: (a -> Bool) -> (a -> b) -> a -> Maybe b
justWhen f g a = if f a then Just (g a) else Nothing

nothingWhen :: (a -> Bool) -> (a -> b) -> a -> Maybe b
nothingWhen f = justWhen (not . f)

chunksOf :: Int -> BS.ByteString -> [BS.ByteString]
chunksOf x = unfoldr (nothingWhen BS.null (BS.splitAt x))
