{-# LANGUAGE BangPatterns          #-}
{-# LANGUAGE CPP                   #-}
{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ParallelListComp      #-}
{-# LANGUAGE PatternGuards         #-}
{-# LANGUAGE ScopedTypeVariables   #-}

module Network.AWS.S3.StreamingUpload
(
    streamUpload
    , UploadLocation(..)
    , concurrentUpload
    , abortAllUploads
    , module Network.AWS.S3.CreateMultipartUpload
    , module Network.AWS.S3.CompleteMultipartUpload
    , chunkSize
) where

import Network.AWS
       ( HasEnv(..)
       , LogLevel(..)
       , MonadAWS
       , getFileSize
       , hashedBody
       , send
       , toBody
       )

import Control.Monad.Trans.AWS (AWSConstraint)
import Network.AWS.Data.Crypto
       (Digest, SHA256, hashFinalize, hashInit, hashUpdate)

import Network.AWS.S3.AbortMultipartUpload
import Network.AWS.S3.CompleteMultipartUpload
import Network.AWS.S3.CreateMultipartUpload
import Network.AWS.S3.ListMultipartUploads
import Network.AWS.S3.Types
       ( BucketName
       , cmuParts
       , completedMultipartUpload
       , completedPart
       , muKey
       , muUploadId
       )
import Network.AWS.S3.UploadPart

import Control.Applicative
import Control.Category             ((>>>))
import Control.Monad                (forM_, when, (>=>))
import Control.Monad.IO.Class       (MonadIO, liftIO)
import Control.Monad.Morph          (lift)
import Control.Monad.Trans.Resource (MonadBaseControl, MonadResource, throwM)

import Data.Conduit      (Sink, await)
import Data.Conduit.List (sourceList)

import           Data.ByteString         (ByteString)
import qualified Data.ByteString         as BS
import           Data.ByteString.Builder (stringUtf8)
import           System.IO.MMap          (mmapFileByteString)

import           Control.DeepSeq    (rnf)
import qualified Data.DList         as D
import           Data.List          (unfoldr)
import           Data.List.NonEmpty (nonEmpty)

import Control.Exception.Lens (catching, handling)
import Control.Lens

import Text.Printf (printf)

import Control.Concurrent.Async.Lifted (forConcurrently)
import System.Mem                      (performGC)

-- | Minimum size of data which will be sent in a single part, currently 6MB
chunkSize :: Int
chunkSize = 6*1024*1024 -- Making this 5MB+1 seemed to cause AWS to complain


{- |
Given a 'CreateMultipartUpload', creates a 'Sink' which will sequentially
upload the data streamed in in chunks of at least 'chunkSize' and return
the 'CompleteMultipartUploadResponse'.

If uploading of any parts fails, an attempt is made to abort the Multipart
upload, but it is likely that if an upload fails this abort may also fail.
'Network.AWS.S3.ListMultipartUploads' can be used to list any pending
uploads - it is important to abort multipart uploads because you will
be charged for storage of the parts until it is completed or aborted.
See the AWS documentation for more details.

May throw 'Network.AWS.Error'
-}
streamUpload :: (MonadResource m, MonadAWS m, AWSConstraint r m)
             => CreateMultipartUpload
             -> Sink ByteString m CompleteMultipartUploadResponse
streamUpload cmu = do
  logger <- lift $ view envLogger
  let logStr :: MonadIO m => String -> m ()
      logStr = liftIO . logger Info . stringUtf8

  cmur <- lift $ send cmu
  when (cmur ^. cmursResponseStatus /= 200) $
    fail "Failed to create upload"

  logStr "\n**** Created upload\n"

  let Just upId = cmur ^. cmursUploadId
      bucket    = cmu  ^. cmuBucket
      key       = cmu  ^. cmuKey
      -- go :: Text -> Builder -> Int -> Int -> Sink ByteString m ()
      go !bss !bufsize !ctx !partnum !completed = Data.Conduit.await >>= \mbs -> case mbs of
        Just bs | l <- BS.length bs
                , bufsize + l <= chunkSize ->
                    go (D.snoc bss bs) (bufsize + l) (hashUpdate ctx bs) partnum completed

                | otherwise -> do
                    rs <- lift $ partUploader partnum (bufsize + BS.length bs)
                                              (hashFinalize $ hashUpdate ctx bs)
                                              (D.snoc bss bs)

                    logStr $ printf "\n**** Uploaded part %d size $d\n" partnum bufsize
                    let part = completedPart partnum <$> (rs ^. uprsETag)
#if MIN_VERSION_amazonka_s3(1,4,1)
                        !_ = rnf part
#endif
                    liftIO performGC
                    go empty 0 hashInit (partnum+1) . D.snoc completed $! part

        Nothing -> lift $ do
            rs <- partUploader partnum bufsize (hashFinalize ctx) bss

            logStr $ printf "\n**** Uploaded (final) part %d size $d\n" partnum bufsize

            let allParts = D.toList $ D.snoc completed $ completedPart partnum <$> (rs ^. uprsETag)
                prts = nonEmpty =<< sequence allParts

            send $ completeMultipartUpload bucket key upId
                    & cMultipartUpload ?~ set cmuParts prts completedMultipartUpload


      partUploader :: MonadAWS m => Int -> Int -> Digest SHA256 -> D.DList ByteString -> m UploadPartResponse
      partUploader pnum size digest =
        D.toList
        >>> sourceList
        >>> hashedBody digest (fromIntegral size)
        >>> toBody
        >>> uploadPart bucket key pnum upId
        >>> send
        >=> checkUpload

      checkUpload :: (Monad m) => UploadPartResponse -> m UploadPartResponse
      checkUpload upr = do
        when (upr ^. uprsResponseStatus /= 200) $ fail "Failed to upload piece"
        return upr

  catching id (go D.empty 0 hashInit 1 D.empty) $ \e ->
      lift (send (abortMultipartUpload bucket key upId)) >> throwM e
      -- Whatever happens, we abort the upload and rethrow


-- | Specifies whether to upload a file or 'ByteString
data UploadLocation
    = FP FilePath -- ^ A file to be uploaded
    | BS ByteString -- ^ A strict 'ByteString'

 --  IO (Int -> IO (Maybe ByteString)) (Either (IO ()) (IO ()))
        -- part number as input, may be called many times until Nothing is returned,
        -- and a function to close either this part or all parts

{-|
Allows a file or 'ByteString' to be uploaded concurrently, using the
async library.  'ByteString's are split into 'chunkSize' chunks
and uploaded directly.

Files are mmapped into 'chunkSize' chunks and each chunk is uploaded in parallel.
This considerably reduces the memory necessary compared to reading the contents
into memory as a strict 'ByteString'. The usual caveats about mmaped files apply:
if the file is modified during this operation, the data may become corrupt.

May throw `Network.AWS.Error`, or `IOError`.
-}
concurrentUpload :: (MonadAWS m, MonadBaseControl IO m)
                 => UploadLocation -> CreateMultipartUpload -> m CompleteMultipartUploadResponse
concurrentUpload ud cmu = do
    cmur <- send cmu
    when (cmur ^. cmursResponseStatus /= 200) $
        fail "Failed to create upload"
    let Just upId = cmur ^. cmursUploadId
        bucket    = cmu  ^. cmuBucket
        key       = cmu  ^. cmuKey
        -- hndlr :: SomeException -> m CompleteMultipartUploadResponse
        hndlr e = send (abortMultipartUpload bucket key upId) >> throwM e

    handling id hndlr $ do
        umrs <- case ud of
            BS bs -> forConcurrently (zip [1..] $ chunksOf chunkSize bs) $ \(partnum, b) -> do
                    umr <- send . uploadPart bucket key partnum upId . toBody $ b
                    pure $ completedPart partnum <$> (umr ^. uprsETag)

            FP fp -> do
                fsize <- liftIO $ getFileSize fp
                let (count,lst) = divMod (fromIntegral fsize) chunkSize
                    params = [(partnum, chunkSize*offset, size)
                            | partnum <- [1..]
                            | offset  <- [0..count]
                            | size    <- (chunkSize <$ [0..count-1]) ++ [lst]
                            ]

                forConcurrently params $ \(partnum,off,size) -> do
                    b <- liftIO $ mmapFileByteString fp (Just (fromIntegral off,size))
                    umr <- send . uploadPart bucket key partnum upId . toBody $ b
                    pure $ completedPart partnum <$> (umr ^. uprsETag)

        let prts = nonEmpty =<< sequence umrs
        send $ completeMultipartUpload bucket key upId
                & cMultipartUpload ?~ set cmuParts prts completedMultipartUpload

-- | Aborts all uploads in a given bucket - useful for cleaning up.
abortAllUploads :: (MonadAWS m) => BucketName -> m ()
abortAllUploads bucket = do
  rs <- send (listMultipartUploads bucket)
  forM_ (rs ^. lmursUploads) $ \mu -> do
    let mki = (,) <$> mu ^. muKey <*> mu ^. muUploadId
    case mki of
      Nothing        -> pure ()
      Just (key,uid) -> send (abortMultipartUpload bucket key uid) >> pure ()



-- http://stackoverflow.com/questions/32826539/chunksof-analog-for-bytestring
justWhen :: (a -> Bool) -> (a -> b) -> a -> Maybe b
justWhen f g a = if f a then Just (g a) else Nothing

nothingWhen :: (a -> Bool) -> (a -> b) -> a -> Maybe b
nothingWhen f = justWhen (not . f)

chunksOf :: Int -> BS.ByteString -> [BS.ByteString]
chunksOf x = unfoldr (nothingWhen BS.null (BS.splitAt x))
