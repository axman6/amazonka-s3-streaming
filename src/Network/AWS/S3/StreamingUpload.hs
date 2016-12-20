{-# LANGUAGE BangPatterns          #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PatternGuards         #-}
{-# LANGUAGE ScopedTypeVariables   #-}

module Network.AWS.S3.StreamingUpload where

import           Network.AWS                            (AsError (..),
                                                         HasEnv (..),
                                                         LogLevel (..),
                                                         MonadAWS, send, toBody)
import           Network.AWS.S3.AbortMultipartUpload
import           Network.AWS.S3.CompleteMultipartUpload
import           Network.AWS.S3.CreateMultipartUpload
import           Network.AWS.S3.Types                   (BucketName,
                                                         CompletedPart,
                                                         ObjectKey, cmuParts,
                                                         completedMultipartUpload,
                                                         completedPart)
import           Network.AWS.S3.UploadPart

import           Control.Monad                          (when)
import           Control.Monad.Catch
import           Control.Monad.IO.Class
import           Control.Monad.Morph
import           Control.Monad.Reader.Class
import           Control.Monad.Trans.Resource

import           Data.Conduit
import           Data.Conduit.Binary                    (sourceFile)

import           Data.ByteString                        (ByteString)
import qualified Data.ByteString                        as BS
import           Data.ByteString.Builder
import           Data.Monoid                            (mempty, (<>))
-- import           Data.Text                              (Text)

import           Control.Applicative

import           Control.Exception.Lens
import           Control.Lens

import           Data.List.NonEmpty                     as NE (NonEmpty,
                                                               nonEmpty)


streamUpload :: (MonadResource m, MonadAWS m, HasEnv r, MonadReader r m, Applicative m) -- , AsStreamingUploadError e, MonadError e m)
             => CreateMultipartUpload
             -> Sink ByteString m CompleteMultipartUploadResponse
streamUpload cmu = do
  logger <- lift $ view envLogger
  let logStr :: MonadIO m => String -> m ()
      logStr = liftIO . logger Info . stringUtf8

  cmur <- lift (send cmu)
  when (cmur ^. cmursResponseStatus /= 200) $
    fail "Failed to create upload"

  logStr "\n**** Created upload\n"

  let Just upId = cmur ^. cmursUploadId
      bucket    = cmu  ^. cmuBucket
      key       = cmu  ^. cmuKey
      -- go :: Text -> Builder -> Int -> Int -> Sink ByteString m ()
      go !bu !bufsize !partnum !completed = Data.Conduit.await >>= \mbs -> case mbs of
        Just bs | l <- BS.length bs
                , bufsize + l <= 6*1024*1024 -> -- Making this 5MB+1 seemed to cause AWS to complain
                    go (bu <> byteString bs) (bufsize + l) partnum completed

                | otherwise -> do
                    rs <- lift $ checkUpload =<< partUploader partnum (bu <> byteString bs)

                    logStr $ concat ["\n**** Uploaded part ", show partnum
                                    ," size: ", show bufsize,"\n"]
                    go mempty 0 (partnum+1) $ (completedPart partnum <$> (rs ^. uprsETag)) : completed

        Nothing -> lift $ do
            rs <- checkUpload =<< partUploader partnum bu

            logStr $ concat ["\n**** Uploaded (final) part ", show partnum
                            ," size: ", show bufsize,"\n"]

            let allParts = (completedPart partnum <$> (rs ^. uprsETag)) : completed
                -- Parts must be in ascending order
                prts = NE.nonEmpty =<< reverse <$> sequence allParts

            send $ completeMultipartUpload bucket key upId
                    & cMultipartUpload ?~ set cmuParts prts completedMultipartUpload


      partUploader :: MonadAWS m => Int -> Builder -> m UploadPartResponse
      partUploader pnum = send . uploadPart bucket key pnum upId . toBody . toLazyByteString

      checkUpload :: (Monad m, Applicative m) => UploadPartResponse -> m UploadPartResponse
      checkUpload upr = do
        when (upr ^. uprsResponseStatus /= 200) $ fail "Failed to upload piece"
        pure upr

  catching id (go mempty 0 1 []) $ \e -> do
      -- Whatever happens, we abort the upload and rethrow
      lift $ send (abortMultipartUpload bucket key upId)
      throwM e


uploadFile :: (MonadAWS m, HasEnv r, MonadReader r m, MonadResource m)
           => BucketName -> ObjectKey -> FilePath
           -> m CompleteMultipartUploadResponse
uploadFile bucket key filepath =
  sourceFile filepath $$ streamUpload (createMultipartUpload bucket key)

-- TODO: This can be more efficient/concurrent if we know we're reading from a file

-- newtype UploadId = UploadId Text

-- data S3Upload m
--     = Feed UploadId (ByteString -> m (S3Upload m))
--     | Done
