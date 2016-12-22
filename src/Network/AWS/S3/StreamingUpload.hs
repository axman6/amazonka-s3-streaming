{-# LANGUAGE BangPatterns          #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE PatternGuards         #-}
{-# LANGUAGE ScopedTypeVariables   #-}

module Network.AWS.S3.StreamingUpload where

import           Network.AWS                            (AsError (..),
                                                         HasEnv (..),
                                                         LogLevel (..),
                                                         MonadAWS, hashedBody,
                                                         send, toBody)

import           Network.AWS.Data.Crypto                (Digest, SHA256,
                                                         hashFinalize, hashInit,
                                                         hashUpdate)

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
import           Data.Conduit.List                      (sourceList)

import           Data.ByteString                        (ByteString)
import qualified Data.ByteString                        as BS
import           Data.ByteString.Builder
import qualified Data.DList                             as D
import           Data.Monoid                            (mempty, (<>))
-- import           Data.Text                              (Text)

import           Control.Applicative

import           Control.Exception.Lens
import           Control.Lens

import           Data.List.NonEmpty                     as NE (NonEmpty,
                                                               nonEmpty)

import           Text.Printf                            (printf)

streamUpload :: (MonadResource m, MonadAWS m, HasEnv r, MonadReader r m, Applicative m)
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
      go !bss !bufsize !ctx !partnum !completed = Data.Conduit.await >>= \mbs -> case mbs of
        Just bs | l <- BS.length bs
                , bufsize + l <= 6*1024*1024 -> -- Making this 5MB+1 seemed to cause AWS to complain
                    go (D.snoc bss bs) (bufsize + l) (hashUpdate ctx bs) partnum completed

                | otherwise -> do
                    rs <- lift $ do
                        res <- partUploader partnum
                                            (bufsize + BS.length bs)
                                            (hashFinalize (hashUpdate ctx bs))
                                            (D.toList $ D.snoc bss bs)
                        checkUpload res

                    logStr $ printf "\n**** Uploaded part %d size $d\n" partnum bufsize
                    go empty 0 hashInit (partnum+1) $ D.snoc completed $ completedPart partnum <$> (rs ^. uprsETag)

        Nothing -> lift $ do
            rs <- checkUpload =<< partUploader partnum bufsize (hashFinalize ctx) (D.toList bss)

            logStr $ printf "\n**** Uploaded (final) part %d size $d\n" partnum bufsize

            let allParts = D.toList $ D.snoc completed $ completedPart partnum <$> (rs ^. uprsETag)
                prts = NE.nonEmpty =<< sequence allParts

            send $ completeMultipartUpload bucket key upId
                    & cMultipartUpload ?~ set cmuParts prts completedMultipartUpload


      partUploader :: MonadAWS m => Int -> Int -> Digest SHA256 -> [ByteString] -> m UploadPartResponse
      partUploader pnum size digest
        = send
        . uploadPart bucket key pnum upId
        . toBody
        . hashedBody digest (fromIntegral size)
        . sourceList

      checkUpload :: (Monad m, Applicative m) => UploadPartResponse -> m UploadPartResponse
      checkUpload upr = do
        when (upr ^. uprsResponseStatus /= 200) $ fail "Failed to upload piece"
        pure upr

  catching id (go D.empty 0 hashInit 1 D.empty) $ \e -> do
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
