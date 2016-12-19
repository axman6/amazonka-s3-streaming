{-# LANGUAGE BangPatterns          #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PatternGuards         #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TemplateHaskell       #-}

module Network.AWS.S3.StreamingUpload where

import           Network.AWS
import           Network.AWS.S3.AbortMultipartUpload
import           Network.AWS.S3.CompleteMultipartUpload
import           Network.AWS.S3.CreateMultipartUpload
import           Network.AWS.S3.ListMultipartUploads
import           Network.AWS.S3.ListParts
import           Network.AWS.S3.Types                   (BucketName,
                                                         CompletedPart,
                                                         ObjectKey, cmuParts,
                                                         completedMultipartUpload,
                                                         completedPart)
import           Network.AWS.S3.UploadPart

import           Control.Monad                          (when)
import           Control.Monad.IO.Class
import           Control.Monad.Morph
import           Control.Monad.Reader.Class
import           Control.Monad.Trans.Resource

import           Data.Conduit
import           Data.Conduit.Binary                    (sourceFile)

import           Control.Applicative
import           Data.ByteString                        as BS
import           Data.ByteString.Builder
import qualified Data.ByteString.Lazy                   as BSL
import           Data.Foldable                          (asum)
import           Data.Monoid                            ((<>))
-- import           Data.Text                              (Text)

import           Control.Lens
import           Control.Monad.Error.Class
import           Control.Monad.Error.Lens

import           Data.List.NonEmpty                     as NE (NonEmpty,
                                                               nonEmpty)

data StreamingUploadError
  = FailedToCreateUpload CreateMultipartUpload CreateMultipartUploadResponse
  | PartUploadFailure UploadPartResponse
  deriving (Show, Eq)

makeClassyPrisms ''StreamingUploadError

streamUpload :: (MonadResource m, MonadAWS m, HasEnv r, MonadReader r m) -- , AsStreamingUploadError e, MonadError e m)
             => CreateMultipartUpload
             -> Sink ByteString m CompleteMultipartUploadResponse
streamUpload cmu = do
  logger <- lift $ view envLogger
  cmur <- lift (send cmu)
  if cmur ^. cmursResponseStatus /= 200
    -- then throwing _FailedToCreateUpload (cmu,cmur)
    then fail "Failed to create upload"
    else do
      liftIO $ logger Info "\n**** Created upload"
      let Just upId = cmur ^. cmursUploadId
          bucket = cmu ^. cmuBucket
          key    = cmu ^. cmuKey
          partUploader = uploadPart bucket key
          logStr :: MonadIO m => String -> m ()
          logStr = liftIO . logger Info . stringUtf8
          -- checkUpload :: (AsStreamingUploadError e, MonadError e m)
          --             => UploadPartResponse -> m ()
          -- checkUpload upr = when (upr ^. uprsResponseStatus /= 200)
          --                       $ throwing _PartUploadFailure upr
          checkUpload :: Monad m => UploadPartResponse -> m UploadPartResponse
          checkUpload upr = do
            when (upr ^. uprsResponseStatus /= 200)
                $ fail "Failed to upload piece"
            pure upr
          -- go :: Text -> Builder -> Int -> Int -> Sink ByteString m ()
          go !bu !bufsize !partnum !completed = do
              mbs <- Data.Conduit.await
              case mbs of
                Nothing -> lift $ do
                            rs <- bu
                                    & toLazyByteString
                                    & toBody
                                    & partUploader partnum upId
                                    & send
                                    >>= checkUpload
                            liftIO $ logger Info $ stringUtf8 $ Prelude.concat ["\n**** Uploaded (final) part ",show partnum," size ",show bufsize,"\n"]
                            let
                              parts :: Maybe (NonEmpty CompletedPart)
                              parts = do
                                  cs <- sequence ((completedPart partnum <$> (rs ^. uprsETag)) : completed)
                                  NE.nonEmpty . Prelude.reverse $ cs

                            send $ completeMultipartUpload bucket key upId
                                    & cMultipartUpload ?~ (completedMultipartUpload & cmuParts .~ parts)


                Just bs | l <- BS.length bs, bufsize + l <= (6*1024^2) ->
                            go (bu <> byteString bs) (bufsize + l) partnum completed
                        | otherwise -> do
                            rs <- lift $ bs
                                          & byteString
                                          & mappend bu
                                          & toLazyByteString
                                          & toBody
                                          & partUploader partnum upId
                                          & send
                                          >>= checkUpload
                            liftIO $ logger Info $ stringUtf8 $ Prelude.concat ["\n**** Uploaded part ",show partnum," size ",show bufsize,"\n"]
                            go mempty 0 (partnum+1) $ (completedPart partnum <$> (rs ^. uprsETag)) : completed

      go (mempty :: Builder) 0 1 []
      --  `catches`
      --   [handler _StreamingUploadError $ \e -> do
      --       rs <- lift $ send (abortMultipartUpload bucket key upId)
      --       pure (Left rs)
      --   ] -- TODO AbortMultipartUpload if this fails


uploadFile :: (MonadAWS m, HasEnv r, MonadReader r m, MonadResource m)
           => BucketName -> ObjectKey -> FilePath
           -> m CompleteMultipartUploadResponse
uploadFile bucket key filepath =
  sourceFile filepath $$ streamUpload (createMultipartUpload bucket key)
