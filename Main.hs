
{-# LANGUAGE OverloadedStrings #-}
module Main where


import           Data.Text                            (pack)
import           Network.AWS
import           Network.AWS.Data.Text                (fromText)
import           Network.AWS.S3.CreateMultipartUpload
import           Network.AWS.S3.StreamingUpload
import           System.Environment

main :: IO ()
main = do
  args <- getArgs
  case args of
    (region:profile:credfile:bucket:key:file:_) ->
      case (,,,) <$> (FromFile <$> fromText (pack profile) <*> pure credfile)
                 <*> fromText (pack region)
                 <*> fromText (pack bucket)
                 <*> fromText (pack key)
      of
        Right (env',reg,buck,ky) -> do
          env <- newEnv reg env'
          res <- runResourceT . runAWS env . concurrentUpload (FP file) $
                   createMultipartUpload buck ky
          print res
        Left err -> print err >> usage
    ("abort":region:profile:credfile:bucket:_) ->
      case (,,) <$> (FromFile <$> fromText (pack profile) <*> pure credfile)
                <*> fromText (pack region)
                <*> fromText (pack bucket)
      of
        Right (env',reg,buck) -> do
          env <- newEnv reg env'
          res <- runResourceT . runAWS env . abortAllUploads $ buck
          print res
        Left err -> print err >> usage

    _ -> usage

usage :: IO ()
usage = putStrLn "\nUsage: \n\n\
                  \  Upload file:\n\
                  \    s3upload <region:Sydney> <profile> <credentials file:~/.aws/credentials> <bucket> <object key> <file to upload>\n\
                  \  Abort all unfinished uploads for bucket:\n\
                  \    s3upload abort <region:Sydney> <profile> <credentials file:~/.aws/credentials> <bucket>\n\n\
                  \all arguments must be supplied"
