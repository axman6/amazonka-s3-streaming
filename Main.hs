
{-# LANGUAGE OverloadedStrings #-}
module Main where


import           Data.Conduit                         (($$))
import           Data.Conduit.Binary                  (sourceHandle)
import           Data.Text                            (pack)
import           Network.AWS
import           Network.AWS.Data.Text                (fromText)
import           Network.AWS.S3.CreateMultipartUpload
import           Network.AWS.S3.StreamingUpload
import           System.Environment
import           System.IO                            (BufferMode (BlockBuffering),
                                                       hSetBuffering, stdin)

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
          hSetBuffering stdin (BlockBuffering Nothing)
          res <- runResourceT . runAWS env $ case file of
                  -- Stream data from stdin
                  "-" -> sourceHandle stdin $$ streamUpload (createMultipartUpload buck ky)
                  -- Read from a file
                  _   -> concurrentUpload (FP file) $ createMultipartUpload buck ky
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
                  \    s3upload <region:ap-southeast-2> <profile> <credentials file:$HOME/.aws/credentials> <bucket> <object key> <file to upload>\n\
                  \  Abort all unfinished uploads for bucket:\n\
                  \    s3upload abort <region:ap-southeast-2> <profile> <credentials file:$HOME/.aws/credentials> <bucket>\n\n\
                  \all arguments must be supplied"
