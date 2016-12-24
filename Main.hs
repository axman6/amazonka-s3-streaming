{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}
module Main where


import Data.Conduit        (($$))
import Data.Conduit.Binary (sourceHandle)
import Data.Text           (pack)

import Network.AWS
import Network.AWS.Data.Text                (fromText)
import Network.AWS.S3.CreateMultipartUpload
import Network.AWS.S3.StreamingUpload

import System.Environment
import System.IO          (BufferMode(BlockBuffering), hSetBuffering, stdin)

main :: IO ()
main = do
  args <- getArgs
  case args of
    (region:profile:credfile:bucket:key:file:_) ->
      case (,,,) <$> (FromFile <$> fromText (pack profile) <*> pure credfile)
                 <*> (fromText (pack region) :: Either String Region)
                 <*> fromText (pack bucket)
                 <*> fromText (pack key)
      of
        Right (creds,reg,buck,ky) -> do
#if !MIN_VERSION_amazonka(1,4,4)
          env <- newEnv reg creds
#else
          env <- newEnv creds
#endif
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
                <*> (fromText (pack region) :: Either String Region)
                <*> fromText (pack bucket)
      of
        Right (creds,reg,buck) -> do
#if !MIN_VERSION_amazonka(1,4,4)
          env <- newEnv reg creds
#else
          env <- newEnv creds
#endif
          res <- runResourceT . runAWS env . abortAllUploads $ buck
          print res
        Left err -> print err >> usage

    _ -> usage

usage :: IO ()
usage = putStrLn . unlines $
  [ "Usage: \n"
  , "  Upload file:"
  , "    s3upload <region:ap-southeast-2> <profile> <credentials file:$HOME/.aws/credentials> <bucket> <object key> <file to upload>"
  , "  Abort all unfinished uploads for bucket:"
  , "    s3upload abort <region:ap-southeast-2> <profile> <credentials file:$HOME/.aws/credentials> <bucket>\n"
  , "all arguments must be supplied - the region will be obtained from the AWS_REGION env var"
  , "if compiled with amazonka > 1.4.4, but must still be supplied (making an option parsing library"
  , "a dependency of this package seemed overkill)"
 ]
