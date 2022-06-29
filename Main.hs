{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Main where

import Control.Lens                         ( set )
import Data.Conduit                         ( runConduit, (.|) )
import Data.Conduit.Binary                  ( sourceHandle )
import Data.Functor                         ( (<&>) )
import Data.Text                            ( pack )

import Amazonka.S3.StreamingUpload
       ( UploadLocation(FP), abortAllUploads, concurrentUpload, streamUpload )

import Amazonka (runResourceT)
import Amazonka.Env (newEnv, envRegion)
import Amazonka.Auth (discover)
import Amazonka.S3.Types (BucketName(..), ObjectKey (..))
import Amazonka.S3.CreateMultipartUpload    ( newCreateMultipartUpload )

import Control.Monad.IO.Class ( liftIO )
import System.Environment     ( getArgs )
import System.IO              ( BufferMode(BlockBuffering), hSetBuffering, stdin )

main :: IO ()
main = do
  args <- getArgs

  env <- newEnv discover

  case args of
    ("upload":bucket:key:file:_) -> do
        let buck = BucketName $ pack bucket
            ky   = ObjectKey $ pack key
        hSetBuffering stdin (BlockBuffering Nothing)
        res <- runResourceT $ case file of
                "-" -> runConduit (sourceHandle stdin .| streamUpload env Nothing (newCreateMultipartUpload buck ky))
                        >>= liftIO . either print print
                _   -> concurrentUpload env Nothing Nothing (FP file) (newCreateMultipartUpload buck ky)
                        >>= liftIO . print

        print res

    ("abort":region:profile:credfile:bucket:_) -> do
          res <- runResourceT $ abortAllUploads env (BucketName $ pack bucket)
          print res
    _ -> usage

usage :: IO ()
usage = putStrLn . unlines $
  [ "Usage:"
  , ""
  , "  Upload file:"
  , "    s3upload upload <bucket> <object key> <file to upload>"
  , ""
  , "  Abort all unfinished uploads for bucket:"
  , "    s3upload abort <bucket>"
  , ""
  , "Uses `newEnv discover` to make the Amazonka environment, so it wil look at"
  , "appropriate env vars, or ~/.aws/credentials, etc."
 ]
