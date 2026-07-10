{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Data.Text
import Database.DuckDB.Simple
import System.Environment (lookupEnv)
import System.Exit (die)
import Infra.Sink.DuckDB

main :: IO ()
main = do
  maybeKey <- lookupEnv "AWS_ACCESS_KEY_ID"
  maybeSecret <- lookupEnv "AWS_SECRET_ACCESS_KEY"
  maybeEndpoint <- lookupEnv "AWS_ENDPOINT"

  (keyId, secretKey, endpoint) <- case (maybeKey, maybeSecret, maybeEndpoint) of
    (Just k, Just s, Just e) -> return (k, s, e)
    _ -> die "Error fatal: Keys storage not found"

  withConnection ":memory:" $ \conn -> do
        _ <- execute_ conn "INSTALL iceberg; LOAD iceberg;"
        _ <- execute_ conn "INSTALL httpfs; LOAD httpfs;"

        let secretQuery = mconcat 
              [ "CREATE SECRET (TYPE S3, "
              , "KEY_ID '", keyId, "', "
              , "SECRET '", secretKey, "', "
              , "ENDPOINT '", endpoint, "', "
              , "URL_STYLE 'path', "
              , "USE_SSL 'false');"
              ]

        _ <- execute_ conn (Query (pack secretQuery))

        let icebergSecretQuery = mconcat
              [ "CREATE SECRET lk_secret ("
              , "TYPE ICEBERG, "
              , "TOKEN 'dummy');"
              ]
        _ <- execute_ conn (Query (pack icebergSecretQuery))

        let attachQuery = mconcat
                [ "ATTACH 'datalake-haumea' AS haumea ("
                , "TYPE iceberg, "
                , "ENDPOINT 'http://localhost:8181/catalog', "
                , "SECRET 'lk_secret'"
                , ");"
                ]
        _ <- execute_ conn (Query (pack attachQuery))
        putStrLn "Iceberg connection done..."
        tables <- fetchFactAries conn
        print tables

