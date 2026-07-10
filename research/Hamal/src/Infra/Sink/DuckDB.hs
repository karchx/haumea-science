{-# LANGUAGE OverloadedStrings #-}

module Infra.Sink.DuckDB (fetchFactAries) where

import Data.Text (Text)
import Database.DuckDB.Simple

fetchFactAries :: Connection -> IO [Maybe Text]
fetchFactAries conn = do
    fmap fromOnly <$> query_ conn "SELECT source_id FROM haumea.gold.fact_aries LIMIT 10"


