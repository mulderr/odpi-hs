{-# language ScopedTypeVariables #-}
{-# language TypeApplications #-}
{-# language FlexibleInstances #-}

module Database.Odpi.Streaming where

import Control.Monad.IO.Class
import Data.ByteString (ByteString)

-- streaming
import Streaming (Stream)
import Streaming.Prelude (Of (..))
import qualified Streaming.Prelude as S

-- streaming-with
import Streaming.With

-- odpi-simple
import Database.Odpi


query :: forall a. (FromRow a) => Connection -> ByteString -> [NativeValue] -> Stream (Of a) IO ()
query conn sql params = do
  bracket (liftIO $ connPrepareStmt conn False sql)
          (liftIO . stmtRelease)
          go
  where
    go s = do
      liftIO $ do
        bindparams s params
        _ <- stmtExecute s ModeExecDefault
        defineValuesForRow @a s
      S.untilLeft (fetchOne s)

    bindparams stmt ps =
      mapM_ (\(idx, v) -> stmtBindValueByPos stmt idx v) (zip [1..] ps)

    fetchOne :: Statement -> IO (Either () a)
    fetchOne s = do
      midx <- stmtFetch s
      case midx of
        Nothing -> pure $ Left ()
        Just _ -> do
          x <- fromRow s
          pure $ Right x
