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

-- exceptions
import Control.Monad.Catch

-- odpi-simple
import Database.Odpi


query :: forall m a b. (FromRow a, MonadIO m, MonadMask m) => Connection -> ByteString -> [NativeValue] -> (Stream (Of a) m () -> m b) -> m b
query conn sql params cont = do
  bracket (liftIO $ connPrepareStmt conn False sql)
          (liftIO . stmtRelease)
          (\s -> prep s >> cont (go s))
  where
    prep :: Statement -> m ()
    prep s = liftIO $ do
      -- bind params
      mapM_ (\(idx, v) -> stmtBindValueByPos s idx v) (zip [1..] params)
      _ <- stmtExecute s ModeExecDefault
      defineValuesForRow @a s
    
    go :: Statement -> Stream (Of a) m ()
    go s =
      S.untilLeft (fetchOne s)

    fetchOne :: Statement -> m (Either () a)
    fetchOne s = liftIO $ do
      midx <- stmtFetch s
      case midx of
        Nothing -> pure $ Left ()
        Just _ -> do
          x <- fromRow s
          pure $ Right x
