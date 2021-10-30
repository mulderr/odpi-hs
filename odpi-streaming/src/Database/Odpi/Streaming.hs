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


-- | Query that produces a stream of results
--
-- This function needs verification but crude tests show that if the provided continuation closes the
-- stream mid-way (ex. S.take n) the remaining rows (mod some buffer) will not be fetched.
--
-- Uses dpiStmt_fetch internally while ODPI-C documentation notes:
--
-- "The function dpiStmt_fetchRows() should be used instead of this function if it is important to
--  control when the internal fetch (and round-trip to the database) takes place."
query :: forall m a b. (FromRow a, MonadIO m, MonadMask m)
  => Connection -- ^ connection
  -> ByteString -- ^ SQL query
  -> [NativeValue] -- ^ values for bind variables (to be bound by position)
  -> (Stream (Of a) m () -> m b) -- ^ continuation that processes the stream
  -> m b
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

    -- here Left does not signal error but simply end of results (ORA-01403)
    -- an error would be thrown as an exception
    fetchOne :: Statement -> m (Either () a)
    fetchOne s = liftIO $ do
      midx <- stmtFetch s
      case midx of
        Nothing -> pure $ Left ()
        Just _ -> Right <$> fromRow s
