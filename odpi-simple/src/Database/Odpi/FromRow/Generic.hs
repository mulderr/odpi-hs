{-# language AllowAmbiguousTypes #-}
{-# language DeriveGeneric #-}
{-# language KindSignatures #-}
{-# language ScopedTypeVariables #-}
{-# language TypeApplications #-}
{-# language TypeOperators #-}

-- | Generic FromRow for simple product types.
module Database.Odpi.FromRow.Generic where

import GHC.Generics
import Data.Proxy
import Data.Kind
import Data.Word

import Database.Odpi.FromField
import Database.Odpi.Types


class GFromRow (f :: Type -> Type) where
  growrep :: [AnyField]
  gfromrow :: Word32 -> Statement -> IO (f a, Word32)

instance forall a b. (GFromRow a, GFromRow b) => GFromRow (a :*: b) where
  growrep = growrep @a ++ growrep @b
  gfromrow idx s = do
    (x, idx2) <- gfromrow @a idx s
    (y, idx3) <- gfromrow @b idx2 s
    pure (x :*: y, idx3)

instance forall i c a. GFromRow a => GFromRow (M1 i c a) where
  growrep = growrep @a
  gfromrow idx s = do
    (x, idx2) <- gfromrow @a idx s
    pure (M1 x, idx2)

instance forall i a. FromField a => GFromRow (K1 i a) where
  growrep = [AF (Proxy @a)]
  gfromrow idx s = do
    x <- field @a s idx
    pure (K1 x, idx + 1)
