{-# language
    ExplicitForAll
  , ExistentialQuantification
  , FlexibleContexts
  , ScopedTypeVariables
  , TypeApplications
  , DeriveGeneric
  , DefaultSignatures
  , AllowAmbiguousTypes
#-}

module Database.Odpi.FromRow where

import GHC.Generics
import Data.Tuple.Only ( Only(Only) )
import Data.Word ( Word32 )
import Data.Proxy

import Database.Odpi.FromField
import Database.Odpi.LibDpi
import Database.Odpi.Statement
import Database.Odpi.Types

import Database.Odpi.FromRow.Generic


-- | A collection that can be read from an ODPI-C statement
class FromRow a where
  -- | A list of target Haskell types for each column in a row
  --
  -- This is used to adjust how a value is fetched from the database if
  -- needed. For example Scientific would be fetched as NativeTypeDouble
  -- by default whereas we want NativeTypeBytes to get full precision.
  -- Uses nativeTypeFor from FromField.
  rowRep :: [AnyField]

  default rowRep :: GFromRow (Rep a) => [AnyField]
  rowRep = growrep @(Rep a)

  -- | Fetch values from the current row
  fromRow :: Statement -> IO a

  default fromRow :: (Generic a, GFromRow (Rep a)) => Statement -> IO a
  fromRow s = do
    (x, _) <- gfromrow @(Rep a) 1 s
    pure $ to x

defineValuesForRow :: forall a. FromRow a => Statement -> IO ()
defineValuesForRow s = do
  n <- stmtGetNumQueryColumns s
  mapM_ f $ zip [1..n] (rowRep @a)
  where
    f :: (Word32, AnyField) -> IO ()
    f (i, AF (_ :: Proxy x)) =
      case nativeTypeFor @x of
        Nothing -> pure ()
        Just ty -> defineValueForTy s i ty

defineValueForTy :: Statement -> Word32 -> NativeTypeNum -> IO ()
defineValueForTy s i ty = do
  ti <- queryInfo_typeInfo <$> stmtGetQueryInfo s i
  _ <- stmtDefineValue s i (dataTypeInfo_oracleTypeNum ti) ty
                           (dataTypeInfo_dbSizeInBytes ti) True
                           (dataTypeInfo_objectType ti)
  pure ()

instance (FromField a) => FromRow (Only a) where
  --rowRep = [AF (Proxy @a)]
  fromRow s = Only <$> field s 1

instance (FromField a, FromField b) => FromRow (a, b) where
  --rowRep = [AF (Proxy @a), AF (Proxy @b)]
  fromRow s = (,) <$> field s 1 <*> field s 2

instance (FromField a, FromField b, FromField c) => FromRow (a, b, c) where
  --rowRep = [AF (Proxy @a), AF (Proxy @b), AF (Proxy @c)]
  fromRow s = (,,) <$> field s 1 <*> field s 2 <*> field s 3

instance (FromField a, FromField b, FromField c, FromField d) => FromRow (a, b, c, d) where
  --rowRep = [AF (Proxy @a), AF (Proxy @b), AF (Proxy @c), AF (Proxy @d)]
  fromRow s = (,,,) <$> field s 1 <*> field s 2 <*> field s 3 <*> field s 4

instance ( FromField a, FromField b, FromField c, FromField d
         , FromField e ) => FromRow (a, b, c, d, e) where
  -- rowRep = [ AF (Proxy @a), AF (Proxy @b), AF (Proxy @c), AF (Proxy @d)
  --          , AF (Proxy @e)
  --          ]
  fromRow s = (,,,,) <$> field s 1 <*> field s 2 <*> field s 3 <*> field s 4
                     <*> field s 5

instance ( FromField a, FromField b, FromField c, FromField d
         , FromField e, FromField f ) => FromRow (a, b, c, d, e, f) where
  -- rowRep = [ AF (Proxy @a), AF (Proxy @b), AF (Proxy @c), AF (Proxy @d)
  --          , AF (Proxy @e), AF (Proxy @f)
  --          ]
  fromRow s = (,,,,,) <$> field s 1 <*> field s 2 <*> field s 3 <*> field s 4
                      <*> field s 5 <*> field s 6

instance ( FromField a, FromField b, FromField c, FromField d
         , FromField e, FromField f, FromField g ) => FromRow (a, b, c, d, e, f, g) where
  -- rowRep = [ AF (Proxy @a), AF (Proxy @b), AF (Proxy @c), AF (Proxy @d)
  --          , AF (Proxy @e), AF (Proxy @f), AF (Proxy @g)
  --          ]
  fromRow s = (,,,,,,) <$> field s 1 <*> field s 2 <*> field s 3 <*> field s 4
                       <*> field s 5 <*> field s 6 <*> field s 7
