{-# language
    LambdaCase
  , ScopedTypeVariables
  , FlexibleInstances
  , StandaloneDeriving
  , GeneralizedNewtypeDeriving
  , DeriveFunctor
  , ExistentialQuantification
  , AllowAmbiguousTypes
  , TypeApplications
#-}
-- | Conversions from ODPI-C NativeValue to Haskell types
--
-- * Numbers
--
-- In ODPI-C the default native type is integer if it the number can be
-- placed in a 64-bit integer and is otherwise returned as double.
--
-- For number types we strictly follow the principle to never implicity
-- truncate. This means we never convert from a "larger" NativeValue to
-- a "smaller" Haskell type unless we can guarantee it's safe by examining
-- the column definition.
--
-- However, we do not try to correct ODPI-C which by default returns all
-- NUMBERs as one of Int64, Word64, Float or Double. If ODPI-C truncates a
-- number column to one of those types we just go with it.
--
-- None of the types returned by ODPI-C by default preserve the full 38
-- decimal digits of precision provided by NUMBER. To read a NUMBER with
-- full precision use Scientific. The instance for Scientific overrides
-- the defaults and requests the value as bytes.
--
-- This can be problematic in practice because unqualified NUMBER
-- means no restrictions on the value which translates to a floating point
-- with *maximum* precision and normally can only be read either as Double
-- (truncated by ODPI-C) or Scientific. So whenever you can, be more specific
-- for primary keys etc. You may at the very least want to limit the values
-- to integers by saying NUMBER(38). Or even better NUMBER(18) to fit into
-- Int64.
--
-- If you have no influence over the schema and need things to work anyway,
-- you can create a newtype wrapper with an instance that does whatever
-- weird things you require. Say you have a primary key of type NUMBER
-- but want to read is as Int32:
--
-- newtype Pk32 = Pk32 { unPk32 :: Int32 } deriving (Eq, Show)
-- instance FromField Pk32 where
--   fromField _ (NativeInt64 x) = pure $ Pk32 $ fromIntegral x
--   fromField i v = convError "Pk32" i v
--   nativeTypeFor _ = Just NativeTypeInt64
--
-- First we request that Pk32 always be fetched as Int64 which is the
-- closest to what we want and then further truncate the value to Int32
-- ourselves.
--
-- Also see 'Exactly' for common cases.
--
--
-- * Dates and timestamps
--
-- For UTCTime by default we are very strict to ensure the value is actually
-- UTC. If the underlying Oracle data type does not provide time zone info,
-- an error will be reported. To read UTCTime from DATE or TIMESTAMP use
-- 'Exactly UTCTime' or write your own wrapper.
module Database.Odpi.FromField where

import Control.Exception
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B8
import Data.Fixed ( Fixed(MkFixed), Pico )
import Data.Int ( Int16, Int32, Int64 )
import Data.Proxy ( Proxy(..) )
import Data.Word ( Word16, Word32, Word64 )
import Data.Scientific (Scientific)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Time.Calendar (fromGregorian)
import Data.Time.Clock (UTCTime(..))
import Data.Time.LocalTime (LocalTime(LocalTime), TimeOfDay(TimeOfDay), TimeZone(TimeZone), localTimeToUTC, utc)

import Database.Odpi.LibDpi
import Database.Odpi.NativeValue
import Database.Odpi.Statement
import Database.Odpi.Types

data AnyField = forall a. FromField a => AF (Proxy a)

data Ok a = Errors [SomeException] | Ok !a
  deriving(Show, Functor)

pureOk :: a -> IO (Ok a)
pureOk = pure . Ok

data DpiConversionError
  = DpiConversionError NativeValue QueryInfo String
  deriving Show

instance Exception DpiConversionError where
  displayException (DpiConversionError v qi hs) = unlines
    [ "Conversion error:"
    , "  value: " ++ show v
    , "  fetched from column: " ++ show qi
    , "  cannot be safely converted to Haskell type: " ++ hs
    ]

convError :: String -> QueryInfo -> NativeValue -> IO (Ok a)
convError s i v = pure $ Errors [SomeException $ DpiConversionError v i s]

fromIntegralField :: Num b => String -> Int16 -> QueryInfo -> NativeValue -> IO (Ok b)
fromIntegralField tyName maxPrec i v = do
  let colPrec = dataTypeInfo_precision $ queryInfo_typeInfo i
      colScale = dataTypeInfo_scale $ queryInfo_typeInfo i
  f v colPrec colScale
  where
    f (NativeInt64 x) p s | p <= maxPrec && s == 0 = pureOk $ fromIntegral x
    f (NativeUint64 x) p s | p <= maxPrec && s == 0 = pureOk $ fromIntegral x
    f _ _ _ = convError tyName i v

field :: FromField a => Statement -> Word32 -> IO a
field s i = do
  qi <- stmtGetQueryInfo s i
  v <- stmtGetQueryValue s i
  r <- fromField qi v
  case r of
    Ok a -> pure a
    Errors (e:_) -> throwIO e
    Errors _ -> error "empty exception list for Errors"

-- | A type that may be read from dpiData
class FromField a where
  fromField :: QueryInfo -> NativeValue -> IO (Ok a)
  nativeTypeFor :: Maybe NativeTypeNum
  nativeTypeFor = Nothing

instance FromField Bool where
  fromField _ (NativeBool b) = pureOk b
  fromField i v = convError "Bool" i v

instance FromField Char where
  fromField _ (NativeBytes b) = pureOk $ B8.head b
  fromField i v = convError "Char" i v

instance FromField ByteString where
  fromField _ (NativeBytes b) = pureOk b
  fromField i v = convError "ByteString" i v

instance FromField T.Text where
  fromField _ (NativeBytes b) = pureOk $ TE.decodeUtf8 b
  fromField i v = convError "Text" i v

instance FromField String where
  fromField _ (NativeBytes b) = pureOk $ T.unpack $ TE.decodeUtf8 b
  fromField i v = convError "String" i v

-- Int is rather vague by definition:
-- A fixed-precision integer type with at least the range [-2^29 .. 2^29-1]
intDecimalPrec :: Int16
intDecimalPrec = truncate $ (logBase 10 $ realToFrac (maxBound :: Int) :: Double)

wordDecimalPrec :: Int16
wordDecimalPrec = truncate $ (logBase 10 $ realToFrac (maxBound :: Word) :: Double)

instance FromField Int where
  fromField = fromIntegralField "Int" intDecimalPrec

instance FromField Int16 where
  fromField = fromIntegralField "Int16" 4

instance FromField Int32 where
  fromField = fromIntegralField "Int32" 9

instance FromField Int64 where
  fromField _ (NativeInt64 x) = pureOk x
  fromField i v = convError "Int64" i v

instance FromField Word where
  fromField = fromIntegralField "Word" wordDecimalPrec

instance FromField Word16 where
  fromField = fromIntegralField "Word16" 4

instance FromField Word32 where
  fromField = fromIntegralField "Word32" 9

instance FromField Word64 where
  fromField _ (NativeUint64 x) = pureOk x
  fromField i v = convError "Word64" i v

instance FromField Integer where
  fromField _ (NativeInt64 x) = pureOk $ fromIntegral x
  fromField _ (NativeUint64 x) = pureOk $ fromIntegral x
  fromField i v = convError "Integer" i v

instance FromField Float where
  fromField _ (NativeFloat x) = pureOk x
  fromField i v = convError "Float" i v

instance FromField Double where
  fromField _ (NativeDouble x) = pureOk x
  fromField _ (NativeFloat x) = pureOk $ realToFrac x
  fromField i v = convError "Double" i v

-- | Scientific is fetched as bytes which is the only way to preserve full precision of
-- Oracle NUMBER type. Beware of using it haphazardly as this incures a performance
-- penalty. Always use Double unless you really need additional range/precision.
instance FromField Scientific where
  fromField _ (NativeBytes x) = pureOk $ read $ B8.unpack x
  fromField i v = convError "Scientific" i v
  nativeTypeFor = Just NativeTypeBytes

instance FromField LocalTime where
  fromField _ (NativeTimestamp x) = do
    let d = fromGregorian (fromIntegral $ timestamp_year x)
                          (fromIntegral $ timestamp_month x)
                          (fromIntegral $ timestamp_day x)
        sec = fromIntegral $ timestamp_second x :: Pico
        fsec = MkFixed $ (fromIntegral $ timestamp_fsecond x) `div` 10
        t = TimeOfDay (fromIntegral $ timestamp_hour x)
                      (fromIntegral $ timestamp_minute x)
                      (sec + fsec)
    pureOk $ LocalTime d t
  fromField i v = convError "LocalTime" i v

-- This instance tries to be very strict in ensuring the value is UTC
--
-- The rule is that the underlying OracleTypeNum needs to provide time zone information
-- either explicitly (stored in the database) or implicitly (where system time zone is
-- used). Otherwise, that is in any case where time zone is unknown, an error will be
-- reported:
--
-- DATE                            error, unknown time zone
-- TIMESTAMP                       error, unknown time zone
-- TIMESTAMP WITH TIME ZONE        ok, explicitly stores time zone
-- TIMESTAMP WITH LOCAL TIME ZONE  ok, system time zone is used
--
-- There are however valid use cases where you'd want to read UTCTime from DATE or
-- TIMESTAMP or do other shenanigans. You can:
-- * use Exactly UTCTime to read UTCTime from any Timestamp by assuming it is UTC if neccessary
-- * use LocalTime and convert to UTC haskell side
-- * use a custom newtype over UTCTime with a custom FromField instance
instance FromField UTCTime where
  fromField i x@(NativeTimestamp t) =
    let 
      tz = TimeZone (60 * (fromIntegral $ timestamp_tzHourOffset t) + (fromIntegral $ timestamp_tzMinuteOffset t)) False "XXX"
    in
      case dataTypeInfo_oracleTypeNum $ queryInfo_typeInfo i of
        OracleTypeTimestampTz -> fmap (localTimeToUTC tz) <$> fromField i x
        OracleTypeTimestampLtz -> fmap (localTimeToUTC tz) <$> fromField i x
        _ -> convError "UTCTime" i x
  fromField i v = convError "UTCTime" i v

instance FromField a => FromField (Maybe a) where
  fromField _ (NativeNull _) = pureOk Nothing
  fromField i v = fmap Just <$> fromField i v
  nativeTypeFor = nativeTypeFor @a

-- | A wrapper that provides alternative, potentially unsafe but very useful FromField instances
--
-- For number types, we allow truncation to the target type even if we cannot guarantee the
-- conversion is safe.
--
-- For UTCTime, we read the Timestamp as UTC even if the time zone is unknown.
newtype Exactly a = Exactly { unExactly :: a } deriving (Eq, Ord, Show)
deriving instance Num a => Num (Exactly a)
deriving instance Enum a => Enum (Exactly a)
deriving instance Real a => Real (Exactly a)
deriving instance Integral a => Integral (Exactly a)

instance FromField (Exactly Int) where
  fromField _ (NativeInt64 x) = pureOk $ Exactly $ fromIntegral x
  fromField i v = convError "Exactly Int" i v
  nativeTypeFor = Just NativeTypeInt64

instance FromField (Exactly Int16) where
  fromField _ (NativeInt64 x) = pureOk $ Exactly $ fromIntegral x
  fromField i v = convError "Exactly Int16" i v
  nativeTypeFor = Just NativeTypeInt64

instance FromField (Exactly Int32) where
  fromField _ (NativeInt64 x) = pureOk $ Exactly $ fromIntegral x
  fromField i v = convError "Exactly Int32" i v
  nativeTypeFor = Just NativeTypeInt64

instance FromField (Exactly Int64) where
  fromField _ (NativeInt64 x) = pureOk $ Exactly x
  fromField i v = convError "Exactly Int64" i v
  nativeTypeFor = Just NativeTypeInt64

instance FromField (Exactly Word) where
  fromField _ (NativeUint64 x) = pureOk $ Exactly $ fromIntegral x
  fromField i v = convError "Exactly Word" i v
  nativeTypeFor = Just NativeTypeUint64

instance FromField (Exactly Word16) where
  fromField _ (NativeUint64 x) = pureOk $ Exactly $ fromIntegral x
  fromField i v = convError "Exactly Word16" i v
  nativeTypeFor = Just NativeTypeUint64

instance FromField (Exactly Word32) where
  fromField _ (NativeUint64 x) = pureOk $ Exactly $ fromIntegral x
  fromField i v = convError "Exactly Word32" i v
  nativeTypeFor = Just NativeTypeUint64

instance FromField (Exactly Word64) where
  fromField _ (NativeUint64 x) = pureOk $ Exactly x
  fromField i v = convError "Exactly Word64" i v
  nativeTypeFor = Just NativeTypeUint64

-- When time zone is unknown assumes UTC
instance FromField (Exactly UTCTime) where
  fromField i x@(NativeTimestamp t) =
    let
      tz = TimeZone (60 * (fromIntegral $ timestamp_tzHourOffset t) + (fromIntegral $ timestamp_tzMinuteOffset t)) False "XXX"
    in
      case dataTypeInfo_oracleTypeNum $ queryInfo_typeInfo i of
        OracleTypeDate -> fmap (Exactly . localTimeToUTC utc) <$> fromField i x
        OracleTypeTimestamp -> fmap (Exactly . localTimeToUTC utc) <$> fromField i x
        OracleTypeTimestampTz -> fmap (Exactly . localTimeToUTC tz) <$> fromField i x
        OracleTypeTimestampLtz -> fmap (Exactly . localTimeToUTC tz) <$> fromField i x
        _ -> convError "Exactly UTCTime" i x
  fromField i v = convError "Exactly UTCTime" i v
