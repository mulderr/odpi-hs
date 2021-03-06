module Database.Odpi.Context
  ( module Database.Odpi.Context
  , contextGetError -- defined in Util
  ) where

import Foreign.Marshal.Alloc ( alloca )
import Foreign.Ptr (nullPtr)
import Foreign.Storable ( Storable(peek) )

import Database.Odpi.LibDpi
import Database.Odpi.Types
import Database.Odpi.Util

contextCreateEither :: IO (Either ErrorInfo Context)
contextCreateEither =
  alloca $ \contextPP -> do
    alloca $ \errorInfoP -> do
      r <- context_createWithParams majorVersion minorVersion nullPtr contextPP errorInfoP
      if isOk r
        then fmap (Right . Context) $ peek contextPP
        else fmap Left $ peek errorInfoP

contextCreate :: IO Context
contextCreate = contextCreateEither >>= throwLeft

contextDestroy :: Context -> IO Bool
contextDestroy = fmap isOk . context_destroy . unContext

contextGetClientVersion :: Context -> IO VersionInfo
contextGetClientVersion (Context ctx) = out1 ctx (context_getClientVersion ctx)
