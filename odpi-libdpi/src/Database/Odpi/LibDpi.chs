{-# options_ghc -fno-warn-missing-signatures -fno-warn-name-shadowing #-}
{-# language
    BangPatterns
  , DeriveGeneric
  , ForeignFunctionInterface
  , OverloadedStrings
#-}

module Database.Odpi.LibDpi where

#include <dpi.h>

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Data.Hashable (Hashable)
import Data.Int
import Data.Word
import Foreign.C.String
import Foreign.C.Types
import Foreign.Marshal.Utils (toBool)
import Foreign.Ptr
import Foreign.Storable
import GHC.Generics

{#context prefix="dpi" #}

-- * Constants

majorVersion :: CUInt
majorVersion = {#const DPI_MAJOR_VERSION #}

minorVersion :: CUInt
minorVersion = {#const DPI_MINOR_VERSION #}

dpiSuccess :: CInt
dpiSuccess = {#const DPI_SUCCESS #}

dpiFailure :: CInt
dpiFailure = {#const DPI_FAILURE #}

-- * Public Enumerations

{#enum define AuthMode
  { DPI_MODE_AUTH_DEFAULT as ModeAuthDefault
  , DPI_MODE_AUTH_SYSDBA as ModeAuthSysdba
  , DPI_MODE_AUTH_SYSOPER as ModeAuthSysoper
  , DPI_MODE_AUTH_PRELIM as ModeAuthPrelim
  , DPI_MODE_AUTH_SYSASM as ModeAuthSysasm
  , DPI_MODE_AUTH_SYSBKP as ModeAuthSysbkp
  , DPI_MODE_AUTH_SYSDGD as ModeAuthSysdgd
  , DPI_MODE_AUTH_SYSKMT as ModeAuthSyskmt
  , DPI_MODE_AUTH_SYSRAC as ModeAuthSysrac
  } deriving (Show, Eq) #}
{#enum define ConnCloseMode
  { DPI_MODE_CONN_CLOSE_DEFAULT as ModeConnCloseDefault
  , DPI_MODE_CONN_CLOSE_DROP as ModeConnCloseDrop
  , DPI_MODE_CONN_CLOSE_RETAG as ModeConnCloseRetag
  } deriving (Show, Eq) #}
{#enum define CreateMode
  { DPI_MODE_CREATE_DEFAULT as ModeCreateDefault
  , DPI_MODE_CREATE_THREADED as ModeCreateThreaded
  , DPI_MODE_CREATE_EVENTS as ModeCreateEvents
  } deriving (Show, Eq) #}
{#enum define DeqMode
  { DPI_MODE_DEQ_BROWSE as ModeDeqBrowse
  , DPI_MODE_DEQ_LOCKED as ModeDeqLocked
  , DPI_MODE_DEQ_REMOVE as ModeDeqRemove
  , DPI_MODE_DEQ_REMOVE_NO_DATA as ModeDeqRemoveNoData
  } deriving (Show, Eq) #}
{#enum define DeqNavigation
  { DPI_DEQ_NAV_FIRST_MSG as DeqNavFirstMsg
  , DPI_DEQ_NAV_NEXT_TRANSACTION as DeqNavNextTransaction
  , DPI_DEQ_NAV_NEXT_MSG as DeqNavNextMsg
  } deriving (Show, Eq) #}
{#enum define EventType
  { DPI_EVENT_NONE as EventNode
  , DPI_EVENT_STARTUP as EventStartup
  , DPI_EVENT_SHUTDOWN as EventShutdown
  , DPI_EVENT_SHUTDOWN_ANY as EventShutdownAny
  , DPI_EVENT_DEREG as EventDereg
  , DPI_EVENT_OBJCHANGE as EventObjchange
  , DPI_EVENT_QUERYCHANGE as EventQuerychange
  , DPI_EVENT_AQ as EventAq
  } deriving (Show, Eq) #}
{#enum define ExecMode
  { DPI_MODE_EXEC_DEFAULT as ModeExecDefault
  , DPI_MODE_EXEC_DESCRIBE_ONLY as ModeExecDescribeOnly
  , DPI_MODE_EXEC_COMMIT_ON_SUCCESS as ModeExecCommitOnSuccess
  , DPI_MODE_EXEC_BATCH_ERRORS as ModeExecBatchErrors
  , DPI_MODE_EXEC_PARSE_ONLY as ModeExecParseOnly
  , DPI_MODE_EXEC_ARRAY_DML_ROWCOUNTS as ModeExecArrayDmlRowcounts
  } deriving (Show, Eq) #}
{#enum define FetchMode
  { DPI_MODE_FETCH_NEXT as ModeFetchNext
  , DPI_MODE_FETCH_FIRST as ModeFetchFirst
  , DPI_MODE_FETCH_LAST as ModeFetchLast
  , DPI_MODE_FETCH_PRIOR as ModeFetchPrior
  , DPI_MODE_FETCH_ABSOLUTE as ModeFetchAbsolute
  , DPI_MODE_FETCH_RELATIVE as ModeFetchRelative
  } deriving (Show, Eq) #}
{#enum define MessageDeliveryMode
  { DPI_MODE_MSG_PERSISTENT as ModeMsgPersistent
  , DPI_MODE_MSG_BUFFERED as ModeMsgBuffered
  , DPI_MODE_MSG_PERSISTENT_OR_BUFFERED as ModeMsgPersistentOrBuffered
  } deriving (Show, Eq) #}
{#enum define MessageState
  { DPI_MSG_STATE_READY as MsgStateReady
  , DPI_MSG_STATE_WAITING as MsgStateWaiting
  , DPI_MSG_STATE_PROCESSED as MsgStateProcessed
  , DPI_MSG_STATE_EXPIRED as MsgStateExpired
  } deriving (Show, Eq) #}
{#enum define NativeTypeNum
  { DPI_NATIVE_TYPE_INT64 as NativeTypeInt64
  , DPI_NATIVE_TYPE_UINT64 as NativeTypeUint64
  , DPI_NATIVE_TYPE_FLOAT as NativeTypeFloat
  , DPI_NATIVE_TYPE_DOUBLE as NativeTypeDouble
  , DPI_NATIVE_TYPE_BYTES as NativeTypeBytes
  , DPI_NATIVE_TYPE_TIMESTAMP as NativeTypeTimestamp
  , DPI_NATIVE_TYPE_INTERVAL_DS as NativeTypeIntervalDs
  , DPI_NATIVE_TYPE_INTERVAL_YM as NativeTypeIntervalYm
  , DPI_NATIVE_TYPE_LOB as NativeTypeLob
  , DPI_NATIVE_TYPE_OBJECT as NativeTypeObject
  , DPI_NATIVE_TYPE_STMT as NativeTypeStmt
  , DPI_NATIVE_TYPE_BOOLEAN as NativeTypeBoolean
  , DPI_NATIVE_TYPE_ROWID as NativeTypeRowid
  } deriving (Show, Eq, Generic) #}
{#enum define OpCode
  { DPI_OPCODE_ALL_OPS as OpcodeAllOps
  , DPI_OPCODE_ALL_ROWS as OpcodeAllRows
  , DPI_OPCODE_INSERT as OpcodeInsert
  , DPI_OPCODE_UPDATE as OpcodeUpdate
  , DPI_OPCODE_DELETE as OpcodeDelete
  , DPI_OPCODE_ALTER as OpcodeAlter
  , DPI_OPCODE_DROP as OpcodeDrop
  , DPI_OPCODE_UNKNOWN as OpcodeUnknown
  } deriving (Show, Eq) #}
{#enum define OracleTypeNum
  { DPI_ORACLE_TYPE_NONE as OracleTypeNone
  , DPI_ORACLE_TYPE_VARCHAR as OracleTypeVarchar
  , DPI_ORACLE_TYPE_NVARCHAR as OracleTypeNvarchar
  , DPI_ORACLE_TYPE_CHAR as OracleTypeChar
  , DPI_ORACLE_TYPE_NCHAR as OracleTypeNchar
  , DPI_ORACLE_TYPE_ROWID as OracleTypeRowid
  , DPI_ORACLE_TYPE_RAW as OracleTypeRaw
  , DPI_ORACLE_TYPE_NATIVE_FLOAT as OracleTypeNativeFloat
  , DPI_ORACLE_TYPE_NATIVE_DOUBLE as OracleTypeNativeDouble
  , DPI_ORACLE_TYPE_NATIVE_INT as OracleTypeNativeInt
  , DPI_ORACLE_TYPE_NUMBER as OracleTypeNumber
  , DPI_ORACLE_TYPE_DATE as OracleTypeDate
  , DPI_ORACLE_TYPE_TIMESTAMP as OracleTypeTimestamp
  , DPI_ORACLE_TYPE_TIMESTAMP_TZ as OracleTypeTimestampTz
  , DPI_ORACLE_TYPE_TIMESTAMP_LTZ as OracleTypeTimestampLtz
  , DPI_ORACLE_TYPE_INTERVAL_DS as OracleTypeIntervalDs
  , DPI_ORACLE_TYPE_INTERVAL_YM as OracleTypeIntervalYm
  , DPI_ORACLE_TYPE_CLOB as OracleTypeClob
  , DPI_ORACLE_TYPE_NCLOB as OracleTypeNclob
  , DPI_ORACLE_TYPE_BLOB as OracleTypeBlob
  , DPI_ORACLE_TYPE_BFILE as OracleTypeBfile
  , DPI_ORACLE_TYPE_STMT as OracleTypeStmt
  , DPI_ORACLE_TYPE_BOOLEAN as OracleTypeBoolean
  , DPI_ORACLE_TYPE_OBJECT as OracleTypeObject
  , DPI_ORACLE_TYPE_LONG_VARCHAR as OracleTypeLongVarchar
  , DPI_ORACLE_TYPE_LONG_RAW as OracleTypeLongRaw
  , DPI_ORACLE_TYPE_NATIVE_UINT as OracleTypeNativeUint
  , DPI_ORACLE_TYPE_MAX as OracleTypeMax
  } deriving (Show, Eq) #}
{#enum define PoolCloseMode
  { DPI_MODE_POOL_CLOSE_DEFAULT as ModePoolCloseDefault
  , DPI_MODE_POOL_CLOSE_FORCE as ModePoolCloseForce
  } deriving (Show, Eq) #}
{#enum define PoolGetMode
  { DPI_MODE_POOL_GET_WAIT as ModePoolGetWait
  , DPI_MODE_POOL_GET_NOWAIT as ModePoolGetNowait
  , DPI_MODE_POOL_GET_FORCEGET as ModePoolGetForceget
  , DPI_MODE_POOL_GET_TIMEDWAIT as ModePoolGetTimedwait
  } deriving (Show, Eq) #}
{#enum define Purity
  { DPI_PURITY_DEFAULT as PurityDefault
  , DPI_PURITY_NEW as PurityNew
  , DPI_PURITY_SELF as PuritySelf
  } deriving (Show, Eq) #}
{#enum define ShutdownMode
  { DPI_MODE_SHUTDOWN_DEFAULT as ModeShutdownDefault
  , DPI_MODE_SHUTDOWN_TRANSACTIONAL as ModeShutdownTransactional
  , DPI_MODE_SHUTDOWN_TRANSACTIONAL_LOCAL as ModeShutdownTransactionalLocal
  , DPI_MODE_SHUTDOWN_IMMEDIATE as ModeShutdownImmediate
  , DPI_MODE_SHUTDOWN_ABORT as ModeShutdownAbort
  , DPI_MODE_SHUTDOWN_FINAL as ModeShutdownFinal
  } deriving (Show, Eq) #}
-- {#enum define SodaFlag {} deriving (Show, Eq) #}
{#enum define StartupMode
  { DPI_MODE_STARTUP_DEFAULT as ModeStartupDefault
  , DPI_MODE_STARTUP_FORCE as ModeStartupForce
  , DPI_MODE_STARTUP_RESTRICT as ModeStartupRestrict
  } deriving (Show, Eq) #}
{#enum define StatementType
  { DPI_STMT_TYPE_UNKNOWN as StmtTypeUnknown
  , DPI_STMT_TYPE_SELECT as StmtTypeSelect
  , DPI_STMT_TYPE_UPDATE as StmtTypeUpdate
  , DPI_STMT_TYPE_DELETE as StmtTypeDelete
  , DPI_STMT_TYPE_INSERT as StmtTypeInsert
  , DPI_STMT_TYPE_CREATE as StmtTypeCreate
  , DPI_STMT_TYPE_DROP as StmtTypeDrop
  , DPI_STMT_TYPE_ALTER as StmtTypeAlter
  , DPI_STMT_TYPE_BEGIN as StmtTypeBegin
  , DPI_STMT_TYPE_DECLARE as StmtTypeDeclare
  , DPI_STMT_TYPE_CALL as StmtTypeCall
  , DPI_STMT_TYPE_EXPLAIN_PLAN as StmtTypeExplainPlan
  , DPI_STMT_TYPE_MERGE as StmtTypeMerge
  , DPI_STMT_TYPE_ROLLBACK as StmtTypeRollback
  , DPI_STMT_TYPE_COMMIT as StmtTypeCommit
  } deriving (Show, Eq) #}
{#enum define SubscrGroupingClass
  { DPI_SUBSCR_GROUPING_CLASS_TIME as SubscrGroupingClassTime
  } deriving (Show, Eq) #}
{#enum define SubscrGroupingType
  { DPI_SUBSCR_GROUPING_TYPE_SUMMARY as SubscrGroupingTypeSummary
  , DPI_SUBSCR_GROUPING_TYPE_LAST as SubscrGroupingTypeLast
  } deriving (Show, Eq) #}
{#enum define SubscrNamespace
  { DPI_SUBSCR_NAMESPACE_AQ as SubscrNamespaceAq
  , DPI_SUBSCR_NAMESPACE_DBCHANGE as SubscrNamespaceDbchange
  } deriving (Show, Eq) #}
{#enum define SubscrProtocol
  { DPI_SUBSCR_PROTO_CALLBACK as SubscrProtoCallback
  , DPI_SUBSCR_PROTO_MAIL as SubscrProtoMail
  , DPI_SUBSCR_PROTO_PLSQL as SubscrProtoPlsql
  , DPI_SUBSCR_PROTO_HTTP as SubscrProtoHttp
  } deriving (Show, Eq) #}
{#enum define SubscrQOS
  { DPI_SUBSCR_QOS_RELIABLE as SubscrQosReliable
  , DPI_SUBSCR_QOS_DEREG_NFY as SubscrQosDeregNfy
  , DPI_SUBSCR_QOS_ROWIDS as SubscrQosRowids
  , DPI_SUBSCR_QOS_QUERY as SubscrQosQuery
  , DPI_SUBSCR_QOS_BEST_EFFORT as SubscrQosBestEffor
  } deriving (Show, Eq) #}
{#enum define Visibility
  { DPI_VISIBILITY_IMMEDIATE as VisibilityImmediate
  , DPI_VISIBILITY_ON_COMMIT as VisibilityOnCommit
  } deriving (Show, Eq) #}

instance Hashable NativeTypeNum

-- * Private Structures

{#pointer *Conn as DpiConn foreign newtype #}
{#pointer *Context as DpiContext foreign newtype #}
{#pointer *Lob as DpiLob foreign newtype #}
{#pointer *Object as DpiObject foreign newtype #}
{#pointer *ObjectType as DpiObjectType foreign newtype #}
{#pointer *Pool as DpiPool foreign newtype #}
{#pointer *Stmt as DpiStmt foreign newtype #}
{#pointer *Subscr as DpiSubscr foreign newtype #}
{#pointer *Rowid as DpiRowid foreign newtype #}
{#pointer *Var as DpiVar foreign newtype #}

-- * Public Structures

{#pointer *AppContext as PtrAppContext -> AppContext #}
{#pointer *Bytes as PtrBytes -> Bytes #}
{#pointer *CommonCreateParams as PtrCommonCreateParams -> CommonCreateParams #}
{#pointer *ConnCreateParams as PtrConnCreateParams -> ConnCreateParams #}
{#pointer *Data as PtrData -> Data #}
{#pointer *DataBuffer as PtrDataBuffer #}
{#pointer *DataTypeInfo as PtrDataTypeInfo -> DataTypeInfo #}
{#pointer *EncodingInfo as PtrEncodingInfo -> EncodingInfo #}
{#pointer *ErrorInfo as PtrErrorInfo -> ErrorInfo #}
{#pointer *IntervalDS as PtrIntervalDs -> IntervalDs #}
{#pointer *IntervalYM as PtrInvervalYM -> IntervalYm #}
{#pointer *ObjectAttrInfo as PtrObjectAttrInfo -> ObjectAttrInfo #}
{#pointer *ObjectTypeInfo as PtrObjectTypeInfo -> ObjectTypeInfo #}
{#pointer *PoolCreateParams as PtrPoolCreateParams -> PoolCreateParams #}
{#pointer *QueryInfo as PtrQueryInfo -> QueryInfo #}
{#pointer *ShardingKeyColumn as PtrShardingKeyColumn -> ShardingKeyColumn #}
{#pointer *StmtInfo as PtrStmtInfo -> StmtInfo #}
{#pointer *SubscrCreateParams as PtrSubscrCreateParams -> SubscrCreateParams #}
{#pointer *SubscrMessage as PtrSubscrMessage -> SubscrMessage #}
{#pointer *SubscrMessageQuery as PtrSubscrMessageQueyr -> SubscrMessageQuery #}
{#pointer *SubscrMessageRow as PtrSubscrMessageRow -> SubscrMessageRow #}
{#pointer *SubscrMessageTable as PtrSubscrMessageTable -> SubscrMessageTable #}
{#pointer *Timestamp as PtrTimestamp -> Timestamp #}
{#pointer *VersionInfo as PtrVersionInfo -> VersionInfo #}

toE :: (Integral n, Enum a) => n -> a
toE = toEnum . fromIntegral

fromE :: (Integral n, Enum a) => a -> n
fromE = fromIntegral . fromEnum

notImplemented :: IO a
notImplemented = error "not implemented"

data AppContext
  = AppContext
  { appContext_namespaceName :: CStringLen
  , appContext_name :: CStringLen
  , appContext_value :: CStringLen
  } deriving Show

instance Storable AppContext where
  sizeOf _ = {#sizeof AppContext #}
  alignment _ = {#alignof AppContext #}
  peek p = AppContext
    <$> ((,) <$> {#get AppContext->namespaceName #} p <*> fmap fromIntegral ({#get AppContext->namespaceNameLength #} p))
    <*> ((,) <$> {#get AppContext->name #} p <*> fmap fromIntegral ({#get AppContext->nameLength #} p))
    <*> ((,) <$> {#get AppContext->value #} p <*> fmap fromIntegral ({#get AppContext->valueLength #} p))
  poke p x = do
    {#set AppContext.namespaceName #} p (fst $ appContext_namespaceName x)
    {#set AppContext.namespaceNameLength #} p (fromIntegral $ snd $ appContext_namespaceName x)
    {#set AppContext.name #} p (fst $ appContext_name x)
    {#set AppContext.nameLength #} p (fromIntegral $ snd $ appContext_name x)
    {#set AppContext.value #} p (fst $ appContext_value x)
    {#set AppContext.valueLength #} p (fromIntegral $ snd $ appContext_value x)

data Bytes
  = Bytes
  { bytes_ptr :: CStringLen
  , bytes_encoding :: CString
  } deriving Show

instance Storable Bytes where
  sizeOf _ = {#sizeof Bytes #}
  alignment _ = {#alignof Bytes #}
  peek p = Bytes
    <$> ((,) <$> {#get Bytes->ptr #} p <*> fmap fromIntegral ({#get Bytes->length #} p))
    <*> {#get Bytes->encoding #} p
  poke p x = do
    {#set Bytes.ptr #} p (fst $ bytes_ptr x)
    {#set Bytes.length #} p (fromIntegral $ snd $ bytes_ptr x)
    {#set Bytes.encoding #} p (bytes_encoding x)

data CommonCreateParams
  = CommonCreateParams
  { commonCreateParams_createMode :: CreateMode
  , commonCreateParams_encoding :: CString
  , commonCreateParams_nencoding :: CString
  , commonCreateParams_edition :: CStringLen
  , commonCreateParams_driverName :: CStringLen
  } deriving Show

instance Storable CommonCreateParams where
  sizeOf _ = {#sizeof CommonCreateParams #}
  alignment _ = {#alignof CommonCreateParams #}
  peek p = CommonCreateParams
    <$> fmap toE ({#get CommonCreateParams->createMode #} p)
    <*> {#get CommonCreateParams->encoding #} p
    <*> {#get CommonCreateParams->nencoding #} p
    <*> ((,) <$> {#get CommonCreateParams->edition #} p <*> fmap fromIntegral ({#get CommonCreateParams->editionLength #} p))
    <*> ((,) <$> {#get CommonCreateParams->driverName #} p <*> fmap fromIntegral ({#get CommonCreateParams->driverNameLength #} p))
  poke p x = do
    {#set CommonCreateParams.createMode #} p (fromE $ commonCreateParams_createMode x)
    {#set CommonCreateParams.encoding #} p (commonCreateParams_encoding x)
    {#set CommonCreateParams.nencoding #} p (commonCreateParams_nencoding x)
    {#set CommonCreateParams.edition #} p (fst $ commonCreateParams_edition x)
    {#set CommonCreateParams.editionLength #} p (fromIntegral $ snd $ commonCreateParams_edition x)
    {#set CommonCreateParams.driverName #} p (fst $ commonCreateParams_driverName x)
    {#set CommonCreateParams.driverNameLength #} p (fromIntegral $ snd $ commonCreateParams_driverName x)

data ConnCreateParams
  = ConnCreateParams
  { connCreateParams_authMode :: AuthMode
  , connCreateParams_connectionClass :: CStringLen
  , connCreateParams_purity :: Purity
  , connCreateParams_newPassword :: CStringLen
  , connCreateParams_appContext :: (Ptr AppContext)
  , connCreateParams_numAppContext :: CUInt
  , connCreateParams_externalAuth :: CInt
  , connCreateParams_externalHandle :: Ptr ()
  , connCreateParams_pool :: (Ptr DpiPool)
  , connCreateParams_tag :: CStringLen
  , connCreateParams_matchAnyTag :: CInt
  , connCreateParams_outTag :: CStringLen
  , connCreateParams_outTagFound :: CInt
  , connCreateParams_shardingKeyColumns :: (Ptr ShardingKeyColumn)
  , connCreateParams_numShardingKeyColumns :: CUChar
  , connCreateParams_superShardingKeyColumns :: (Ptr ShardingKeyColumn)
  , connCreateParams_numSuperShardingKeyColumns :: CUChar
  } deriving Show

instance Storable ConnCreateParams where
  sizeOf _ = {#sizeof ConnCreateParams #}
  alignment _ = {#alignof ConnCreateParams #}
  peek p = ConnCreateParams
    <$> fmap toE ({#get ConnCreateParams->authMode #} p)
    <*> ((,) <$> {#get ConnCreateParams->connectionClass #} p <*> fmap fromIntegral ({#get ConnCreateParams->connectionClassLength #} p))
    <*> fmap toE ({#get ConnCreateParams->purity #} p)
    <*> ((,) <$> {#get ConnCreateParams->newPassword #} p <*> fmap fromIntegral ({#get ConnCreateParams->newPasswordLength #} p))
    <*> {#get ConnCreateParams->appContext #} p
    <*> {#get ConnCreateParams->numAppContext #} p
    <*> {#get ConnCreateParams->externalAuth #} p
    <*> {#get ConnCreateParams->externalHandle #} p
    <*> {#get ConnCreateParams->pool #} p
    <*> ((,) <$> {#get ConnCreateParams->tag #} p <*> fmap fromIntegral ({#get ConnCreateParams->tagLength #} p))
    <*> {#get ConnCreateParams->matchAnyTag #} p
    <*> ((,) <$> {#get ConnCreateParams->outTag #} p <*> fmap fromIntegral ({#get ConnCreateParams->outTagLength #} p))
    <*> {#get ConnCreateParams->outTagFound #} p
    <*> {#get ConnCreateParams->shardingKeyColumns #} p
    <*> {#get ConnCreateParams->numShardingKeyColumns #} p
    <*> {#get ConnCreateParams->superShardingKeyColumns #} p
    <*> {#get ConnCreateParams->numSuperShardingKeyColumns #} p
  poke p x = do
    {#set ConnCreateParams->authMode #} p (fromE $ connCreateParams_authMode x)
    {#set ConnCreateParams->connectionClass #} p (fst $ connCreateParams_connectionClass x)
    {#set ConnCreateParams->connectionClassLength #} p (fromIntegral $ snd $ connCreateParams_connectionClass x)
    {#set ConnCreateParams->purity #} p (fromE $ connCreateParams_purity x)
    {#set ConnCreateParams->newPassword #} p (fst $ connCreateParams_newPassword x)
    {#set ConnCreateParams->newPasswordLength #} p (fromIntegral $ snd $ connCreateParams_newPassword x)
    {#set ConnCreateParams->appContext #} p (connCreateParams_appContext x)
    {#set ConnCreateParams->numAppContext #} p (connCreateParams_numAppContext x)
    {#set ConnCreateParams->externalAuth #} p (connCreateParams_externalAuth x)
    {#set ConnCreateParams->externalHandle #} p (connCreateParams_externalHandle x)
    {#set ConnCreateParams->pool #} p (connCreateParams_pool x)
    {#set ConnCreateParams->tag #} p (fst $ connCreateParams_tag x)
    {#set ConnCreateParams->tagLength #} p (fromIntegral $ snd $ connCreateParams_tag x)
    {#set ConnCreateParams->matchAnyTag #} p (connCreateParams_matchAnyTag x)
    {#set ConnCreateParams->outTag #} p (fst $ connCreateParams_outTag x)
    {#set ConnCreateParams->outTagLength #} p (fromIntegral $ snd $ connCreateParams_outTag x)
    {#set ConnCreateParams->shardingKeyColumns #} p (connCreateParams_shardingKeyColumns x)
    {#set ConnCreateParams->numShardingKeyColumns #} p (connCreateParams_numShardingKeyColumns x)
    {#set ConnCreateParams->superShardingKeyColumns #} p (connCreateParams_superShardingKeyColumns x)
    {#set ConnCreateParams->numSuperShardingKeyColumns #} p (connCreateParams_numSuperShardingKeyColumns x)

data Data
  = Data
  { data_isNull :: Bool
  , data_value :: PtrDataBuffer
  } deriving Show

instance Storable Data where
  sizeOf _ = {#sizeof Data #}
  alignment _ = {#alignof Data #}
  peek p = Data
    <$> fmap toBool ({#get Data->isNull #} p)
    <*> {#get Data->value #} p
  poke _ _ = notImplemented

data DataTypeInfo
  = DataTypeInfo
  { dataTypeInfo_oracleTypeNum :: OracleTypeNum
  , dataTypeInfo_defaultNativeTypeNum :: NativeTypeNum
  , dataTypeInfo_ociTypeCode :: Word16
  , dataTypeInfo_dbSizeInBytes :: Word32
  , dataTypeInfo_clientSizeInBytes :: Word32
  , dataTypeInfo_sizeInChars :: Word32
  , dataTypeInfo_precision :: Int16
  , dataTypeInfo_scale :: Int8
  , dataTypeInfo_fsPrecision :: Int16
  , dataTypeInfo_objectType :: Ptr DpiObjectType
  } deriving Show

instance Storable DataTypeInfo where
  sizeOf _ = {#sizeof DataTypeInfo #}
  alignment _ = {#alignof DataTypeInfo #}
  peek p = DataTypeInfo
    <$> fmap toE ({#get DataTypeInfo->oracleTypeNum #} p)
    <*> fmap toE ({#get DataTypeInfo->defaultNativeTypeNum #} p)
    <*> fmap fromIntegral ({#get DataTypeInfo->ociTypeCode #} p)
    <*> fmap fromIntegral ({#get DataTypeInfo->dbSizeInBytes #} p)
    <*> fmap fromIntegral ({#get DataTypeInfo->clientSizeInBytes #} p)
    <*> fmap fromIntegral ({#get DataTypeInfo->sizeInChars #} p)
    <*> fmap fromIntegral ({#get DataTypeInfo->precision #} p)
    <*> fmap fromIntegral ({#get DataTypeInfo->scale #} p)
    <*> fmap fromIntegral ({#get DataTypeInfo->fsPrecision #} p)
    <*> {#get DataTypeInfo->objectType #} p
  poke _ _ = notImplemented

data EncodingInfo
  = EncodingInfo
  { encodingInfo_encoding :: CString
  , encodingInfo_maxBytesPerCharacter :: CInt
  , encodingInfo_nencoding :: CString
  , encodingInfo_nmaxBytesPerCaracter :: CInt
  } deriving Show

instance Storable EncodingInfo where
  sizeOf _ = {#sizeof EncodingInfo #}
  alignment _ = {#alignof EncodingInfo #}
  peek p = EncodingInfo
    <$> {#get EncodingInfo->encoding #} p
    <*> {#get EncodingInfo->maxBytesPerCharacter #} p
    <*> {#get EncodingInfo->nencoding #} p
    <*> {#get EncodingInfo->nmaxBytesPerCharacter #} p
  poke _ _ = notImplemented

data ErrorInfo
  = ErrorInfo
  { errorInfo_code :: Int32
  , errorInfo_offset :: Word16
  , errorInfo_message :: ByteString
  , errorInfo_encoding :: ByteString
  , errorInfo_fnName :: ByteString
  , errorInfo_action :: ByteString
  , errorInfo_sqlState :: ByteString
  , errorInfo_isRecoverable :: Bool
  } deriving Show

instance Storable ErrorInfo where
  sizeOf _ = {#sizeof ErrorInfo #}
  alignment _ = {#alignof ErrorInfo #}
  peek p = ErrorInfo
    <$> fmap fromIntegral ({#get ErrorInfo->code #} p)
    <*> fmap fromIntegral ({#get ErrorInfo->offset #} p)
    <*> (((,) <$> {#get ErrorInfo->message #} p <*> fmap fromIntegral ({#get ErrorInfo->messageLength #} p)) >>= B.packCStringLen)
    <*> ({#get ErrorInfo->encoding #} p >>= B.packCString)
    <*> ({#get ErrorInfo->fnName #} p >>= B.packCString)
    <*> ({#get ErrorInfo->action #} p >>= B.packCString)
    <*> ({#get ErrorInfo->sqlState #} p >>= B.packCString)
    <*> fmap toBool ({#get ErrorInfo->isRecoverable #} p)
  poke _ _ = notImplemented

data IntervalDs
  = IntervalDs
  { intervalDs_days :: Int32
  , intervalDs_hours :: Int32
  , intervalDs_minutes :: Int32
  , intervalDs_seconds :: Int32
  , intervalDs_fseconds :: Int32
  } deriving (Eq, Generic, Show)

instance Storable IntervalDs where
  sizeOf _ = {#sizeof IntervalDS #}
  alignment _ = {#alignof IntervalDS #}
  peek p = IntervalDs
    <$> fmap fromIntegral ({#get IntervalDS->days #} p)
    <*> fmap fromIntegral ({#get IntervalDS->hours #} p)
    <*> fmap fromIntegral ({#get IntervalDS->minutes #} p)
    <*> fmap fromIntegral ({#get IntervalDS->seconds #} p)
    <*> fmap fromIntegral ({#get IntervalDS->fseconds #} p)
  poke p x = do
    {#set IntervalDS->days #} p (fromIntegral $ intervalDs_days x)
    {#set IntervalDS->hours #} p (fromIntegral $ intervalDs_hours x)
    {#set IntervalDS->minutes #} p (fromIntegral $ intervalDs_minutes x)
    {#set IntervalDS->seconds #} p (fromIntegral $ intervalDs_seconds x)
    {#set IntervalDS->fseconds #} p (fromIntegral $ intervalDs_fseconds x)

instance Hashable IntervalDs

data IntervalYm
  = IntervalYm
  { intervalYm_years :: Int32
  , intervalYm_months :: Int32
  } deriving (Eq, Generic, Show)

instance Storable IntervalYm where
  sizeOf _ = {#sizeof IntervalYM #}
  alignment _ = {#alignof IntervalYM #}
  peek p = IntervalYm
    <$> fmap fromIntegral ({#get IntervalYM->years #} p)
    <*> fmap fromIntegral ({#get IntervalYM->months #} p)
  poke p x = do
    {#set IntervalYM->years #} p (fromIntegral $ intervalYm_years x)
    {#set IntervalYM->months #} p (fromIntegral $ intervalYm_months x)

instance Hashable IntervalYm

data ObjectAttrInfo
  = ObjectAttrInfo
  { objectAttrInfo_name :: CStringLen
  , objectAttrInfo_typeInfo :: DataTypeInfo
  } deriving Show

data ObjectTypeInfo
  = ObjectTypeInfo
  { objectTypeInfo_schema :: CStringLen
  , objectTypeInfo_name :: CStringLen
  , objectTypeInfo_isCollection :: CInt
  , objectTypeInfo_elementTypeInfo :: DataTypeInfo
  , objectTypeInfo_numAttributes :: Word16
  } deriving Show

data PoolCreateParams
  = PoolCreateParams
  { poolCreateParams_minSessions :: CUInt
  , poolCreateParams_maxSessions :: CUInt
  , poolCreateParams_sessionIncrement :: CUInt
  , poolCreateParams_pingInterval :: CInt
  , poolCreateParams_pingTimeout :: CInt
  , poolCreateParams_externalAuth :: CInt
  , poolCreateParams_getMode :: PoolGetMode
  , poolCreateParams_outPoolName :: CStringLen
  , poolCreateParams_timeout :: CUInt
  , poolCreateParams_waitTimetout :: CUInt
  , poolCreateParams_maxLifetimeSession :: CUInt
  } deriving Show

instance Storable PoolCreateParams where
  sizeOf _ = {#sizeof VersionInfo #}
  alignment _ = {#alignof VersionInfo #}
  peek p = PoolCreateParams
    <$> {#get PoolCreateParams->minSessions #} p
    <*> {#get PoolCreateParams->maxSessions #} p
    <*> {#get PoolCreateParams->sessionIncrement #} p
    <*> {#get PoolCreateParams->pingInterval #} p
    <*> {#get PoolCreateParams->pingTimeout #} p
    <*> {#get PoolCreateParams->externalAuth #} p
    <*> fmap toE ({#get PoolCreateParams->getMode #} p)
    <*> ((,) <$> {#get PoolCreateParams->outPoolName #} p <*> fmap fromIntegral ({#get PoolCreateParams->outPoolNameLength #} p))
    <*> {#get PoolCreateParams->timeout #} p
    <*> {#get PoolCreateParams->waitTimeout #} p
    <*> {#get PoolCreateParams->maxLifetimeSession #} p
  poke _ _ = notImplemented

data QueryInfo
  = QueryInfo
  { queryInfo_name :: ByteString
  , queryInfo_typeInfo :: DataTypeInfo
  , queryInfo_nullOk :: Bool
  } deriving Show

instance Storable QueryInfo where
  sizeOf _ = {#sizeof QueryInfo #}
  alignment _ = {#alignof QueryInfo #}
  peek p = QueryInfo
    <$> (((,) <$> {#get QueryInfo->name #} p <*> fmap fromIntegral ({#get QueryInfo->nameLength #} p)) >>= B.packCStringLen)
    <*> (DataTypeInfo
          <$> fmap toE ({#get QueryInfo->typeInfo.oracleTypeNum #} p)
          <*> fmap toE ({#get QueryInfo->typeInfo.defaultNativeTypeNum #} p)
          <*> fmap fromIntegral ({#get QueryInfo->typeInfo.ociTypeCode #} p)
          <*> fmap fromIntegral ({#get QueryInfo->typeInfo.dbSizeInBytes #} p)
          <*> fmap fromIntegral ({#get QueryInfo->typeInfo.clientSizeInBytes #} p)
          <*> fmap fromIntegral ({#get QueryInfo->typeInfo.sizeInChars #} p)
          <*> fmap fromIntegral ({#get QueryInfo->typeInfo.precision #} p)
          <*> fmap fromIntegral ({#get QueryInfo->typeInfo.scale #} p)
          <*> fmap fromIntegral ({#get QueryInfo->typeInfo.fsPrecision #} p)
          <*> {#get QueryInfo->typeInfo.objectType #} p)
    <*> fmap toBool ({#get QueryInfo->nullOk #} p)
  poke _ _ = notImplemented

data ShardingKeyColumn
  = ShardingKeyColumn
  { shardingKeyColumn_oracleTypeNum :: OracleTypeNum
  , shardingKeyColumn_nativeTypeNum :: NativeTypeNum
  , shardingKeyColumn_value :: PtrDataBuffer
  } deriving Show

data StmtInfo
  = StmtInfo
  { stmtInfo_isQuery :: CInt
  , stmtInfo_isPLSQL :: CInt
  , stmtInfo_isDDL :: CInt
  , stmtInfo_isDML :: CInt
  , stmtInfo_statementType :: StatementType
  , stmtInfo_isReturning :: CInt
  } deriving Show

type SubscrCallback = FunPtr (Ptr () -> Ptr SubscrMessage -> IO ())

data SubscrCreateParams
  = SubscrCreateParams
  { subscrCreateParams_subscrNamespace :: SubscrNamespace
  , subscrCreateParams_protocol :: SubscrProtocol
  , subscrCreateParams_qos :: SubscrQOS
  , subscrCreateParams_operations :: OpCode
  , subscrCreateParams_portNumber :: Word32
  , subscrCreateParams_timeout :: Word32
  , subscrCreateParams_name :: CStringLen
  , subscrCreateParams_callback :: SubscrCallback
  , subscrCreateParams_callbackContext :: Ptr ()
  , subscrCreateParams_recipientName :: CStringLen
  , subscrCreateParams_ipAddress :: CStringLen
  , subscrCreateParams_groupingClass :: Word8
  , subscrCreateParams_groupingValue :: Word32
  , subscrCreateParams_groupingType :: Word8
  } deriving Show

data SubscrMessage
  = SubscrMessage
  { subscrMessage_eventType :: EventType
  , subscrMessage_dbName :: CStringLen
  , subscrMessage_tables :: Ptr SubscrMessageTable
  , subscrMessage_numTables :: Word32
  , subscrMessage_queries :: Ptr SubscrMessageQuery
  , subscrMessage_numQueries :: Word32
  , subscrMessage_errorInfo :: Ptr ErrorInfo
  , subscrMessage_txId :: Ptr ()
  , subscrMessage_txIdLength :: Word32
  , subscrMessage_registered :: CInt
  , subscrMessage_queueName :: CStringLen
  , subscrMessage_consumerName :: CStringLen
  } deriving Show

data SubscrMessageQuery
  = SubscrMessageQuery
  { subscrMessageQuery_id :: Word64
  , subscrMessageQuery_operation :: OpCode
  , subscrMessageQuery_tables :: Ptr SubscrMessageTable
  , subscrMessageQuery_numTables :: Word32
  } deriving Show

data SubscrMessageRow
  = SubscrMessageRow
  { subscrMessageRow_operation :: OpCode
  , subscrMessageRow_rowid :: CStringLen
  } deriving Show

data SubscrMessageTable
  = SubscrMessageTable
  { subscrMessageTable_operation :: OpCode
  , subscrMessageTable_name :: CStringLen
  , subscrMessageTable_rows :: Ptr SubscrMessageRow
  , subscrMessageTable_numRows :: Word32
  } deriving Show

data Timestamp
  = Timestamp
  { timestamp_year :: Int16
  , timestamp_month :: Word8
  , timestamp_day :: Word8
  , timestamp_hour :: Word8
  , timestamp_minute :: Word8
  , timestamp_second :: Word8
  , timestamp_fsecond :: Word32
  , timestamp_tzHourOffset :: Int8
  , timestamp_tzMinuteOffset :: Int8
  } deriving (Eq, Generic, Show)

instance Storable Timestamp where
  sizeOf _ = {#sizeof Timestamp #}
  alignment _ = {#alignof Timestamp #}
  peek p = Timestamp
    <$> fmap fromIntegral ({#get Timestamp->year #} p)
    <*> fmap fromIntegral ({#get Timestamp->month #} p)
    <*> fmap fromIntegral ({#get Timestamp->day #} p)
    <*> fmap fromIntegral ({#get Timestamp->hour #} p)
    <*> fmap fromIntegral ({#get Timestamp->minute #} p)
    <*> fmap fromIntegral ({#get Timestamp->second #} p)
    <*> fmap fromIntegral ({#get Timestamp->fsecond #} p)
    <*> fmap fromIntegral ({#get Timestamp->tzHourOffset #} p)
    <*> fmap fromIntegral ({#get Timestamp->tzMinuteOffset #} p)
  poke p x = do
    {#set Timestamp->year #} p (fromIntegral $ timestamp_year x)
    {#set Timestamp->month #} p (fromIntegral $ timestamp_month x)
    {#set Timestamp->day #} p (fromIntegral $ timestamp_day x)
    {#set Timestamp->hour #} p (fromIntegral $ timestamp_hour x)
    {#set Timestamp->minute #} p (fromIntegral $ timestamp_minute x)
    {#set Timestamp->second #} p (fromIntegral $ timestamp_second x)
    {#set Timestamp->fsecond #} p (fromIntegral $ timestamp_fsecond x)
    {#set Timestamp->tzHourOffset #} p (fromIntegral $ timestamp_tzHourOffset x)
    {#set Timestamp->tzMinuteOffset #} p (fromIntegral $ timestamp_tzMinuteOffset x)

instance Hashable Timestamp

data VersionInfo
  = VersionInfo
  { versionInfo_versionNum :: CInt
  , versionInfo_releaseNum :: CInt
  , versionInfo_updateNum :: CInt
  , versionInfo_portReleaseNum :: CInt
  , versionInfo_portUpdateNum :: CInt
  , versionInfo_fullVersionNum :: CUInt
  } deriving Show

instance Storable VersionInfo where
  sizeOf _ = {#sizeof VersionInfo #}
  alignment _ = {#alignof VersionInfo #}
  peek p = VersionInfo
    <$> {#get VersionInfo->versionNum #} p
    <*> {#get VersionInfo->releaseNum #} p
    <*> {#get VersionInfo->updateNum #} p
    <*> {#get VersionInfo->portReleaseNum #} p
    <*> {#get VersionInfo->portUpdateNum #} p
    <*> {#get VersionInfo->fullVersionNum #} p
  poke _ _ = notImplemented

-- * Public Functions

-- ** Connection

conn_addRef = {#call Conn_addRef #}
conn_beginDistribTrans = {#call Conn_beginDistribTrans #}
conn_breakExecution = {#call Conn_breakExecution #}
conn_changePassword = {#call Conn_changePassword #}
conn_close = {#call Conn_close #}
conn_commit = {#call Conn_commit #}
conn_create = {#call Conn_create #}
conn_deqObject = {#call Conn_deqObject #}
conn_enqObject = {#call Conn_enqObject #}
conn_getCallTimeout = {#call Conn_getCallTimeout #}
conn_getCurrentSchema = {#call Conn_getCurrentSchema #}
conn_getEdition = {#call Conn_getEdition #}
conn_getEncodingInfo = {#call Conn_getEncodingInfo #}
conn_getExternalName = {#call Conn_getExternalName #}
conn_getHandle = {#call Conn_getHandle #}
conn_getInternalName = {#call Conn_getInternalName #}
conn_getLTXID = {#call Conn_getLTXID #}
conn_getObjectType = {#call Conn_getObjectType #}
conn_getServerVersion = {#call Conn_getServerVersion #}
conn_getSodaDb = {#call Conn_getSodaDb #}
conn_getStmtCacheSize = {#call Conn_getStmtCacheSize #}
conn_newDeqOptions = {#call Conn_newDeqOptions #}
conn_newEnqOptions = {#call Conn_newEnqOptions #}
conn_newMsgProps = {#call Conn_newMsgProps #}
conn_newTempLob = {#call Conn_newTempLob #}
conn_newVar = {#call Conn_newVar #}
conn_ping = {#call Conn_ping #}
conn_prepareDistribTrans = {#call Conn_prepareDistribTrans #}
conn_prepareStmt = {#call Conn_prepareStmt #}
conn_release = {#call Conn_release #}
conn_rollback = {#call Conn_rollback #}
conn_setAction = {#call Conn_setAction #}
conn_setCallTimeout = {#call Conn_setCallTimeout #}
conn_setClientIdentifier = {#call Conn_setClientIdentifier #}
conn_setClientInfo = {#call Conn_setClientInfo #}
conn_setCurrentSchema = {#call Conn_setCurrentSchema #}
conn_setDbOp = {#call Conn_setDbOp #}
conn_setExternalName = {#call Conn_setExternalName #}
conn_setInternalName = {#call Conn_setInternalName #}
conn_setModule = {#call Conn_setModule #}
conn_setStmtCacheSize = {#call Conn_setStmtCacheSize #}
conn_shutdownDatabase = {#call Conn_shutdownDatabase #}
conn_startupDatabase = {#call Conn_startupDatabase #}
conn_subscribe = {#call Conn_subscribe #}
conn_unsubscribe = {#call Conn_unsubscribe #}

-- ** Context

context_createWithParams = {#call Context_createWithParams #}
context_destroy = {#call Context_destroy #}
context_getClientVersion = {#call Context_getClientVersion #}
context_getError = {#call Context_getError #}
context_initCommonCreateParams = {#call Context_initCommonCreateParams #}
context_initConnCreateParams = {#call Context_initConnCreateParams #}
context_initPoolCreateParams = {#call Context_initPoolCreateParams #}
context_initSodaOperOptions = {#call Context_initSodaOperOptions #}
context_initSubscrCreateParams = {#call Context_initSubscrCreateParams #}

-- ** Data

data_getBool = {#call Data_getBool #}
data_getBytes = {#call Data_getBytes #}
data_getDouble = {#call Data_getDouble #}
data_getFloat = {#call Data_getFloat #}
data_getInt64 = {#call Data_getInt64 #}
data_getIntervalDS = {#call Data_getIntervalDS #}
data_getIntervalYM = {#call Data_getIntervalYM #}
data_getIsNull = {#call Data_getIsNull #}
data_getLOB = {#call Data_getLOB #}
data_getObject = {#call Data_getObject #}
data_getStmt = {#call Data_getStmt #}
data_getTimestamp = {#call Data_getTimestamp #}
data_getUint64 = {#call Data_getUint64 #}
data_setBool = {#call Data_setBool #}
data_setBytes = {#call Data_setBytes #}
data_setDouble = {#call Data_setDouble #}
data_setFloat = {#call Data_setFloat #}
data_setInt64 = {#call Data_setInt64 #}
data_setIntervalDS = {#call Data_setIntervalDS #}
data_setIntervalYM = {#call Data_setIntervalYM #}
data_setLOB = {#call Data_setLOB #}
data_setNull = {#call Data_setNull #}
data_setObject = {#call Data_setObject #}
data_setStmt = {#call Data_setStmt #}
data_setTimestamp = {#call Data_setTimestamp #}
data_setUint64 = {#call Data_setUint64 #}

-- ** DeqOptions

deqOptions_addRef = {#call DeqOptions_addRef #}
deqOptions_getCondition = {#call DeqOptions_getCondition #}
deqOptions_getConsumerName = {#call DeqOptions_getConsumerName #}
deqOptions_getCorrelation = {#call DeqOptions_getCorrelation #}
deqOptions_getMode = {#call DeqOptions_getMode #}
deqOptions_getMsgId = {#call DeqOptions_getMsgId #}
deqOptions_getNavigation = {#call DeqOptions_getNavigation #}
deqOptions_getTransformation = {#call DeqOptions_getTransformation #}
deqOptions_getVisibility = {#call DeqOptions_getVisibility #}
deqOptions_getWait = {#call DeqOptions_getWait #}
deqOptions_release = {#call DeqOptions_release #}
deqOptions_setCondition = {#call DeqOptions_setCondition #}
deqOptions_setConsumerName = {#call DeqOptions_setConsumerName #}
deqOptions_setCorrelation = {#call DeqOptions_setCorrelation #}
deqOptions_setDeliveryMode = {#call DeqOptions_setDeliveryMode #}
deqOptions_setMode = {#call DeqOptions_setMode #}
deqOptions_setMsgId = {#call DeqOptions_setMsgId #}
deqOptions_setNavigation = {#call DeqOptions_setNavigation #}
deqOptions_setTransformation = {#call DeqOptions_setTransformation #}
deqOptions_setVisibility = {#call DeqOptions_setVisibility #}
deqOptions_setWait = {#call DeqOptions_setWait #}

-- ** EnqOptions

enqOptions_addRef = {#call EnqOptions_addRef #}
enqOptions_getTransformation = {#call EnqOptions_getTransformation #}
enqOptions_getVisibility = {#call EnqOptions_getVisibility #}
enqOptions_release = {#call EnqOptions_release #}
enqOptions_setDeliveryMode = {#call EnqOptions_setDeliveryMode #}
enqOptions_setTransformation = {#call EnqOptions_setTransformation #}
enqOptions_setVisibility = {#call EnqOptions_setVisibility #}

-- ** LOB

lob_addRef = {#call Lob_addRef #}
lob_close = {#call Lob_close #}
lob_closeResource = {#call Lob_closeResource #}
lob_copy = {#call Lob_copy #}
lob_getBufferSize = {#call Lob_getBufferSize #}
lob_getChunkSize = {#call Lob_getChunkSize #}
lob_getDirectoryAndFileName = {#call Lob_getDirectoryAndFileName #}
lob_getFileExists = {#call Lob_getFileExists #}
lob_getIsResourceOpen = {#call Lob_getIsResourceOpen #}
lob_getSize = {#call Lob_getSize #}
lob_openResource = {#call Lob_openResource #}
lob_readBytes = {#call Lob_readBytes #}
lob_release = {#call Lob_release #}
lob_setDirectoryAndFileName = {#call Lob_setDirectoryAndFileName #}
lob_setFromBytes = {#call Lob_setFromBytes #}
lob_trim = {#call Lob_trim #}
lob_writeBytes = {#call Lob_writeBytes #}

-- ** MsgProps

msgProps_addRef = {#call MsgProps_addRef #}
msgProps_getNumAttempts = {#call MsgProps_getNumAttempts #}
msgProps_getCorrelation = {#call MsgProps_getCorrelation #}
msgProps_getDelay = {#call MsgProps_getDelay #}
msgProps_getDeliveryMode = {#call MsgProps_getDeliveryMode #}
msgProps_getEnqTime = {#call MsgProps_getEnqTime #}
msgProps_getExceptionQ = {#call MsgProps_getExceptionQ #}
msgProps_getExpiration = {#call MsgProps_getExpiration #}
--msgProps_getMsgId = {#call MsgProps_getMsgId #}
msgProps_getOriginalMsgId = {#call MsgProps_getOriginalMsgId #}
--msgProps_getPayload = {#call MsgProps_getPayload #}
msgProps_getPriority = {#call MsgProps_getPriority #}
msgProps_getState = {#call MsgProps_getState #}
msgProps_release = {#call MsgProps_release #}
msgProps_setCorrelation = {#call MsgProps_setCorrelation #}
msgProps_setDelay = {#call MsgProps_setDelay #}
msgProps_setExceptionQ = {#call MsgProps_setExceptionQ #}
msgProps_setExpiration = {#call MsgProps_setExpiration #}
msgProps_setOriginalMsgId = {#call MsgProps_setOriginalMsgId #}
--msgProps_setPayloadBytes = {#call MsgProps_setPayloadBytes #}
--msgProps_setPayloadObject = {#call MsgProps_setPayloadObject #}
msgProps_setPriority = {#call MsgProps_setPriority #}

-- ** Object

object_addRef = {#call Object_addRef #}
object_appendElement = {#call Object_appendElement #}
object_copy = {#call Object_copy #}
object_deleteElementByIndex = {#call Object_deleteElementByIndex #}
object_getAttributeValue = {#call Object_getAttributeValue #}
object_getElementExistsByIndex = {#call Object_getElementExistsByIndex #}
object_getElementValueByIndex = {#call Object_getElementValueByIndex #}
object_getFirstIndex = {#call Object_getFirstIndex #}
object_getLastIndex = {#call Object_getLastIndex #}
object_getNextIndex = {#call Object_getNextIndex #}
object_getPrevIndex = {#call Object_getPrevIndex #}
object_getSize = {#call Object_getSize #}
object_release = {#call Object_release #}
object_setAttributeValue = {#call Object_setAttributeValue #}
object_setElementValueByIndex = {#call Object_setElementValueByIndex #}
object_trim = {#call Object_trim #}

-- ** ObjectAttr

objectAttr_addRef = {#call ObjectAttr_addRef #}
objectAttr_getInfo = {#call ObjectAttr_getInfo #}
objectAttr_release = {#call ObjectAttr_release #}

-- ** ObjectType

objectType_addRef = {#call ObjectType_addRef #}
objectType_createObject = {#call ObjectType_createObject #}
objectType_getAttributes = {#call ObjectType_getAttributes #}
objectType_getInfo = {#call ObjectType_getInfo #}
objectType_release = {#call ObjectType_release #}

-- ** Pool

pool_acquireConnection = {#call Pool_acquireConnection #}
pool_addRef = {#call Pool_addRef #}
pool_close = {#call Pool_close #}
pool_create = {#call Pool_create #}
pool_getBusyCount = {#call Pool_getBusyCount #}
pool_getEncodingInfo = {#call Pool_getEncodingInfo #}
pool_getGetMode = {#call Pool_getGetMode #}
pool_getMaxLifetimeSession = {#call Pool_getMaxLifetimeSession #}
pool_getOpenCount = {#call Pool_getOpenCount #}
pool_getStmtCacheSize = {#call Pool_getStmtCacheSize #}
pool_getTimeout = {#call Pool_getTimeout #}
pool_getWaitTimeout = {#call Pool_getWaitTimeout #}
pool_release = {#call Pool_release #}
pool_setGetMode = {#call Pool_setGetMode #}
pool_setMaxLifetimeSession = {#call Pool_setMaxLifetimeSession #}
pool_setStmtCacheSize = {#call Pool_setStmtCacheSize #}
pool_setTimeout = {#call Pool_setTimeout #}
pool_setWaitTimeout = {#call Pool_setWaitTimeout #}

-- ** AQ Queue

-- queue_addRef = {#call Queue_addRef #}
-- queue_deqMany = {#call Queue_deqMany #}
-- queue_deqOne = {#call Queue_deqOne #}
-- queue_enqMany = {#call Queue_enqMany #}
-- queue_enqOne = {#call Queue_enqOne #}
-- queue_getDeqOptions = {#call Queue_getDeqOptions #}
-- queue_getEnqOptions = {#call Queue_getEnqOptions #}
-- queue_release = {#call Queue_release #}

-- ** Rowid

rowid_addRef = {#call Rowid_addRef #}
rowid_getStringValue = {#call Rowid_getStringValue #}
rowid_release = {#call Rowid_release #}

-- ** Stmt

stmt_addRef = {#call Stmt_addRef #}
stmt_bindByName = {#call Stmt_bindByName #}
stmt_bindByPos = {#call Stmt_bindByPos #}
stmt_bindValueByName = {#call Stmt_bindValueByName #}
stmt_bindValueByPos = {#call Stmt_bindValueByPos #}
stmt_close = {#call Stmt_close #}
stmt_define = {#call Stmt_define #}
stmt_defineValue = {#call Stmt_defineValue #}
stmt_execute = {#call Stmt_execute #}
stmt_executeMany = {#call Stmt_executeMany #}
stmt_fetch = {#call Stmt_fetch #}
stmt_fetchRows = {#call Stmt_fetchRows #}
stmt_getBatchErrorCount = {#call Stmt_getBatchErrorCount #}
stmt_getBatchErrors = {#call Stmt_getBatchErrors #}
stmt_getBindCount = {#call Stmt_getBindCount #}
stmt_getBindNames = {#call Stmt_getBindNames #}
stmt_getFetchArraySize = {#call Stmt_getFetchArraySize #}
stmt_getImplicitResult = {#call Stmt_getImplicitResult #}
stmt_getInfo = {#call Stmt_getInfo #}
stmt_getNumQueryColumns = {#call Stmt_getNumQueryColumns #}
stmt_getQueryInfo = {#call Stmt_getQueryInfo #}
stmt_getQueryValue = {#call Stmt_getQueryValue #}
stmt_getRowCount = {#call Stmt_getRowCount #}
stmt_getRowCounts = {#call Stmt_getRowCounts #}
stmt_getSubscrQueryId = {#call Stmt_getSubscrQueryId #}
stmt_release = {#call Stmt_release #}
stmt_scroll = {#call Stmt_scroll #}
stmt_setFetchArraySize = {#call Stmt_setFetchArraySize #}

-- ** Subscr

subscr_addRef = {#call Subscr_addRef #}
subscr_prepareStmt = {#call Subscr_prepareStmt #}
subscr_release = {#call Subscr_release #}

-- ** Var

var_addRef = {#call Var_addRef #}
var_copyData = {#call Var_copyData #}
var_getNumElementsInArray = {#call Var_getNumElementsInArray #}
var_getReturnedData = {#call Var_getReturnedData #}
var_getSizeInBytes = {#call Var_getSizeInBytes #}
var_release = {#call Var_release #}
var_setFromBytes = {#call Var_setFromBytes #}
var_setFromLob = {#call Var_setFromLob #}
var_setFromObject = {#call Var_setFromObject #}
var_setFromRowid = {#call Var_setFromRowid #}
var_setFromStmt = {#call Var_setFromStmt #}
var_setNumElementsInArray = {#call Var_setNumElementsInArray #}


