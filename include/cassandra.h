/*
  Copyright (c) 2014 DataStax

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#ifndef __CASSANDRA_H_INCLUDED__
#define __CASSANDRA_H_INCLUDED__

#include <stddef.h>

#if defined _WIN32
#   if defined CASS_STATIC
#       define CASS_EXPORT
#   elif defined DLL_EXPORT
#       define CASS_EXPORT __declspec(dllexport)
#   else
#       define CASS_EXPORT __declspec(dllimport)
#   endif
#else
#   if defined __SUNPRO_C  || defined __SUNPRO_CC
#       define CASS_EXPORT __global
#   elif (defined __GNUC__ && __GNUC__ >= 4) || defined __INTEL_COMPILER
#       define CASS_EXPORT __attribute__ ((visibility("default")))
#   else
#       define CASS_EXPORT
#   endif
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef enum { cass_false = 0, cass_true = 1 } cass_bool_t;
typedef float cass_float_t;
typedef double cass_double_t;
typedef unsigned char cass_byte_t;
typedef unsigned int cass_duration_t;
typedef char cass_int8_t;
typedef short cass_int16_t;
typedef int cass_int32_t;
typedef long long cass_int64_t;
typedef unsigned char cass_uint8_t;
typedef unsigned short cass_uint16_t;
typedef unsigned int cass_uint32_t;
typedef unsigned long long cass_uint64_t;
typedef size_t cass_size_t;

typedef struct CassBytes_ {
    const cass_byte_t* data;
    cass_size_t size;
} CassBytes;

typedef struct CassString_ {
    const char* data;
    cass_size_t length;
} CassString;

typedef struct CassInet_ {
  cass_uint8_t address[16];
  cass_uint8_t address_length;
} CassInet;

typedef struct CassDecimal_ {
  cass_int32_t scale;
  CassBytes varint;
} CassDecimal;

#define CASS_UUID_STRING_LENGTH 37

typedef cass_uint8_t CassUuid[16];

typedef struct CassCluster_ CassCluster;
typedef struct CassSession_ CassSession;
typedef struct CassStatement_ CassStatement;
typedef struct CassBatch_ CassBatch;
typedef struct CassFuture_ CassFuture;
typedef struct CassPrepared_ CassPrepared;
typedef struct CassResult_ CassResult;
typedef struct CassIterator_ CassIterator;
typedef struct CassRow_ CassRow;
typedef struct CassValue_ CassValue;
typedef struct CassCollection_ CassCollection;

typedef enum CassConsistency_ {
  CASS_CONSISTENCY_ANY          = 0x0000,
  CASS_CONSISTENCY_ONE          = 0x0001,
  CASS_CONSISTENCY_TWO          = 0x0002,
  CASS_CONSISTENCY_THREE        = 0x0003,
  CASS_CONSISTENCY_QUORUM       = 0x0004,
  CASS_CONSISTENCY_ALL          = 0x0005,
  CASS_CONSISTENCY_LOCAL_QUORUM = 0x0006,
  CASS_CONSISTENCY_EACH_QUORUM  = 0x0007,
  CASS_CONSISTENCY_SERIAL       = 0x0008,
  CASS_CONSISTENCY_LOCAL_SERIAL = 0x0009,
  CASS_CONSISTENCY_LOCAL_ONE    = 0x000A
} CassConsistency;

typedef enum CassValueType_ {
  CASS_VALUE_TYPE_UNKNOWN   = 0xFFFF,
  CASS_VALUE_TYPE_CUSTOM    = 0x0000,
  CASS_VALUE_TYPE_ASCII     = 0x0001,
  CASS_VALUE_TYPE_BIGINT    = 0x0002,
  CASS_VALUE_TYPE_BLOB      = 0x0003,
  CASS_VALUE_TYPE_BOOLEAN   = 0x0004,
  CASS_VALUE_TYPE_COUNTER   = 0x0005,
  CASS_VALUE_TYPE_DECIMAL   = 0x0006,
  CASS_VALUE_TYPE_DOUBLE    = 0x0007,
  CASS_VALUE_TYPE_FLOAT     = 0x0008,
  CASS_VALUE_TYPE_INT       = 0x0009,
  CASS_VALUE_TYPE_TEXT      = 0x000A,
  CASS_VALUE_TYPE_TIMESTAMP = 0x000B,
  CASS_VALUE_TYPE_UUID      = 0x000C,
  CASS_VALUE_TYPE_VARCHAR   = 0x000D,
  CASS_VALUE_TYPE_VARINT    = 0x000E,
  CASS_VALUE_TYPE_TIMEUUID  = 0x000F,
  CASS_VALUE_TYPE_INET      = 0x0010,
  CASS_VALUE_TYPE_LIST      = 0x0020,
  CASS_VALUE_TYPE_MAP       = 0x0021,
  CASS_VALUE_TYPE_SET       = 0x0022
} CassValueType;

typedef enum CassCompression_ {
  CASS_COMPRESSION_NONE   = 0,
  CASS_COMPRESSION_SNAPPY = 1,
  CASS_COMPRESSION_LZ4    = 2
} CassCompression;

typedef enum CassOption_ {
  CASS_OPTION_THREADS_IO,
  CASS_OPTION_THREADS_CALLBACK,
  CASS_OPTION_CONTACT_POINT_ADD,
  CASS_OPTION_PORT,
  CASS_OPTION_CQL_VERSION,
  CASS_OPTION_SCHEMA_AGREEMENT_WAIT,
  CASS_OPTION_CONTROL_CONNECTION_TIMEOUT,
  CASS_OPTION_COMPRESSION
} CassOption;

#define CASS_LOG_LEVEL_MAP(XX) \
  XX(CASS_LOG_DISABLED, "") \
  XX(CASS_LOG_CRITICAL, "CRITICAL") \
  XX(CASS_LOG_ERROR, "ERROR") \
  XX(CASS_LOG_WARN, "WARN") \
  XX(CASS_LOG_INFO, "INFO") \
  XX(CASS_LOG_DEBUG, "DEBUG")

typedef enum CassLogLevel_ {
#define XX(log_level, _) log_level,
  CASS_LOG_LEVEL_MAP(XX)
#undef XX
  CASS_LOG_LAST_ENTRY
} CassLogLevel;

typedef enum  CassErrorSource_ {
  CASS_ERROR_SOURCE_NONE,
  CASS_ERROR_SOURCE_LIB,
  CASS_ERROR_SOURCE_SERVER,
  CASS_ERROR_SOURCE_SSL,
  CASS_ERROR_SOURCE_COMPRESSION
} CassErrorSource;

#define CASS_ERROR_MAP(XX) \
  XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_BAD_PARAMS, 1, "Bad parameters") \
  XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_INVALID_OPTION, 2, "Invalid option") \
  XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_NO_STREAMS, 3, "No streams available") \
  XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_ALREADY_CONNECTED, 4, "Already connected") \
  XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_NOT_CONNECTED, 5, "Not connected") \
  XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_MESSAGE_PREPARE, 6, "Unable to prepare message") \
  XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_HOST_RESOLUTION, 7, "Unable to reslove host") \
  XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_UNEXPECTED_RESPONSE, 8, "Unexpected reponse from server") \
  XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_REQUEST_QUEUE_FULL, 9, "The request queue is full") \
  XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD, 10, "No available IO threads") \
  XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_WRITE_ERROR, 11, "Write error") \
  XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, 12, "No hosts available") \
  XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS, 13, "Index out of bounds") \
  XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_INVALID_ITEM_COUNT, 14, "Invalid item count") \
  XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_INVALID_VALUE_TYPE, 15, "Invalid value type") \
  XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_LIB_REQUEST_TIMED_OUT, 16, "Request timed out") \
  XX(CASS_ERROR_SOURCE_LIB, CASS_ERROR_UNABLE_TO_SET_KEYSPACE, 17, "Unable to set keyspace") \
  XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_SERVER_ERROR, 0x0000, "Server error") \
  XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_PROTOCOL_ERROR, 0x000A, "Protocol error") \
  XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_BAD_CREDENTIALS, 0x0100, "Bad credentials") \
  XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_UNAVAILABLE, 0x1000, "Unavailable") \
  XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_OVERLOADED, 0x1001, "Overloaded") \
  XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_IS_BOOTSTRAPPING, 0x1002, "Is bootstrapping") \
  XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_TRUNCATE_ERROR, 0x1003, "Truncate error") \
  XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_WRITE_TIMEOUT, 0x1100, "Write timeout") \
  XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_READ_TIMEOUT, 0x1200, "Read timeout") \
  XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_SYNTAX_ERROR, 0x2000, "Sytax error") \
  XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_UNAUTHORIZED, 0x2100, "Unauthorized") \
  XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_INVALID_QUERY, 0x2200, "Invalid query") \
  XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_CONFIG_ERROR, 0x2300, "Configuration error") \
  XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_ALREADY_EXISTS, 0x2400, "Already exists") \
  XX(CASS_ERROR_SOURCE_SERVER, CASS_ERROR_SERVER_UNPREPARED, 0x2500, "Unprepared") \
  XX(CASS_ERROR_SOURCE_SSL, CASS_ERROR_SSL_CERT, 1, "Unable to load certificate") \
  XX(CASS_ERROR_SOURCE_SSL, CASS_ERROR_SSL_CA_CERT, 2, "Unable to load CA certificate") \
  XX(CASS_ERROR_SOURCE_SSL, CASS_ERROR_SSL_PRIVATE_KEY, 3, "Unable to load private key") \
  XX(CASS_ERROR_SOURCE_SSL, CASS_ERROR_SSL_CRL, 4, "Unable to load certificate revocation list")

typedef enum CassError_ {
  CASS_OK = 0,
#define XX(source, name, code, _) name = ((source << 24) | code),
  CASS_ERROR_MAP(XX)
#undef XX
  CASS_ERROR_LAST_ENTRY
} CassError;

typedef void (*CassLogCallback)(void* data,
                                cass_uint64_t time,
                                CassLogLevel severity,
                                CassString message);

/***********************************************************************************
 *
 * Cluster
 *
 ***********************************************************************************/

/**
 * Creates a new cluster.
 *
 * @return Returns a cluster that must be freed.
 *
 * @see cass_cluster_free()
 */
CASS_EXPORT CassCluster*
cass_cluster_new();

/**
 * Set an option on the specified cluster.
 *
 * @param[in] cluster
 * @param[in] option
 * @param[in] data
 * @param[in] data_length
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_cluster_setopt(CassCluster* cluster,
                    CassOption option,
                    const void* data, cass_size_t data_length);

/**
 * Get the option value for the specified cluster
 *
 * @param[in] cluster
 * @param[in] option
 * @param[out] data
 * @param[out] data_length
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_cluster_getopt(const CassCluster* cluster,
                    CassOption option,
                    void** data, cass_size_t* data_length);

/**
 * Connnects a session to the cluster.
 *
 * @param[in] cluster
 * @param[out] future A future that must be freed.
 * @return A session that must be freed.
 */
CASS_EXPORT CassSession*
cass_cluster_connect(CassCluster* cluster, CassFuture** future);

/**
 * Connnects a session to the cluster and sets the keyspace.
 *
 * @param[in] cluster
 * @param[in] keyspace
 * @param[out] future A future that must be freed.
 * @return A session that must be freed.
 */
CASS_EXPORT CassSession*
cass_cluster_connect_keyspace(CassCluster* cluster,
                              const char* keyspace,
                              CassFuture** future);

/**
 * Frees a cluster instance.
 *
 * @param[in] cluster
 */
CASS_EXPORT void
cass_cluster_free(CassCluster* cluster);

/***********************************************************************************
 *
 * Session
 *
 ***********************************************************************************/

/**
 * Frees a session instance. A session must be shutdown before it can be freed.
 *
 * @param[in] session
 */
CASS_EXPORT void
cass_session_free(CassSession* session);

/**
 * Shutdowns the session instance, output a shutdown future which can
 * be used to determine when the session has been terminated. This allows
 * in-flight requests to finish.
 *
 * @param[in] session
 * @return A future that must be freed.
 *
 * @see cass_future_get_session()
 */
CASS_EXPORT CassFuture*
cass_session_shutdown(CassSession* session);


/**
 * Create a prepared statement.
 *
 * @param[in] session
 * @param[in] statement
 * @return A future that must be freed.
 *
 * @see cass_future_get_prepared()
 */
CASS_EXPORT CassFuture*
cass_session_prepare(CassSession* session,
                     CassString statement);

/**
 * Execute a query or bound statement.
 *
 * @param[in] session
 * @param[in] statement
 * @return A future that must be freed.
 *
 * @see cass_future_get_result()
 */
CASS_EXPORT CassFuture*
cass_session_execute(CassSession* session,
                     CassStatement* statement);

/**
 * Execute a batch statement.
 *
 * @param[in] session
 * @param[in] batch
 * @return A future that must be freed.
 *
 * @see cass_future_get_result()
 */
CASS_EXPORT CassFuture*
cass_session_execute_batch(CassSession* session,
                           CassBatch* batch);

/***********************************************************************************
 *
 * Future
 *
 ***********************************************************************************/

/**
 * Frees a future instance. A future can be freed anytime.
 */
CASS_EXPORT void
cass_future_free(CassFuture* future);

/**
 * Gets the set status of the future.
 *
 * @param[in] future
 * @return true if set
 */
CASS_EXPORT cass_bool_t
cass_future_ready(CassFuture* future);

/**
 * Wait for the future to be set with either a result or error.
 *
 * @param[in] future
 */
CASS_EXPORT void
cass_future_wait(CassFuture* future);

/**
 * Wait for the future to be set or timeout.
 *
 * @param[in] future
 * @param[in] timeout wait time in microseconds
 * @return false if returned due to timeout
 */
CASS_EXPORT cass_bool_t
cass_future_wait_timed(CassFuture* future,
                       cass_duration_t timeout);

/**
 * Gets the result of a successful future. If the future is not ready this method will
 * wait for the future to be set. The first successful call consumes the future, all
 * subsequent calls will return NULL.
 *
 * @param[in] future
 * @return CassResult instance if successful, otherwise NULL for error. The return instance
 * must be freed using cass_result_free();
 *
 * @see cass_session_execute() and cass_session_execute_batch()
 */
CASS_EXPORT const CassResult*
cass_future_get_result(CassFuture* future);

/**
 * Gets the result of a successful future. If the future is not ready this method will
 * wait for the future to be set. The first successful call consumes the future, all
 * subsequent calls will return NULL.
 *
 * @param[in] future
 * @return CassPrepared instance if successful, otherwise NULL for error. The return instance
 * must be freed using cass_prepared_free();
 *
 * @see cass_session_prepare()
 */
CASS_EXPORT const CassPrepared*
cass_future_get_prepared(CassFuture* future);

/**
 * Gets the error code from future. If the future is not ready this method will
 * wait for the future to be set.
 *
 * @param[in] future
 * @return CASS_OK if successful, otherwise an error occurred.
 *
 * @see cass_error_desc()
 */
CASS_EXPORT CassError
cass_future_error_code(CassFuture* future);

/**
 * Gets the error messsage from future. If the future is not ready this method will
 * wait for the future to be set.
 *
 * @param[in] future
 * @return Empty string returned if successful, otherwise a message describing the
 * error is returned.
 */
CASS_EXPORT CassString
cass_future_error_message(CassFuture* future);

/***********************************************************************************
 *
 * Statement
 *
 ***********************************************************************************/

/**
 * Creates a new query statement.
 *
 * @param[in] statement The statement string.
 * @param[in] parameter_count The number of bound paramerters.
 * @param[in] consistency The statement's read/write consistency.
 * @return Returns a statement that must be freed.
 *
 * @see cass_statement_free()
 */
CASS_EXPORT CassStatement*
cass_statement_new(CassString statement,
                   cass_size_t parameter_count,
                   CassConsistency consistency);

/**
 * Frees a statement instance.
 *
 * @param[in] statement
 */
CASS_EXPORT void
cass_statement_free(CassStatement* statement);


/**
 * Binds null to a query or bound statement at the specified index.
 *
 * @param[in] statement
 * @param[in] index
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_statement_bind_null(CassStatement* statement,
                         cass_size_t index);

/**
 * Binds an "int" to a query or bound statement at the specified index.
 *
 * @param[in] statement
 * @param[in] index
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_statement_bind_int32(CassStatement* statement,
                          cass_size_t index,
                          cass_int32_t value);

/**
 * Binds a "bigint", "counter" or "timestamp" to a query or bound statement
 * at the specified index.
 *
 * @param[in] statement
 * @param[in] index
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_statement_bind_int64(CassStatement* statement,
                          cass_size_t index,
                          cass_int64_t value);

/**
 * Binds a "float" to a query or bound statement at the specified index.
 *
 * @param[in] statement
 * @param[in] index
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_statement_bind_float(CassStatement* statement,
                          cass_size_t index,
                          cass_float_t value);

/**
 * Binds a "double" to a query or bound statement at the specified index.
 *
 * @param[in] statement
 * @param[in] index
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_statement_bind_double(CassStatement* statement,
                           cass_size_t index,
                           cass_double_t value);

/**
 * Binds a "boolean" to a query or bound statement at the specified index.
 *
 * @param[in] statement
 * @param[in] index
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_statement_bind_bool(CassStatement* statement,
                         cass_size_t index,
                         cass_bool_t value);

/**
 * Binds a "ascii", "text" or "varchar" to a query or bound statement
 * at the specified index.
 *
 * @param[in] statement
 * @param[in] index
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_statement_bind_string(CassStatement* statement,
                           cass_size_t index,
                           CassString value);

/**
 * Binds a "blob" or "varint" to a query or bound statement at the specified index.
 *
 * @param[in] statement
 * @param[in] index
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_statement_bind_bytes(CassStatement* statement,
                           cass_size_t index,
                           CassBytes value);

/**
 * Binds a "uuid" or "timeuuid" to a query or bound statement at the specified index.
 *
 * @param[in] statement
 * @param[in] index
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_statement_bind_uuid(CassStatement* statement,
                         cass_size_t index,
                         CassUuid value);

/**
 * Binds an "inet" to a query or bound statement at the specified index.
 *
 * @param[in] statement
 * @param[in] index
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_statement_bind_inet(CassStatement* statement,
                         cass_size_t index,
                         CassInet value);

/**
 * Bind a "decimal" to a query or bound statement at the specified index.
 *
 * @param[in] statement
 * @param[in] index
 * @param[in] scale
 * @param[in] varint
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_statement_bind_decimal(CassStatement* statement,
                            cass_size_t index,
                            CassDecimal value);

/**
 * Binds any type to a query or bound statement at the specified index. A value
 * can be copied into the resulting output buffer. This is normally reserved for
 * large values to avoid extra memory copies.
 *
 * @param[in] statement
 * @param[in] index
 * @param[in] size
 * @param[out] output
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_statement_bind_custom(CassStatement* statement,
                           cass_size_t index,
                           cass_size_t size,
                           cass_byte_t** output);

/**
 * Bind a "list", "map", or "set" to a query or bound statement at the
 * specified index.
 *
 * @param[in] statement
 * @param[in] index
 * @param[in] collection
 * @param[in] is_map This must be set to true if the collection represents a map.
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_statement_bind_collection(CassStatement* statement,
                               cass_size_t index,
                               const CassCollection* collection,
                               cass_bool_t is_map);


/***********************************************************************************
 *
 * Prepared
 *
 ***********************************************************************************/

/**
 * Frees a prepared instance.
 *
 * @param[in] prepared
 */
CASS_EXPORT void
cass_prepared_free(const CassPrepared* prepared);

/**
 * Creates a bound statement from a pre-prepared statement.
 *
 * @param[in] prepared A previously prepared statement.
 * @param[in] parameter_count The number of bound parameters.
 * @param[in] consistency The statement's read/write consistency.
 * @return Returns a bound statement that must be freed.
 *
 * @see cass_statement_free()
 */
CASS_EXPORT CassStatement*
cass_prepared_bind(const CassPrepared* prepared,
                   cass_size_t parameter_count,
                   CassConsistency consistency);

/***********************************************************************************
 *
 * Batch
 *
 ***********************************************************************************/

/**
 * Creates a new batch statement.
 *
 * @param[in] consistency The statement's read/write consistency.
 * @return Returns a batch statement that must be freed.
 *
 * @see cass_batch_free()
 */
CASS_EXPORT CassBatch*
cass_batch_new(CassConsistency consistency);

/**
 * Frees a batch instance.
 *
 * @param[in] batch
 */
CASS_EXPORT void
cass_batch_free(CassBatch* batch);

/**
 * Adds a statement to a batch.
 *
 * @param[in] batch
 * @param[in] statement
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_batch_add_statement(CassBatch* batch,
                         CassStatement* statement);


/***********************************************************************************
 *
 * Collection
 *
 ***********************************************************************************/

/**
 * Creates a new collection.
 *
 * @param[in] item_count The approximate number of items in the collection.
 * @return Returns a collection that must be freed.
 *
 * @see cass_collection_free()
 */
CASS_EXPORT CassCollection*
cass_collection_new(cass_size_t item_count);

/**
 * Frees a collection instance.
 *
 * @param[in] collection
 */
CASS_EXPORT void
cass_collection_free(CassCollection* collection);

/**
 * Appends an "int" to the collection.
 *
 * @param[in] collection
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_collection_append_int32(CassCollection* collection,
                             cass_int32_t value);

/**
 * Appends a "bigint", "counter" or "timestamp" to the collection.
 *
 * @param[in] collection
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_collection_append_int64(CassCollection* collection,
                              cass_int64_t value);

/**
 * Appends a "float" to the collection.
 *
 * @param[in] collection
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_collection_append_float(CassCollection* collection,
                             cass_float_t value);

/**
 * Appends a "double" to the collection.
 *
 * @param[in] collection
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_collection_append_double(CassCollection* collection,
                              cass_double_t value);

/**
 * Appends a "boolean" to the collection.
 *
 * @param[in] collection
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_collection_append_bool(CassCollection* collection,
                            cass_bool_t value);

/**
 * Appends a "ascii", "text" or "varchar" to the collection.
 *
 * @param[in] collection
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_collection_append_string(CassCollection* collection,
                              CassString value);

/**
 * Appends a "blob" or "varint" to the collection.
 *
 * @param[in] collection
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_collection_append_bytes(CassCollection* collection,
                             CassBytes value);

/**
 * Appends a "uuid" or "timeuuid"  to the collection.
 *
 * @param[in] collection
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_collection_append_uuid(CassCollection* collection,
                            CassUuid value);

/**
 * Appends an "inet" to the collection.
 *
 * @param[in] collection
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_collection_append_inet(CassCollection* collection,
                            CassInet value);

/**
 * Appends a "decimal" to the collection.
 *
 * @param[in] collection
 * @param[in] scale
 * @param[in] varint
 * @return CASS_OK if successful, otherwise an error occurred.
 */
CASS_EXPORT CassError
cass_collection_append_decimal(CassCollection* collection,
                               CassDecimal value);

/***********************************************************************************
 *
 * Result
 *
 ***********************************************************************************/

/**
 * Frees a result instance.
 *
 * @param[in] result
 */
CASS_EXPORT void
cass_result_free(const CassResult* result);

/**
 * Gets the number of rows for the specified result.
 *
 * @param[in] result
 * @return The number of rows in the result.
 */
CASS_EXPORT cass_size_t
cass_result_row_count(const CassResult* result);

/**
 * Gets the number of columns per row for the specified result.
 *
 * @param[in] result
 * @return The number of columns per row in the result.
 */
CASS_EXPORT cass_size_t
cass_result_column_count(const CassResult* result);

/**
 * Gets the column type at index for the specified result.
 *
 * @param[in] result
 * @param[in] index
 * @return The column type at the specified index. CASS_VALUE_TYPE_UNKNOWN
 * is returned if the index is out of bounds.
 */
CASS_EXPORT CassValueType
cass_result_column_type(const CassResult* result,
                        cass_size_t index);

/**
 * Gets the first row of the result.
 *
 * @param[in] result
 * @return The first row of the result. NULL if there are no rows.
 */
CASS_EXPORT const CassRow*
cass_result_first_row(const CassResult* result);

/***********************************************************************************
 *
 * Iterator
 *
 ***********************************************************************************/

/**
 * Creates a new iterator for the specified result. This can be
 * used to iterate over rows in the result.
 *
 * @param[in] result
 * @return A new iterator that must be freed.
 *
 * @see cass_iterator_free()
 */
CASS_EXPORT CassIterator*
cass_iterator_from_result(const CassResult* result);

/**
 * Creates a new iterator for the specified row. This can be
 * used to iterate over columns in a row.
 *
 * @param[in] row
 * @return A new iterator that must be freed.
 *
 * @see cass_iterator_free()
 */
CASS_EXPORT CassIterator*
cass_iterator_from_row(const CassRow* row);

/**
 * Creates a new iterator for the specified collection. This can be
 * used to iterate over values in a collection.
 *
 * @param[in] value
 * @return A new iterator that must be freed.
 *
 * @see cass_iterator_free()
 */
CASS_EXPORT CassIterator*
cass_iterator_from_collection(const CassValue* value);

/**
 * Frees an iterator instance.
 *
 * @param[in] iterator
 */
CASS_EXPORT void
cass_iterator_free(CassIterator* iterator);

/**
 * Advance the iterator to the next row, column, or collection item.
 *
 * @param[in] iterator
 * @return false if no more rows, columns, or items, otherwise true
 */
CASS_EXPORT cass_bool_t
cass_iterator_next(CassIterator* iterator);

/**
 * Gets the row at the result iterator's current position.
 *
 * @param[in] iterator
 * @return A row
 */
CASS_EXPORT const CassRow*
cass_iterator_get_row(CassIterator* iterator);

/**
 * Gets the column value at the row iterator's current position.
 *
 * @param[in] iterator
 * @return A value
 */
CASS_EXPORT const CassValue*
cass_iterator_get_column(CassIterator* iterator);

/**
 * Gets the value at the collection iterator's current position.
 *
 * @param[in] iterator
 * @return A value
 */
CASS_EXPORT const CassValue*
cass_iterator_get_value(CassIterator* iterator);


/***********************************************************************************
 *
 * Row
 *
 ***********************************************************************************/

/**
 * Get the column value at index for the specified row.
 *
 * @param[in] row
 * @param[in] index
 * @return The column value at the specified index. NULL is
 * returned if the index is out of bounds.
 */
CASS_EXPORT const CassValue*
cass_row_get_column(const CassRow* row,
                    cass_size_t index);

/***********************************************************************************
 *
 * Value
 *
 ***********************************************************************************/

/**
 * Gets an int32 for the specified value
 *
 * @param[in] value
 * @param[out] output
 * @return CASS_OK if successful, otherwise error occured
 */
CASS_EXPORT CassError
cass_value_get_int32(const CassValue* value,
                     cass_int32_t* output);

/**
 * Gets an int64 for the specified value
 *
 * @param[in] value
 * @param[out] output
 * @return CASS_OK if successful, otherwise error occured
 */
CASS_EXPORT CassError
cass_value_get_int64(const CassValue* value,
                     cass_int64_t* output);

/**
 * Gets a float for the specified value
 *
 * @param[in] value
 * @param[out] output
 * @return CASS_OK if successful, otherwise error occured
 */
CASS_EXPORT CassError
cass_value_get_float(const CassValue* value,
                     cass_float_t* output);

/**
 * Gets a double for the specified value
 *
 * @param[in] value
 * @param[out] output
 * @return CASS_OK if successful, otherwise error occured
 */
CASS_EXPORT CassError
cass_value_get_double(const CassValue* value,
                      cass_double_t* output);

/**
 * Gets a bool for the specified value
 *
 * @param[in] value
 * @param[out] output
 * @return CASS_OK if successful, otherwise error occured
 */
CASS_EXPORT CassError
cass_value_get_bool(const CassValue* value,
                    cass_bool_t* output);

/**
 * Gets a UUID for the specified value
 *
 * @param[in] value
 * @param[out] output
 * @return CASS_OK if successful, otherwise error occured
 */
CASS_EXPORT CassError
cass_value_get_uuid(const CassValue* value,
                    CassUuid output);

/**
 * Gets an INET for the specified value
 *
 * @param[in] value
 * @param[out] output
 * @return CASS_OK if successful, otherwise error occured
 */
CASS_EXPORT CassError
cass_value_get_inet(const CassValue* value,
                    CassInet* output);

/**
 * Gets a string for the specified value
 *
 * @param[in] value
 * @param[out] output
 * @return CASS_OK if successful, otherwise error occured
 */
CASS_EXPORT CassError
cass_value_get_string(const CassValue* value,
                      CassString* output);

/**
 * Gets the bytes of the specified value
 *
 * @param[in] value
 * @param[out] output
 * @return CASS_OK if successful, otherwise error occured
 */
CASS_EXPORT CassError
cass_value_get_bytes(const CassValue* value,
                     CassBytes* output);

/**
 * Gets a decimal for the specified value
 *
 * @param[in] value
 * @param[out] output_scale
 * @param[out] output_varint
 * @return CASS_OK if successful, otherwise error occured
 */
CASS_EXPORT CassError
cass_value_get_decimal(const CassValue* value,
                       CassDecimal* output);

/**
 * Gets the type of the specified value.
 *
 * @param[in] value
 * @return The type of the specified value.
 */
CASS_EXPORT CassValueType
cass_value_type(const CassValue* value);

/**
 * Returns true if a specified value is null.
 *
 * @param[in] value
 * @return true if the value is null, otherwise false.
 */
CASS_EXPORT cass_bool_t
cass_value_is_null(const CassValue* value);

/**
 * Returns true if a specified value is a collection.
 *
 * @param[in] value
 * @return true if the value is a collection, otherwise false.
 */
CASS_EXPORT cass_bool_t
cass_value_is_collection(const CassValue* value);

/**
 * Get the number of items in a collection. Works for all collection types.
 *
 * @param[in] collection
 * @return Count of items in a collection. 0 if not a collection.
 */
CASS_EXPORT cass_size_t
cass_value_item_count(const CassValue* collection);

/**
 * Get the primary sub-type for a collection. This returns the sub-type for a
 * list or set and the key type for a map.
 *
 * @param[in] collection
 * @return The type of the primary sub-type. CASS_VALUE_TYPE_UNKNOWN
 * returned if not a collection.
 */
CASS_EXPORT CassValueType
cass_value_primary_sub_type(const CassValue* collection);

/**
 * Get the secondary sub-type for a collection. This returns the value type for a
 * map.
 *
 * @param[in] collection
 * @return The type of the primary sub-type. CASS_VALUE_TYPE_UNKNOWN
 * returned if not a collection or not a map.
 */
CASS_EXPORT CassValueType
cass_value_secondary_sub_type(const CassValue* collection);


/***********************************************************************************
 *
 * UUID
 *
 ************************************************************************************/

/**
 * Generates a V1 (time) UUID.
 *
 * @param[out] output A V1 UUID for the current time.
 */
CASS_EXPORT void
cass_uuid_generate_time(CassUuid output);

/**
 * Generates a V1 (time) UUID for the specified time.
 * 
 * @param[in] time
 * @param[out] output A V1 UUID for the specified time.
 */
CASS_EXPORT void
cass_uuid_from_time(cass_uint64_t time,
                    CassUuid output);

/**
 * Generates a minimum V1 (time) UUID for the specified time.
 *
 * @param[in] time
 * @param[out] output A minimum V1 UUID for the specified time.
 */
CASS_EXPORT void
cass_uuid_min_from_time(cass_uint64_t time,
                        CassUuid output);

/**
 * Generates a maximum V1 (time) UUID for the specified time.
 *
 * @param[in] time
 * @param[out] output A maximum V1 UUID for the specified time.
 */
CASS_EXPORT void
cass_uuid_max_from_time(cass_uint64_t time,
                        CassUuid output);

/**
 * Generates a new V4 (random) UUID
 *
 * @param output A randomly generated V4 UUID.
 */
CASS_EXPORT void
cass_uuid_generate_random(CassUuid output);

/**
 * Gets the timestamp for a V1 UUID
 *
 * @param[in] uuid
 * @return The timestamp in milliseconds since the Epoch
 * (00:00:00 UTC on 1 January 1970). 0 returned if the UUID
 * is not V1.
 */
CASS_EXPORT cass_uint64_t
cass_uuid_timestamp(CassUuid uuid);

/**
 * Gets the version for a UUID
 *
 * @param[in] uuid
 * @return The version of the UUID (1 or 4)
 */
CASS_EXPORT cass_uint8_t
cass_uuid_version(CassUuid uuid);

/**
 * Returns a null-terminated string for the specified UUID.
 *
 * @param[in] uuid
 * @param[out] output A null-terminated string of length CASS_UUID_STRING_LENGTH.
 * Example: "550e8400-e29b-41d4-a716-446655440000"
 */
CASS_EXPORT void
cass_uuid_string(CassUuid uuid,
                 char* output);

/***********************************************************************************
 *
 * Error
 *
 ***********************************************************************************/

/**
 * Gets a description for an error code.
 *
 * @param[in] error
 * @return A null-terminated string describing the error.
 */
CASS_EXPORT const char*
cass_error_desc(CassError error);

/***********************************************************************************
 *
 * Log level
 *
 ***********************************************************************************/

/**
 * Gets the string for a log level.
 *
 * @param[in] log_level
 * @return A null-terminated string for the log level.
 * Example: "ERROR", "WARN", "INFO", etc.
 */
CASS_EXPORT const char*
cass_log_level_string(CassLogLevel log_level);

/***********************************************************************************
 *
 * Bytes/String
 *
 ************************************************************************************/

/**
 * Constructs a bytes object.
 *
 * @param[in] data
 * @param[in] size
 * @return A bytes object.
 */
CASS_EXPORT CassBytes
cass_bytes_init(const cass_byte_t* data, cass_size_t size);

/**
 * Constructs a string object from a null-terminated string.
 *
 * @param[in] null_terminated
 * @return A string object.
 */
CASS_EXPORT CassString
cass_string_init(const char* null_terminated);

/**
 * Constructs a string object.
 *
 * @param[in] data
 * @param[in] length
 * @return A string object.
 */
CASS_EXPORT CassString
cass_string_init2(const char* data, cass_size_t length);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* __CASS_H_INCLUDED__ */
