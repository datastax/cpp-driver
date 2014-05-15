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

#ifndef __CASS_H_INCLUDED__
#define __CASS_H_INCLUDED__

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

/* TODO(mpenick) handle primitive types for other compilers and platforms */

typedef int cass_bool_t;
#define cass_false 0
#define cass_true  1

typedef float cass_float_t;
typedef double cass_double_t;

typedef char cass_int8_t;
typedef short cass_int16_t;
typedef int cass_int32_t;
typedef long long cass_int64_t;

typedef unsigned char cass_uint8_t;
typedef unsigned short cass_uint16_t;
typedef unsigned int cass_uint32_t;
typedef unsigned long long cass_uint64_t;

typedef size_t cass_size_t;

typedef cass_uint8_t CassUuid[16];

typedef struct {
  cass_uint8_t  address[16];
  cass_uint8_t  address_len;
} CassInet;

struct CassSession;
typedef struct CassSession CassSession;

struct CassStatement;
typedef struct CassStatement CassStatement;

struct CassBatchStatement;
typedef struct CassBatchStatement CassBatchStatement;

struct CassFuture;
typedef struct CassFuture CassFuture;

struct CassPrepared;
typedef struct CassPrepared CassPrepared;

struct CassResult;
typedef struct CassResult CassResult;

struct CassIterator;
typedef struct CassIterator CassIterator;

struct CassRow;
typedef struct CassRow CassRow;

struct CassValue;
typedef struct CassValue CassValue;

typedef enum {
  CASS_LOG_CRITICAL = 0x00,
  CASS_LOG_ERROR    = 0x01,
  CASS_LOG_INFO     = 0x02,
  CASS_LOG_DEBUG    = 0x03
} CassLogLevel;

typedef enum {
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

typedef enum {
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

typedef enum {
  CASS_OPTION_THREADS_IO                 = 1,
  CASS_OPTION_THREADS_CALLBACK           = 2,
  CASS_OPTION_CONTACT_POINT_ADD          = 3,
  CASS_OPTION_PORT                       = 4,
  CASS_OPTION_CQL_VERSION                = 5,
  CASS_OPTION_SCHEMA_AGREEMENT_WAIT      = 6,
  CASS_OPTION_CONTROL_CONNECTION_TIMEOUT = 7,
  CASS_OPTION_COMPRESSION                = 9
} CassOption;

typedef enum {
  CASS_COMPRESSION_NONE   = 0,
  CASS_COMPRESSION_SNAPPY = 1,
  CASS_COMPRESSION_LZ4    = 2
} CassCompression;

typedef enum {
  CASS_HOST_DISTANCE_LOCAL,
  CASS_HOST_DISTANCE_REMOTE,
  CASS_HOST_DISTANCE_IGNORE
} CassHostDistance;


typedef enum {
  CASS_OK                        = 0,

  CASS_ERROR_SSL_CERT            = 1000000,
  CASS_ERROR_SSL_PRIVATE_KEY     = 1000001,
  CASS_ERROR_SSL_CA_CERT         = 1000002,
  CASS_ERROR_SSL_CRL             = 1000003,
  CASS_ERROR_SSL_READ            = 1000004,
  CASS_ERROR_SSL_WRITE           = 1000005,
  CASS_ERROR_SSL_READ_WAITING    = 1000006,
  CASS_ERROR_SSL_WRITE_WAITING   = 1000007,

  CASS_ERROR_LIB_BAD_PARAMS      = 2000001,
  CASS_ERROR_LIB_INVALID_OPTION  = 2000002,
  CASS_ERROR_LIB_NO_STREAMS      = 2000008,
  CASS_ERROR_LIB_MAX_CONNECTIONS = 2000009,
  CASS_ERROR_LIB_SESSION_STATE   = 2000010,
  CASS_ERROR_LIB_MESSAGE_PREPARE = 2000011,
  CASS_ERROR_LIB_HOST_RESOLUTION = 2000012
} CassCode;

typedef enum  {
  CASS_ERROR_SOURCE_OS          = 1,
  CASS_ERROR_SOURCE_NETWORK     = 2,
  CASS_ERROR_SOURCE_SSL         = 3,
  CASS_ERROR_SOURCE_COMPRESSION = 4,
  CASS_ERROR_SOURCE_SERVER      = 5,
  CASS_ERROR_SOURCE_LIBRARY     = 6
} CassSource;

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Initialize a new session. Instance must be freed by caller.
 *
 * @return
 */
CASS_EXPORT CassSession*
cass_session_new();

/**
 * Initialize a new session using previous session's configuration.
 * Instance must be freed by caller.
 *
 * @return
 */
CASS_EXPORT CassSession*
cass_session_clone(CassSession* session);

/**
 * Free a session instance.
 *
 * @return
 */
CASS_EXPORT void
cass_session_free(
    CassSession* session);


/**
 * Set an option on the specified session
 *
 * @param cluster
 * @param option
 * @param data
 * @param data_len
 *
 * @return 0 if successful, otherwise an error occurred
 */
CASS_EXPORT CassCode
cass_session_setopt(
    CassSession* session,
    CassOption   option,
    const void* data,
    size_t      data_len);

/**
 * Get the option value for the specified session
 *
 * @param option
 * @param data
 * @param data_len
 *
 * @return 0 if successful, otherwise an error occurred
 */
CASS_EXPORT CassCode
cass_cluster_getopt(
    CassSession* session,
    CassOption   option,
    void**      data,
    size_t*     data_len);


/**
 * Initiate a session using the specified session. Resulting
 * future must be freed by caller.
 *
 * @param cluster
 * @param future output future, must be freed by caller, pass NULL to avoid allocation
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_session_connect(
    CassSession* session,
    CassFuture** future);

/**
 * Initiate a session using the specified session, and set the keyspace. Resulting
 * future must be freed by caller.
 *
 * @param session
 * @param keyspace
 * @param future output future, must be freed by caller, pass NULL to avoid allocation
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_session_connect_keyspace(
    CassSession* session,
    const char* keyspace,
    CassFuture** future);

/**
 * Shutdown the session instance, output a shutdown future which can
 * be used to determine when the session has been terminated
 *
 * @param session
 */
CASS_EXPORT CassCode
cass_session_shutdown(
    CassSession* session,
    CassFuture** future);

/***********************************************************************************
 *
 * Functions which deal with futures
 *
 ***********************************************************************************/

/**
 * Free a session instance.
 *
 * @return
 */
CASS_EXPORT void
cass_future_free(
    CassFuture* future);

/**
 * Is the specified future ready
 *
 * @param future
 *
 * @return true if ready
 */
CASS_EXPORT int
cass_future_ready(
    CassFuture* future);

/**
 * Wait the linked event occurs or error condition is reached
 *
 * @param future
 */
CASS_EXPORT void
cass_future_wait(
    CassFuture* future);

/**
 * Wait the linked event occurs, error condition is reached, or time has elapsed.
 *
 * @param future
 * @param wait time in microseconds
 *
 * @return false if returned due to timeout
 */
CASS_EXPORT int
cass_future_wait_timed(
    CassFuture* future,
    size_t     wait);

/**
 * If the linked event was successful get the session instance
 *
 * @param future
 *
 * @return NULL if unsuccessful, otherwise pointer to CassSession instance
 */
CASS_EXPORT CassSession*
cass_future_get_session(
    CassFuture* future);

/**
 * If the linked event was successful get the result instance. Result
 * instance must be freed by caller. May only be called once.
 *
 * @param future
 *
 * @return NULL if unsuccessful, otherwise pointer to CassResult instance
 */
CASS_EXPORT CassResult*
cass_future_get_result(
    CassFuture* future);

/**
 * If the linked event was successful get the prepared
 * instance. Prepared instance must be freed by caller. May only be
 * called once
 *
 * @param future
 *
 * @return NULL if unsuccessful, otherwise pointer to CassPrepare instance
 */
CASS_EXPORT CassPrepared*
cass_future_get_prepared(
    CassFuture* future);

/***********************************************************************************
 *
 * Functions which deal with errors
 *
 ***********************************************************************************/

/**
 * Obtain the message from an error structure. This function follows
 * the pattern similar to that of snprintf. The user passes in a
 * pre-allocated builder of size n, to which the decoded value will be
 * copied. The number of bytes written had the builder been
 * sufficiently large will be returned via the output parameter
 * 'total'. Only when total is less than n has the builder been fully
 * consumed.
 *
 * @param source
 * @param output
 * @param output_len
 * @param total
 *
 */
CASS_EXPORT void
cass_future_error_string(
    CassFuture* future,
    char*      output,
    size_t     output_len,
    size_t*    total);

/**
 * Obtain the code from an error structure.
 *
 * @param error
 *
 * @return error source
 *
 */
CASS_EXPORT CassSource
cass_future_error_source(
    CassFuture* future);

/**
 * Obtain the source from an error structure.
 *
 * @param error
 *
 * @return source code
 *
 */
CASS_EXPORT CassCode
cass_future_error_code(
    CassFuture* future);

/**
 * Get description for error code
 *
 * @param code
 *
 * @return error description
 */
CASS_EXPORT const char*
cass_error_desc(
    int code);

/***********************************************************************************
 *
 * Functions which create ad-hoc queries, prepared statements, bound statements, and
 * allow for the composition of batch statements from queries and bound statements.
 *
 ***********************************************************************************/

/**
 * Initialize an ad-hoc query statement
 *
 * @param session
 * @param statement string
 * @param statement_length statement string length
 * @param parameter_count number of bound paramerters
 * @param consistency statement read/write consistency
 * @param output
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_session_query(
    CassSession*    session,
    const char*    statement,
    size_t         statement_len,
    size_t         parameter_count,
    CassConsistency consistency,
    CassStatement** output);

/**
 * Create a prepared statement. Future must be freed by caller.
 *
 * @param session
 * @param statement string
 * @param statement_len statement string length
 * @param future output future, must be freed by caller, pass NULL to avoid allocation
 *
 * @return
 */
CASS_EXPORT CassCode
cass_session_prepare(
    CassSession* session,
    const char* statement,
    size_t      statement_len,
    CassFuture** output);

/**
 * Initialize a bound statement from a pre-prepared statement
 * parameters
 *
 * @param session
 * @param prepared the previously prepared statement
 * @param parameter_count number of bound paramerters
 * @param consistency statement read/write consistency
 * @param output
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_prepared_bind(
    CassPrepared*   prepared,
    size_t         paramater_count,
    CassConsistency consistency,
    CassStatement** output);

/**
 * Bind a short to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
CASS_EXPORT CassCode
cass_statement_bind_short(
    CassStatement* statement,
    size_t        index,
    cass_int16_t   value);

/**
 * Bind an int to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
CASS_EXPORT CassCode
cass_statement_bind_int(
    CassStatement* statement,
    size_t        index,
    cass_int32_t   value);

/**
 * Bind a bigint to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
CASS_EXPORT CassCode
cass_statement_bind_bigint(
    CassStatement* statement,
    size_t        index,
    cass_int64_t   value);

/**
 * Bind a float to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
CASS_EXPORT CassCode
cass_statement_bind_float(
    CassStatement* statement,
    size_t        index,
    cass_float_t     value);

/**
 * Bind a double to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_statement_bind_double(
    CassStatement*  statement,
    size_t         index,
    cass_double_t     value);

/**
 * Bind a bool to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_statement_bind_bool(
    CassStatement*  statement,
    size_t         index,
    cass_bool_t    value);

/**
 * Bind a time stamp to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_statement_bind_timestamp(
    CassStatement*  statement,
    size_t         index,
    cass_int64_t    value);

/**
 * Bind a UUID to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_statement_bind_uuid(
    CassStatement*  statement,
    size_t         index,
    CassUuid       value);

/**
 * Bind a counter to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_statement_bind_counter(
    CassStatement*  statement,
    size_t         index,
    cass_int64_t    value);

/**
 * Bind a string to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 * @param length
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_statement_bind_string(
    CassStatement*  statement,
    size_t         index,
    const char*    value,
    size_t         length);

/**
 * Bind a blob to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param output
 * @param length
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_statement_bind_blob(
    CassStatement*  statement,
    size_t         index,
    cass_uint8_t*   value,
    size_t         length);

CASS_EXPORT CassCode
cass_statement_bind_inet(
    CassStatement* statement,
    size_t         index,
    const cass_uint8_t*  address,
    cass_uint8_t   address_len);

/**
 * Bind a decimal to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param scale
 * @param value
 * @param length
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_statement_bind_decimal(
    CassStatement* statement,
    size_t        index,
    cass_uint32_t  scale,
    cass_uint8_t*  value,
    size_t        length);

/**
 * Bind a varint to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 * @param length
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_statement_bind_varint(
    CassStatement*  statement,
    size_t         index,
    cass_uint8_t*   value,
    size_t         length);

/**
 * Execute a query, bound statement and obtain a future. Future must be freed by
 * caller.
 *
 * @param session
 * @param statement
 * @param future output future, must be freed by caller, pass NULL to avoid allocation
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_session_exec(
    CassSession*   session,
    CassStatement* statement,
    CassFuture**   future);

/***********************************************************************************
 *
 * Functions dealing with batching multiple statements
 *
 ***********************************************************************************/

CASS_EXPORT CassCode
cass_session_batch(
    CassSession*         session,
    CassConsistency      consistency,
    CassBatchStatement** output);

CASS_EXPORT CassCode
cass_batch_add_statement(
    CassBatchStatement* batch,
    CassStatement*      statement);

CASS_EXPORT CassCode
cass_batch_set_timestamp(
    CassBatchStatement* batch,
    cass_int64_t        timestamp);

/**
 * Execute a batch statement and obtain a future. Future must be freed by
 * caller.
 *
 * @param session
 * @param statement
 * @param future output future, must be freed by caller, pass NULL to avoid allocation
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_session_exec_batch(
    CassSession*        session,
    CassBatchStatement* statement,
    CassFuture**        future);


/***********************************************************************************
 *
 * Collection
 *
 ***********************************************************************************/

struct CassCollection;
typedef struct CassCollection CassCollection;

CASS_EXPORT CassCollection*
cass_collection_new(size_t element_count);

CASS_EXPORT void
cass_collection_free(CassCollection* collection);

CASS_EXPORT CassCode
cass_collection_append_int(CassCollection* collection,
                         cass_int32_t i);

CASS_EXPORT CassCode
cass_collection_append_bigint(CassCollection* collection,
                         cass_int32_t bi);

CASS_EXPORT CassCode
cass_collection_append_float(CassCollection* collection,
                         cass_float_t f);

CASS_EXPORT CassCode
cass_collection_append_double(CassCollection* collection,
                         cass_double_t d);

CASS_EXPORT CassCode
cass_collection_append_bool(CassCollection* collection,
                        cass_bool_t b);

CASS_EXPORT CassCode
cass_collection_append_inet(CassCollection* collection,
                        CassInet inet);

CASS_EXPORT CassCode
cass_collection_append_decimal(CassCollection* collection,
                          cass_int32_t scale,
                          cass_uint8_t* varint, cass_size_t varint_length);

CASS_EXPORT CassCode
cass_collection_append_uuid(CassCollection* collection,
                        CassUuid uuid);

CASS_EXPORT CassCode
cass_collection_append_blob(CassCollection* builder,
                        cass_uint8_t* blob, cass_size_t blob_length);

CASS_EXPORT CassCode
cass_statement_bind_collection(
    CassStatement*  statement,
    size_t          index,
    CassCollection*    collection,
    cass_bool_t     is_map);

/***********************************************************************************
 *
 * Functions dealing with fetching data and meta information from statement results
 *
 ***********************************************************************************/


/**
 * Get number of rows for the specified result
 *
 * @param result
 *
 * @return
 */
CASS_EXPORT size_t
cass_result_rowcount(
    CassResult* result);

/**
 * Get number of columns per row for the specified result
 *
 * @param result
 *
 * @return
 */
CASS_EXPORT size_t
cass_result_colcount(
    CassResult* result);

/**
 * Get the type for the column at index for the specified result
 *
 * @param result
 * @param index
 * @param coltype output
 *
 * @return
 */
CASS_EXPORT CassCode
cass_result_coltype(
    CassResult*     result,
    size_t          index,
    CassValueType* coltype);

/**
 * Get an iterator for the specified result or collection. Iterator must be freed by caller.
 *
 * @param result
 * @param iterator
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_result_iterator_new(
    CassResult* result,
    CassIterator** iterator);

CASS_EXPORT CassCode
cass_result_iterator_get_row(
    CassIterator* iterator,
    CassRow** row);

CASS_EXPORT CassCode
cass_collection_iterator_new(
    CassValue* collection,
    CassIterator** iterator);

CASS_EXPORT CassCode
cass_collection_iterator_get_pair(
    CassIterator* iterator,
    CassValue** key,
    CassValue** value);

CASS_EXPORT CassCode
cass_collection_iterator_get_entry(
    CassIterator* iterator,
    CassValue* entry);

/**
 * Advance the iterator to the next row or collection item.
 *
 * @param iterator
 *
 * @return next row, NULL if no rows remain or error
 */
CASS_EXPORT cass_bool_t
cass_iterator_next(
    CassIterator* iterator);

CASS_EXPORT void
cass_iterator_free(
    CassIterator* iterator);

/**
 * Get the column value at index for the specified row.
 *
 * @param row
 * @param index
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_row_getcol(
    CassRow*  row,
    size_t index,
    CassValue** value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_decode_short(
    CassValue*   source,
    cass_int16_t* value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_decode_int(
    CassValue*    source,
    cass_int32_t* value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_decode_bigint(
    CassValue*     source,
    cass_int64_t* value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_decode_float(
    CassValue*  source,
    cass_float_t*  value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_decode_double(
    CassValue*       source,
    cass_double_t  value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_decode_bool(
    CassValue*    source,
    cass_bool_t value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_decode_time(
    CassValue*       source,
    cass_int64_t value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_decode_uuid(
    CassValue*   source,
    CassUuid value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_decode_counter(
    CassValue*       source,
    cass_int64_t value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value. This function follows the pattern similar to that of snprintf. The user
 * passes in a pre-allocated builder of size n, to which the decoded value will be
 * copied. The number of bytes written had the builder been sufficiently large will be
 * returned via the output parameter 'total'. Only when total is less than n has
 * the builder been fully consumed.
 *
 * @param source
 * @param output
 * @param output_len
 * @param total
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_decode_string(
    CassValue*   source,
    char*   output,
    size_t  output_len,
    size_t* total);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value. This function follows the pattern similar to that of snprintf. The user
 * passes in a pre-allocated builder of size n, to which the decoded value will be
 * copied. The number of bytes written had the builder been sufficiently large will be
 * returned via the output parameter 'total'. Only when total is less than n has
 * the builder been fully consumed.
 *
 * @param source
 * @param output
 * @param output_len
 * @param total
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_decode_blob(
    CassValue*        source,
    cass_uint8_t* output,
    size_t       output_len,
    size_t*      total);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_decode_decimal(
    CassValue*         source,
    cass_uint32_t* scale,
    cass_uint8_t** value,
    size_t*       len);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value. This function follows the pattern similar to that of snprintf. The user
 * passes in a pre-allocated builder of size n, to which the decoded value will be
 * copied. The number of bytes written had the builder been sufficiently large will be
 * returned via the output parameter 'total'. Only when total is less than n has
 * the builder been fully consumed.
 *
 * @param source
 * @param output
 * @param output_len
 * @param total
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_decode_varint(
    CassValue*        source,
    cass_uint8_t* output,
    size_t       output_len,
    size_t*      total);

/**
 * Get the number of items in a collection. Works for all collection types.
 *
 * @param source collection column
 *
 * @return size, 0 if error
 */
CASS_EXPORT size_t
cass_collection_count(
    CassValue*  source);

/**
 * Get the collection sub-type. Works for collections that have a single sub-type
 * (lists and sets).
 *
 * @param collection
 * @param output
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_collection_subtype(
    CassValue*   collection,
    CassValueType* output);

/**
 * Get the sub-type of the key for a map collection. Works only for maps.
 *
 * @param collection
 * @param output
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_collection_map_key_type(
    CassValue*   collection,
    CassValueType* output);

/**
 * Get the sub-type of the value for a map collection. Works only for maps.
 *
 * @param collection
 * @param output
 *
 * @return
 */
CASS_EXPORT CassCode
cass_collection_map_value_type(
    CassValue*   collection,
    CassValueType* output);

/**
 * Use an iterator to obtain each pair from a map. Once a pair has been obtained from
 * the iterator use this function to fetch the key portion of the pair
 *
 * @param item
 * @param output
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_map_get_key(
    CassValue*  item,
    void** output);

/**
 * Use an iterator to obtain each pair from a map. Once a pair has been obtained from
 * the iterator use this function to fetch the value portion of the pair
 *
 * @param item
 * @param output
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassCode
cass_map_get_value(
    void*  item,
    void** output);

/***********************************************************************************
 *
 * Misc
 *
 ************************************************************************************/


/**
 * Generate a new V1 (time) UUID
 *
 * @param output
 */
CASS_EXPORT void
cass_uuid_v1(
    CassUuid* output);

/**
 * Generate a new V1 (time) UUID for the specified time
 *
 * @param output
 */
CASS_EXPORT void
cass_uuid_v1_for_time(
    cass_uint64_t  time,
    CassUuid*      output);

/**
 * Generate a new V4 (random) UUID
 *
 * @param output
 *
 * @return
 */
CASS_EXPORT void
cass_uuid_v4(
    CassUuid* output);

/**
 * Return the corresponding null terminated string for the specified UUID.
 *
 * @param uuid
 * @param output char[37]
 *
 * @return
 */
CASS_EXPORT CassCode
cass_uuid_string(
    CassUuid uuid,
    char*   output);

#ifdef __cplusplus
} /* extern "C" */
#endif




#endif /* __CASS_H_INCLUDED__ */
