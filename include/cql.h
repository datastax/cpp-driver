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

#ifndef __CQL_H_INCLUDED__
#define __CQL_H_INCLUDED__

#include <stddef.h>

#if defined _WIN32
#   if defined CQL_STATIC
#       define CQL_EXPORT
#   elif defined DLL_EXPORT
#       define CQL_EXPORT __declspec(dllexport)
#   else
#       define CQL_EXPORT __declspec(dllimport)
#   endif
#else
#   if defined __SUNPRO_C  || defined __SUNPRO_CC
#       define CQL_EXPORT __global
#   elif (defined __GNUC__ && __GNUC__ >= 4) || defined __INTEL_COMPILER
#       define CQL_EXPORT __attribute__ ((visibility("default")))
#   else
#       define CQL_EXPORT
#   endif
#endif

/* TODO(mpenick) handle primitive types for other compilers and platforms */

typedef int cql_bool;
#define cql_false 0
#define cql_true  1

typedef float cql_float;
typedef double cql_double;

typedef char cql_int8_t;
typedef short cql_int16_t;
typedef int cql_int32_t;
typedef long long cql_int64_t;

typedef unsigned char cql_uint8_t;
typedef unsigned short cql_uint16_t;
typedef unsigned int cql_uint32_t;
typedef unsigned long long cql_uint64_t;

typedef cql_uint8_t CqlUuid[16];

typedef struct {
  cql_uint8_t  length;
  cql_uint8_t  address[6];
  cql_uint32_t port;
} CqlInet;

struct CqlSession;
typedef struct CqlSession CqlSession;

struct CqlStatement;
typedef struct CqlStatement CqlStatement;

struct CqlBatchStatement;
typedef struct CqlBatchStatement CqlBatchStatement;

struct CqlFuture;
typedef struct CqlFuture CqlFuture;

struct CqlPrepared;
typedef struct CqlPrepared CqlPrepared;

struct CqlResult;
typedef struct CqlResult CqlResult;

struct CqlHost;
typedef struct CqlHost CqlHost;

struct CqlLoadBalancingPolicy;
typedef struct CqlLoadBalancingPolicy CqlLoadBalancingPolicy;

typedef enum {
  CQL_LOG_CRITICAL = 0x00,
  CQL_LOG_ERROR    = 0x01,
  CQL_LOG_INFO     = 0x02,
  CQL_LOG_DEBUG    = 0x03
} CqlLogLevel;

typedef enum {
  CQL_CONSISTENCY_ANY          = 0x0000,
  CQL_CONSISTENCY_ONE          = 0x0001,
  CQL_CONSISTENCY_TWO          = 0x0002,
  CQL_CONSISTENCY_THREE        = 0x0003,
  CQL_CONSISTENCY_QUORUM       = 0x0004,
  CQL_CONSISTENCY_ALL          = 0x0005,
  CQL_CONSISTENCY_LOCAL_QUORUM = 0x0006,
  CQL_CONSISTENCY_EACH_QUORUM  = 0x0007,
  CQL_CONSISTENCY_SERIAL       = 0x0008,
  CQL_CONSISTENCY_LOCAL_SERIAL = 0x0009,
  CQL_CONSISTENCY_LOCAL_ONE    = 0x000A
} CqlConsistency;

typedef enum {
  CQL_COLUMN_TYPE_UNKNOWN   = 0xFFFF,
  CQL_COLUMN_TYPE_CUSTOM    = 0x0000,
  CQL_COLUMN_TYPE_ASCII     = 0x0001,
  CQL_COLUMN_TYPE_BIGINT    = 0x0002,
  CQL_COLUMN_TYPE_BLOB      = 0x0003,
  CQL_COLUMN_TYPE_BOOLEAN   = 0x0004,
  CQL_COLUMN_TYPE_COUNTER   = 0x0005,
  CQL_COLUMN_TYPE_DECIMAL   = 0x0006,
  CQL_COLUMN_TYPE_DOUBLE    = 0x0007,
  CQL_COLUMN_TYPE_FLOAT     = 0x0008,
  CQL_COLUMN_TYPE_INT       = 0x0009,
  CQL_COLUMN_TYPE_TEXT      = 0x000A,
  CQL_COLUMN_TYPE_TIMESTAMP = 0x000B,
  CQL_COLUMN_TYPE_UUID      = 0x000C,
  CQL_COLUMN_TYPE_VARCHAR   = 0x000D,
  CQL_COLUMN_TYPE_VARINT    = 0x000E,
  CQL_COLUMN_TYPE_TIMEUUID  = 0x000F,
  CQL_COLUMN_TYPE_INET      = 0x0010,
  CQL_COLUMN_TYPE_LIST      = 0x0020,
  CQL_COLUMN_TYPE_MAP       = 0x0021,
  CQL_COLUMN_TYPE_SET       = 0x0022
} CqlColumnType;

typedef enum {
  CQL_OPTION_THREADS_IO                 = 1,
  CQL_OPTION_THREADS_CALLBACK           = 2,
  CQL_OPTION_CONTACT_POINT_ADD          = 3,
  CQL_OPTION_PORT                       = 4,
  CQL_OPTION_CQL_VERSION                = 5,
  CQL_OPTION_SCHEMA_AGREEMENT_WAIT      = 6,
  CQL_OPTION_CONTROL_CONNECTION_TIMEOUT = 7,
  CQL_OPTION_COMPRESSION                = 9
} CqlOption;

typedef enum {
  CQL_COMPRESSION_NONE   = 0,
  CQL_COMPRESSION_SNAPPY = 1,
  CQL_COMPRESSION_LZ4    = 2
} CqlCompression;

typedef enum {
  CQL_HOST_DISTANCE_LOCAL,
  CQL_HOST_DISTANCE_REMOTE,
  CQL_HOST_DISTANCE_IGNORE
} CqlHostDistance;

typedef void (*CqlLoadBalancingInitFunction)(CqlLoadBalancingPolicy* policy);
typedef CqlHostDistance (*CqlLoadBalancingHostDistanceFunction)(CqlLoadBalancingPolicy* policy,
                                                                const CqlHost* host);

typedef const char* (*CqlLoadBalancingNextHostFunction)(CqlLoadBalancingPolicy* policy, int is_initial);

typedef struct {
  CqlLoadBalancingInitFunction init_func;
  CqlLoadBalancingHostDistanceFunction host_distance_func;
  CqlLoadBalancingNextHostFunction next_host_func;
} CqlLoadBalancingPolicyImpl;

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Initialize a new session. Instance must be freed by caller.
 *
 * @return
 */
CQL_EXPORT CqlSession*
cql_session_new();

/**
 * Initialize a new session using previous session's configuration.
 * Instance must be freed by caller.
 *
 * @return
 */
CQL_EXPORT CqlSession*
cql_session_clone(CqlSession* session);

/**
 * Free a session instance.
 *
 * @return
 */
CQL_EXPORT void
cql_session_free(
    CqlSession* session);


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
CQL_EXPORT int
cql_session_setopt(
    CqlSession* session,
    CqlOption   option,
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
CQL_EXPORT int
cql_cluster_getopt(
    CqlSession* session,
    CqlOption   option,
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
CQL_EXPORT int
cql_session_connect(
    CqlSession* session,
    CqlFuture** future);

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
CQL_EXPORT int
cql_session_connect_keyspace(
    CqlSession* session,
    const char* keyspace,
    CqlFuture** future);

/**
 * Shutdown the session instance, output a shutdown future which can
 * be used to determine when the session has been terminated
 *
 * @param session
 */
CQL_EXPORT int
cql_session_shutdown(
    CqlSession* session,
    CqlFuture** future);

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
CQL_EXPORT void
cql_future_free(
    CqlFuture* future);

/**
 * Is the specified future ready
 *
 * @param future
 *
 * @return true if ready
 */
CQL_EXPORT int
cql_future_ready(
    CqlFuture* future);

/**
 * Wait the linked event occurs or error condition is reached
 *
 * @param future
 */
CQL_EXPORT void
cql_future_wait(
    CqlFuture* future);

/**
 * Wait the linked event occurs, error condition is reached, or time has elapsed.
 *
 * @param future
 * @param wait time in microseconds
 *
 * @return false if returned due to timeout
 */
CQL_EXPORT int
cql_future_wait_timed(
    CqlFuture* future,
    size_t     wait);

/**
 * If the linked event was successful get the session instance
 *
 * @param future
 *
 * @return NULL if unsuccessful, otherwise pointer to CqlSession instance
 */
CQL_EXPORT CqlSession*
cql_future_get_session(
    CqlFuture* future);

/**
 * If the linked event was successful get the result instance. Result
 * instance must be freed by caller. May only be called once.
 *
 * @param future
 *
 * @return NULL if unsuccessful, otherwise pointer to CqlResult instance
 */
CQL_EXPORT CqlResult*
cql_future_get_result(
    CqlFuture* future);

/**
 * If the linked event was successful get the prepared
 * instance. Prepared instance must be freed by caller. May only be
 * called once
 *
 * @param future
 *
 * @return NULL if unsuccessful, otherwise pointer to CqlPrepare instance
 */
CQL_EXPORT CqlPrepared*
cql_future_get_prepare(
    CqlFuture* future);

/***********************************************************************************
 *
 * Functions which deal with errors
 *
 ***********************************************************************************/

/**
 * Obtain the message from an error structure. This function follows
 * the pattern similar to that of snprintf. The user passes in a
 * pre-allocated buffer of size n, to which the decoded value will be
 * copied. The number of bytes written had the buffer been
 * sufficiently large will be returned via the output parameter
 * 'total'. Only when total is less than n has the buffer been fully
 * consumed.
 *
 * @param source
 * @param output
 * @param output_len
 * @param total
 *
 */
CQL_EXPORT void
cql_future_error_string(
    CqlFuture* future,
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
CQL_EXPORT int
cql_future_error_source(
    CqlFuture* future);

/**
 * Obtain the source from an error structure.
 *
 * @param error
 *
 * @return source code
 *
 */
CQL_EXPORT int
cql_future_error_code(
    CqlFuture* future);

/**
 * Get description for error code
 *
 * @param code
 *
 * @return error description
 */
CQL_EXPORT const char*
cql_error_desc(
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
CQL_EXPORT int
cql_session_query(
    CqlSession*    session,
    const char*    statement,
    size_t         statement_len,
    size_t         paramater_count,
    CqlConsistency consistency,
    CqlStatement** output);

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
CQL_EXPORT int
cql_session_prepare(
    CqlSession* session,
    const char* statement,
    size_t      statement_len,
    CqlFuture** output);

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
CQL_EXPORT int
cql_session_bind(
    CqlSession*    session,
    CqlPrepared*   prepared,
    size_t         paramater_count,
    CqlConsistency consistency,
    CqlStatement** output);

/**
 * Bind a short to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
CQL_EXPORT int
cql_statement_bind_short(
    CqlStatement* statement,
    size_t        index,
    cql_int16_t   value);

/**
 * Bind an int to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
CQL_EXPORT int
cql_statement_bind_int(
    CqlStatement* statement,
    size_t        index,
    cql_int32_t   value);

/**
 * Bind a bigint to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
CQL_EXPORT int
cql_statement_bind_bigint(
    CqlStatement* statement,
    size_t        index,
    cql_int64_t   value);

/**
 * Bind a float to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
CQL_EXPORT int
cql_statement_bind_float(
    CqlStatement* statement,
    size_t        index,
    cql_float     value);

/**
 * Bind a double to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_statement_bind_double(
    CqlStatement*  statement,
    size_t         index,
    cql_double     value);

/**
 * Bind a bool to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_statement_bind_bool(
    CqlStatement*  statement,
    size_t         index,
    cql_bool       value);

/**
 * Bind a time stamp to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_statement_bind_time(
    CqlStatement*  statement,
    size_t         index,
    cql_int64_t    value);

/**
 * Bind a UUID to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_statement_bind_uuid(
    CqlStatement*  statement,
    size_t         index,
    CqlUuid        value);

/**
 * Bind a counter to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_statement_bind_counter(
    CqlStatement*  statement,
    size_t         index,
    cql_int64_t    value);

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
CQL_EXPORT int
cql_statement_bind_string(
    CqlStatement*  statement,
    size_t         index,
    char*          value,
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
CQL_EXPORT int
cql_statement_bind_blob(
    CqlStatement*  statement,
    size_t         index,
    cql_uint8_t*   value,
    size_t         length);

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
CQL_EXPORT int
cql_statement_bind_decimal(
    CqlStatement* statement,
    size_t        index,
    cql_uint32_t  scale,
    cql_uint8_t*  value,
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
CQL_EXPORT int
cql_statement_bind_varint(
    CqlStatement*  statement,
    size_t         index,
    cql_uint8_t*   value,
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
CQL_EXPORT int
cql_session_exec(
    CqlSession*   session,
    CqlStatement* statement,
    CqlFuture**   future);

/***********************************************************************************
 *
 * Functions dealing with batching multiple statements
 *
 ***********************************************************************************/

CQL_EXPORT int 
cql_session_batch(
    CqlSession*         session,
    CqlConsistency      consistency,
    CqlBatchStatement** output);

CQL_EXPORT int
cql_batch_add_statement(
    CqlBatchStatement* batch,
    CqlStatement*      statement);

CQL_EXPORT int
cql_batch_set_timestamp(
    CqlBatchStatement* batch,
    cql_int64_t        timestamp);

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
CQL_EXPORT int
cql_session_exec_batch(
    CqlSession*        session,
    CqlBatchStatement* statement,
    CqlFuture**        future);


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
CQL_EXPORT size_t
cql_result_rowcount(
    CqlResult* result);

/**
 * Get number of columns per row for the specified result
 *
 * @param result
 *
 * @return
 */
CQL_EXPORT size_t
cql_result_colcount(
    CqlResult* result);

/**
 * Get the type for the column at index for the specified result
 *
 * @param result
 * @param index
 * @param coltype output
 *
 * @return
 */
CQL_EXPORT int
cql_result_coltype(
    CqlResult*     result,
    size_t         index,
    CqlColumnType* coltype);

/**
 * Get an iterator for the specified result or collection. Iterator must be freed by caller.
 *
 * @param result
 * @param iterator
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_iterator_new(
    void*  value,
    void** iterator);

/**
 * Advance the iterator to the next row or collection item.
 *
 * @param iterator
 *
 * @return next row, NULL if no rows remain or error
 */
CQL_EXPORT int
cql_iterator_next(
    void* iterator);

/**
 * Get the column value at index for the specified row.
 *
 * @param row
 * @param index
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_row_getcol(
    void*  row,
    size_t index,
    void** value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_decode_short(
    void*       source,
    cql_int16_t value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_decode_int(
    void*       source,
    cql_int32_t value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_decode_bigint(
    void*       source,
    cql_int64_t value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_decode_float(
    void*  source,
    cql_float  value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_decode_double(
    void*       source,
    cql_double  value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_decode_bool(
    void*    source,
    cql_bool value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_decode_time(
    void*       source,
    cql_int64_t value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_decode_uuid(
    void*   source,
    CqlUuid value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_decode_counter(
    void*       source,
    cql_int64_t value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value. This function follows the pattern similar to that of snprintf. The user
 * passes in a pre-allocated buffer of size n, to which the decoded value will be
 * copied. The number of bytes written had the buffer been sufficiently large will be
 * returned via the output parameter 'total'. Only when total is less than n has
 * the buffer been fully consumed.
 *
 * @param source
 * @param output
 * @param output_len
 * @param total
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_decode_string(
    void*   source,
    char*   output,
    size_t  output_len,
    size_t* total);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value. This function follows the pattern similar to that of snprintf. The user
 * passes in a pre-allocated buffer of size n, to which the decoded value will be
 * copied. The number of bytes written had the buffer been sufficiently large will be
 * returned via the output parameter 'total'. Only when total is less than n has
 * the buffer been fully consumed.
 *
 * @param source
 * @param output
 * @param output_len
 * @param total
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_decode_blob(
    void*        source,
    cql_uint8_t* output,
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
CQL_EXPORT int
cql_decode_decimal(
    void*         source,
    cql_uint32_t* scale,
    cql_uint8_t** value,
    size_t*       len);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value. This function follows the pattern similar to that of snprintf. The user
 * passes in a pre-allocated buffer of size n, to which the decoded value will be
 * copied. The number of bytes written had the buffer been sufficiently large will be
 * returned via the output parameter 'total'. Only when total is less than n has
 * the buffer been fully consumed.
 *
 * @param source
 * @param output
 * @param output_len
 * @param total
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_decode_varint(
    void*        source,
    cql_uint8_t* output,
    size_t       output_len,
    size_t*      total);

/**
 * Get the number of items in a collection. Works for all collection types.
 *
 * @param source collection column
 *
 * @return size, 0 if error
 */
CQL_EXPORT size_t
cql_collection_count(
    void*  source);

/**
 * Get the collection sub-type. Works for collections that have a single sub-type
 * (lists and sets).
 *
 * @param collection
 * @param output
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_collection_subtype(
    void*   collection,
    CqlColumnType* output);

/**
 * Get the sub-type of the key for a map collection. Works only for maps.
 *
 * @param collection
 * @param output
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_collection_map_key_type(
    void*   collection,
    CqlColumnType* output);

/**
 * Get the sub-type of the value for a map collection. Works only for maps.
 *
 * @param collection
 * @param output
 *
 * @return
 */
CQL_EXPORT int
cql_collection_map_value_type(
    void*   collection,
    CqlColumnType* output);

/**
 * Use an iterator to obtain each pair from a map. Once a pair has been obtained from
 * the iterator use this function to fetch the key portion of the pair
 *
 * @param item
 * @param output
 *
 * @return 0 if successful, otherwise error occured
 */
CQL_EXPORT int
cql_map_get_key(
    void*  item,
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
CQL_EXPORT int
cql_map_get_value(
    void*  item,
    void** output);

/***********************************************************************************
 *
 * Load balancing policy
 *
 ************************************************************************************/

CQL_EXPORT size_t
cql_lb_policy_hosts_count(CqlLoadBalancingPolicy* policy);

CQL_EXPORT CqlHost*
cql_lb_policy_get_host(CqlLoadBalancingPolicy* policy, size_t index);

CQL_EXPORT void*
cql_lb_policy_get_data(CqlLoadBalancingPolicy* policy);

CQL_EXPORT const char*
cql_host_get_address(CqlHost* host);

CQL_EXPORT void
cql_session_set_load_balancing_policy(CqlSession* session,
                                      void* data,
                                      CqlLoadBalancingPolicyImpl* impl);

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
CQL_EXPORT void
cql_uuid_v1(
    CqlUuid* output);

/**
 * Generate a new V1 (time) UUID for the specified time
 *
 * @param output
 */
CQL_EXPORT void
cql_uuid_v1_for_time(
    cql_uint64_t  time,
    CqlUuid*      output);

/**
 * Generate a new V4 (random) UUID
 *
 * @param output
 *
 * @return
 */
CQL_EXPORT void
cql_uuid_v4(
    CqlUuid* output);

/**
 * Return the corresponding null terminated string for the specified UUID.
 *
 * @param uuid
 * @param output char[37]
 *
 * @return
 */
CQL_EXPORT int
cql_uuid_string(
    CqlUuid uuid,
    char*   output);

#ifdef __cplusplus
} /* extern "C" */
#endif

#define CQL_ERROR_SOURCE_OS          1
#define CQL_ERROR_SOURCE_NETWORK     2
#define CQL_ERROR_SOURCE_SSL         3
#define CQL_ERROR_SOURCE_COMPRESSION 4
#define CQL_ERROR_SOURCE_SERVER      5
#define CQL_ERROR_SOURCE_LIBRARY     6

#define CQL_ERROR_NO_ERROR            0

#define CQL_ERROR_SSL_CERT            1000000
#define CQL_ERROR_SSL_PRIVATE_KEY     1000001
#define CQL_ERROR_SSL_CA_CERT         1000002
#define CQL_ERROR_SSL_CRL             1000003
#define CQL_ERROR_SSL_READ            1000004
#define CQL_ERROR_SSL_WRITE           1000005
#define CQL_ERROR_SSL_READ_WAITING    1000006
#define CQL_ERROR_SSL_WRITE_WAITING   1000007

#define CQL_ERROR_LIB_BAD_PARAMS      2000001
#define CQL_ERROR_LIB_INVALID_OPTION  2000002
#define CQL_ERROR_LIB_NO_STREAMS      2000008
#define CQL_ERROR_LIB_MAX_CONNECTIONS 2000009
#define CQL_ERROR_LIB_SESSION_STATE   2000010
#define CQL_ERROR_LIB_MESSAGE_PREPARE 2000011
#define CQL_ERROR_LIB_HOST_RESOLUTION 2000012

#endif /* __CQL_H_INCLUDED__ */
