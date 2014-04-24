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
#include <stdint.h>

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

typedef uint8_t CqlUuid[16];

typedef struct {
  uint8_t  length;
  uint8_t  address[6];
  uint32_t port;
} CqlInet;

struct CqlBatchStatement;
struct CqlCluster;
struct CqlError;
struct CqlFuture;
struct CqlPrepared;
struct CqlResult;
struct CqlSession;
struct CqlStatement;

/**
 * Initialize a new cluster. Instance must be freed by caller.
 *
 * @return
 */
CQL_EXPORT CqlCluster*
cql_cluster_new();

/**
 * Free a cluster instance.
 *
 * @return
 */
CQL_EXPORT void
cql_cluster_free(
    CqlCluster* cluster);

/**
 * Set an option on the specified connection cluster
 *
 * @param cluster
 * @param option
 * @param data
 * @param datalen
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_cluster_setopt(
    CqlCluster* cluster,
    int                option,
    const void*        data,
    size_t             datalen);

/**
 * Get the option value for the specified cluster
 *
 * @param option
 * @param data
 * @param datalen
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_cluster_getopt(
    CqlCluster* cluster,
    int                option,
    void**             data,
    size_t*            datalen);

/**
 * Instantiate a new session using the specified cluster instance. Instance must be freed by caller.
 *
 * @param cluster
 * @param session
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_session_new(
    CqlCluster*  cluster,
    CqlSession** session);

/**
 * Free a session instance.
 *
 * @return
 */
CQL_EXPORT void
cql_session_free(
    CqlSession* session);

/**
 * Initiate a session using the specified session. Resulting
 * future must be freed by caller.
 *
 * @param session
 * @param future output future, must be freed by caller, pass NULL to avoid allocation
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_session_connect(
    CqlSession*        session,
    CqlFuture** future);

/**
 * Initiate a session using the specified session, and set the keyspace. Resulting
 * future must be freed by caller.
 *
 * @param session
 * @param keyspace
 * @param future output future, must be freed by caller, pass NULL to avoid allocation
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_session_connect_keyspace(
    CqlSession* session,
    char*              keyspace,
    CqlFuture** future);

/**
 * Shutdown the session instance, output a shutdown future which can
 * be used to determine when the session has been terminated
 *
 * @param session
 */
CQL_EXPORT CqlError*
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
 * If the linked event resulted in an error, get that error
 *
 * @param future
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_future_get_error(
    CqlFuture* future);

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
 * @param n
 * @param total
 *
 */
CQL_EXPORT void
cql_error_string(
    CqlError* error,
    char*     output,
    size_t    n,
    size_t*   total);

/**
 * Obtain the code from an error structure.
 *
 * @param error
 *
 * @return error code
 *
 */
CQL_EXPORT int
cql_error_source(
    CqlError* error);

/**
 * Obtain the source from an error structure.
 *
 * @param error
 *
 * @return source code
 *
 */
CQL_EXPORT int
cql_error_code(
    CqlError* error);


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
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_session_query(
    CqlSession*    session,
    char*          statement,
    size_t         statement_length,
    size_t         paramater_count,
    size_t         consistency,
    CqlStatement** output);

/**
 * Create a prepared statement. Future must be freed by caller.
 *
 * @param session
 * @param statement string
 * @param statement_length statement string length
 * @param future output future, must be freed by caller, pass NULL to avoid allocation
 *
 * @return
 */
CQL_EXPORT CqlError*
cql_session_prepare(
    CqlSession*        session,
    char*              statement,
    size_t             statement_length,
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
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_session_bind(
    CqlSession*    session,
    CqlPrepared*   prepared,
    size_t         paramater_count,
    size_t         consistency,
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
CQL_EXPORT CqlError*
cql_statement_bind_short(
    CqlStatement* statement,
    size_t        index,
    int16_t       value);

/**
 * Bind an int to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
CQL_EXPORT CqlError*
cql_statement_bind_int(
    CqlStatement* statement,
    size_t        index,
    int32_t       value);

/**
 * Bind a bigint to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
CQL_EXPORT CqlError*
cql_statement_bind_bigint(
    CqlStatement* statement,
    size_t        index,
    int64_t       value);

/**
 * Bind a float to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
CQL_EXPORT CqlError*
cql_statement_bind_float(
    CqlStatement* statement,
    size_t        index,
    float         value);

/**
 * Bind a double to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_statement_bind_double(
    CqlStatement*  statement,
    size_t         index,
    double         value);

/**
 * Bind a bool to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_statement_bind_bool(
    CqlStatement*  statement,
    size_t         index,
    float          value);

/**
 * Bind a time stamp to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_statement_bind_time(
    CqlStatement*  statement,
    size_t         index,
    int64_t        value);

/**
 * Bind a UUID to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
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
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_statement_bind_counter(
    CqlStatement*  statement,
    size_t         index,
    int64_t        value);

/**
 * Bind a string to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 * @param length
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
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
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_statement_bind_blob(
    CqlStatement*  statement,
    size_t         index,
    uint8_t*       value,
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
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_statement_bind_decimal(
    CqlStatement* statement,
    size_t        index,
    uint32_t      scale,
    uint8_t*      value,
    size_t        length);

/**
 * Bind a varint to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 * @param length
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_statement_bind_varint(
    CqlStatement*  statement,
    size_t         index,
    uint8_t*       value,
    size_t         length);

/**
 * Execute a query, bound statement and obtain a future. Future must be freed by
 * caller.
 *
 * @param session
 * @param statement
 * @param future output future, must be freed by caller, pass NULL to avoid allocation
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_statement_exec(
    CqlSession*       session,
    CqlStatement*     statement,
    CqlFuture** future);

/***********************************************************************************
 *
 * Functions dealing with batching multiple statements
 *
 ***********************************************************************************/

CQL_EXPORT CqlError*
cql_session_batch(
    CqlSession*         session,
    size_t              consistency,
    CqlBatchStatement** output);

CQL_EXPORT CqlError*
cql_batch_add_statement(
    CqlBatchStatement* batch,
    CqlStatement*      statement);

/**
 * Execute a batch statement and obtain a future. Future must be freed by
 * caller.
 *
 * @param session
 * @param statement
 * @param future output future, must be freed by caller, pass NULL to avoid allocation
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_batch_statement_exec(
    CqlSession*        session,
    CqlBatchStatement* statement,
    CqlFuture**  future);


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
    void* result);

/**
 * Get number of columns per row for the specified result
 *
 * @param result
 *
 * @return
 */
CQL_EXPORT size_t
cql_result_colcount(
    void* result);

/**
 * Get the type for the column at index for the specified result
 *
 * @param result
 * @param index
 * @param coltype output
 *
 * @return
 */
CQL_EXPORT CqlError*
cql_result_coltype(
    void*   result,
    size_t  index,
    size_t* coltype);

/**
 * Get an iterator for the specified result or collection. Iterator must be freed by caller.
 *
 * @param result
 * @param iterator
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
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
CQL_EXPORT CqlError*
cql_iterator_next(
    void* iterator);

/**
 * Get the column value at index for the specified row.
 *
 * @param row
 * @param index
 * @param value
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
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
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_decode_short(
    void*   source,
    int16_t value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_decode_int(
    void*   source,
    int32_t value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_decode_bigint(
    void*   source,
    int64_t value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_decode_float(
    void*  source,
    float  value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_decode_double(
    void*  source,
    double  value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_decode_bool(
    void*  source,
    float  value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_decode_time(
    void*   source,
    int64_t value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_decode_uuid(
    void*      source,
    CqlUuid value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_decode_counter(
    void*   source,
    int64_t value);

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
 * @param n
 * @param total
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_decode_string(
    void*   source,
    char*   output,
    size_t  n,
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
 * @param n
 * @param total
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_decode_blob(
    void*    source,
    uint8_t* output,
    size_t   n,
    size_t*  total);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_decode_decimal(
    void*     source,
    uint32_t* scale,
    uint8_t** value,
    size_t*   len);

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
 * @param n
 * @param total
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_decode_varint(
    void*    source,
    uint8_t* output,
    size_t   n,
    size_t*  total);

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
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_collection_subtype(
    void*   collection,
    size_t* output);

/**
 * Get the sub-type of the key for a map collection. Works only for maps.
 *
 * @param collection
 * @param output
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
cql_collection_map_key_type(
    void*   collection,
    size_t* output);

/**
 * Get the sub-type of the value for a map collection. Works only for maps.
 *
 * @param collection
 * @param output
 *
 * @return
 */
CQL_EXPORT CqlError*
cql_collection_map_value_type(
    void*   collection,
    size_t* output);

/**
 * Use an iterator to obtain each pair from a map. Once a pair has been obtained from
 * the iterator use this function to fetch the key portion of the pair
 *
 * @param item
 * @param output
 *
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT CqlError*
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
 * @return NULL if successful, otherwise pointer to CqlError structure
 */
CQL_EXPORT int
cql_map_get_value(
    void*  item,
    void** output);


/***********************************************************************************
 *
 * Misc
 *
 ************************************************************************************/


/**
 * Get the corresponding string for the specified error. This function follows the
 * pattern similar to that of snprintf. The user passes in a pre-allocated buffer of
 * size n, to which the decoded value will be copied. The number of bytes written had
 * the buffer been sufficiently large will be returned. Only when total is less than n
 * has the buffer been fully consumed.
 *
 * @param error
 * @param output output buffer
 * @param n output buffer length
 *
 * @return total message size irrespective of output buffer size
 */
CQL_EXPORT size_t
cql_error_string(
    int    error,
    char*  output,
    size_t n);

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
    uint64_t    time,
    CqlUuid* output);

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
CQL_EXPORT CqlError*
cql_uuid_string(
    CqlUuid uuid,
    char*   output);

#define CQL_LOG_CRITICAL 0x00
#define CQL_LOG_ERROR    0x01
#define CQL_LOG_INFO     0x02
#define CQL_LOG_DEBUG    0x03

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
#define CQL_ERROR_LIB_NO_STREAMS      2000008
#define CQL_ERROR_LIB_MAX_CONNECTIONS 2000009
#define CQL_ERROR_LIB_SESSION_STATE   2000010

#define CQL_CONSISTENCY_ANY          0x0000
#define CQL_CONSISTENCY_ONE          0x0001
#define CQL_CONSISTENCY_TWO          0x0002
#define CQL_CONSISTENCY_THREE        0x0003
#define CQL_CONSISTENCY_QUORUM       0x0004
#define CQL_CONSISTENCY_ALL          0x0005
#define CQL_CONSISTENCY_LOCAL_QUORUM 0x0006
#define CQL_CONSISTENCY_EACH_QUORUM  0x0007
#define CQL_CONSISTENCY_SERIAL       0x0008
#define CQL_CONSISTENCY_LOCAL_SERIAL 0x0009
#define CQL_CONSISTENCY_LOCAL_ONE    0x000A

#define CQL_COLUMN_TYPE_UNKNOWN   0xFFFF
#define CQL_COLUMN_TYPE_CUSTOM    0x0000
#define CQL_COLUMN_TYPE_ASCII     0x0001
#define CQL_COLUMN_TYPE_BIGINT    0x0002
#define CQL_COLUMN_TYPE_BLOB      0x0003
#define CQL_COLUMN_TYPE_BOOLEAN   0x0004
#define CQL_COLUMN_TYPE_COUNTER   0x0005
#define CQL_COLUMN_TYPE_DECIMAL   0x0006
#define CQL_COLUMN_TYPE_DOUBLE    0x0007
#define CQL_COLUMN_TYPE_FLOAT     0x0008
#define CQL_COLUMN_TYPE_INT       0x0009
#define CQL_COLUMN_TYPE_TEXT      0x000A
#define CQL_COLUMN_TYPE_TIMESTAMP 0x000B
#define CQL_COLUMN_TYPE_UUID      0x000C
#define CQL_COLUMN_TYPE_VARCHAR   0x000D
#define CQL_COLUMN_TYPE_VARINT    0x000E
#define CQL_COLUMN_TYPE_TIMEUUID  0x000F
#define CQL_COLUMN_TYPE_INET      0x0010
#define CQL_COLUMN_TYPE_LIST      0x0020
#define CQL_COLUMN_TYPE_MAP       0x0021
#define CQL_COLUMN_TYPE_SET       0x0022

#define CQL_OPTION_THREADS_IO                 1
#define CQL_OPTION_THREADS_CALLBACK           2
#define CQL_OPTION_CONTACT_POINT_ADD          3
#define CQL_OPTION_PORT                       4
#define CQL_OPTION_CQL_VERSION                5
#define CQL_OPTION_SCHEMA_AGREEMENT_WAIT      6
#define CQL_OPTION_CONTROL_CONNECTION_TIMEOUT 7

#define CQL_OPTION_COMPRESSION                9
#define CQL_OPTION_COMPRESSION_NONE           0
#define CQL_OPTION_COMPRESSION_SNAPPY         1
#define CQL_OPTION_COMPRESSION_LZ4            2

#endif // __CQL_H_INCLUDED__
