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

typedef uint8_t cql_uuid_t[16];

struct cql_inet_t {
   uint8_t  length;
   uint8_t  address[6];
   uint32_t port;
};

/**
 * Free an instance allocated by the library, works for all types unless otherwise noted
 *
 * @param instance
 */
CQL_EXPORT void
cql_free(
   void* instance);

/**
 * Initialize a new cluster builder
 *
 * @return
 */
CQL_EXPORT void*
cql_builder_new();


/**
 * Set an option on the specified connection builder
 *
 * @param builder
 * @param option
 * @param data
 * @param datalen
 *
 * @return error
 */
CQL_EXPORT int
cql_builder_setopt(
   void   builder,
   int    option,
   void*  data,
   size_t datalen);

/**
 * Get the option value for the specified builder
 *
 * @param option
 * @param data
 * @param datalen
 *
 * @return error
 */
CQL_EXPORT int
cql_builder_getopt(
   int     option,
   void**  data,
   size_t* datalen);

/**
 * Instantiate a new cluster using the specified builder instance
 *
 * @param builder
 * @param cluster
 *
 * @return error
 */
CQL_EXPORT int
cql_cluster_new(
   void*  builder,
   void** cluster);

/**
 * Initiate a session using the specified cluster. Resulting
 * future must be freed by caller.
 *
 * @param cluster
 * @param future output future, must be freed by caller, pass NULL to avoid allocation
 *
 * @return error
 */
CQL_EXPORT int
cql_cluster_connect(
   void*  cluster,
   void** future);

/**
 * Initiate a session using the specified cluster, and set the keyspace. Resulting
 * future must be freed by caller.
 *
 * @param cluster
 * @param keyspace
 * @param future output future, must be freed by caller, pass NULL to avoid allocation
 *
 * @return error
 */
CQL_EXPORT int
cql_cluster_connect_keyspace(
   void*  cluster,
   char*  keyspace,
   void** future);

/**
 * Is the specified future ready
 *
 * @param future
 *
 * @return true if ready
 */
CQL_EXPORT int
cql_future_ready(
   void* future);

/**
 * Wait the linked event occurs or error condition is reached
 *
 * @param future
 */
CQL_EXPORT void
cql_future_wait(
   void* future);

/**
 * Wait the linked event occurs, error condition is reached, or time has elapsed.
 *
 * @param future
 * @param wait time in milliseconds
 *
 * @return false if returned due to timeout
 */
CQL_EXPORT int
cql_future_wait_timed(
   void*    future,
   uint64_t wait);

/**
 * If the linked event resulted in an error, get that error
 *
 * @param future
 *
 * @return error
 */
CQL_EXPORT int
cql_future_get_error(
   void* future);

/**
 * If the server returned an error message obtain the string. This function follows
 * the pattern similar to that of snprintf. The user passes in a pre-allocated buffer
 * of size n, to which the decoded value will be copied. The number of bytes written
 * had the buffer been sufficiently large will be returned via the output parameter
 * 'total'. Only when total is less than n has the buffer been fully consumed.
 *
 * @param source
 * @param output
 * @param n
 * @param total
 *
 * @return error
 */
CQL_EXPORT int
cql_future_get_error_string(
   void*   future,
   char*   output,
   size_t  n,
   size_t* total);

/**
 * Get the data returned by the linked event and set the underlying
 * pointer to NULL. Once the data is released ownership is transferred
 * to the caller. Prior to release the life-cycle of the data is bound
 * to the future. This method may only be called once.
 *
 * @param future
 * @param data
 *
 * @return error
 */
CQL_EXPORT int
cql_future_release_data(
   void*  future,
   void** data);

/**
 * Shutdown the session instance, output a shutdown future which can
 * be used to determine when the session has been terminated
 *
 * @param session
 */
CQL_EXPORT void
cql_session_shutdown(
   void*  session);

/**
 * Initialize a query statement, reserving N slots for parameters
 *
 * @param session
 * @param query
 * @param length
 * @param query
 *
 * @return error
 */
CQL_EXPORT int
cql_session_query(
   void*  session,
   char*  query,
   size_t length,
   size_t params,
   void** query);

/**
 * Create a prepared statement. Future must be freed by caller.
 *
 * @param session
 * @param query
 * @param length
 * @param future output future, must be freed by caller, pass NULL to avoid allocation
 *
 * @return
 */
CQL_EXPORT int
cql_session_prepare(
   void*  session,
   char*  query,
   size_t length,
   void** future);

/**
 * Initialize a bound statement, reserving N slots for parameters
 *
 * @param session
 * @param prepared
 * @param params count
 * @param bound
 *
 * @return error
 */
CQL_EXPORT int
cql_prepared_bind(
   void*  session,
   void*  prepared,
   size_t params,
   void** bound);

/**
 * Execute a query or bound statement and obtain a future. Future must be freed by
 * caller.
 *
 * @param session
 * @param statement
 * @param future output future, must be freed by caller, pass NULL to avoid allocation
 *
 * @return error
 */
CQL_EXPORT int
cql_statement_exec(
   void*  session,
   void*  statement,
   void** future);

CQL_EXPORT int
cql_statement_setopt(
   int    option,
   void*  data,
   size_t datalen);

CQL_EXPORT int
cql_statement_getopt(
   int     option,
   void**  data,
   size_t* datalen);

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
CQL_EXPORT int
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
 * @return error
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
CQL_EXPORT void*
cql_iterator_next(
   void* iterator);

/**
 * Get the column value at index for the specified row.
 *
 * @param row
 * @param index
 * @param value
 *
 * @return error
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
 * @return error
 */
CQL_EXPORT int
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
 * @return error
 */
CQL_EXPORT int
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
 * @return error
 */
CQL_EXPORT int
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
 * @return error
 */
CQL_EXPORT int
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
 * @return error
 */
CQL_EXPORT int
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
 * @return error
 */
CQL_EXPORT int
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
 * @return error
 */
CQL_EXPORT int
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
 * @return error
 */
CQL_EXPORT int
cql_decode_uuid(
   void*      source,
   cql_uuid_t value);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return error
 */
CQL_EXPORT int
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
 * @return error
 */
CQL_EXPORT int
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
 * @return error
 */
CQL_EXPORT int
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
 * @return error
 */
CQL_EXPORT int
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
 * @return error
 */
CQL_EXPORT int
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
 * @return error
 */
CQL_EXPORT int
cql_collection_subtype(
   void*   collection,
   size_t* output);

/**
 * Get the sub-type of the key for a map collection. Works only for maps.
 *
 * @param collection
 * @param output
 *
 * @return error
 */
CQL_EXPORT int
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
CQL_EXPORT int
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
 * @return error
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
 * @return error
 */
CQL_EXPORT int
cql_map_get_value(
   void*  item,
   void** output);

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
   cql_uuid_t* output);

/**
 * Generate a new V1 (time) UUID for the specified time
 *
 * @param output
 */
CQL_EXPORT void
cql_uuid_v1_for_time(
   uint64_t    time,
   cql_uuid_t* output);

/**
 * Generate a new V4 (random) UUID
 *
 * @param output
 *
 * @return
 */
CQL_EXPORT void
cql_uuid_v4(
   cql_uuid_t* output);

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
   cql_uuid_t uuid,
   char*      output);


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
#define CQL_CONSISTENCY_DEFAULT      CQL_CONSISTENCY_ONE

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


#endif // __CQL_H_INCLUDED__
