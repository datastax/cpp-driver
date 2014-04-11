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

#include "cql.h"

/**
 * Free an instance allocated by the library, works for all types unless otherwise noted
 *
 * @param instance
 */
void
cql_free(
    void* instance) {
  (void) instance;
}

/**
 * Initialize a new cluster builder. Instance must be freed by caller.
 *
 * @return
 */
void*
cql_builder_new() {
  return NULL;
}

/**
 * Set an option on the specified connection builder
 *
 * @param builder
 * @param option
 * @param data
 * @param datalen
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_builder_setopt(
    void*  builder,
    int    option,
    void*  data,
    size_t datalen) {
  (void) builder;
  (void) option;
  (void) data;
  (void) datalen;
  return nullptr;
}

/**
 * Get the option value for the specified builder
 *
 * @param option
 * @param data
 * @param datalen
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_builder_getopt(
    int     option,
    void**  data,
    size_t* datalen) {
  return nullptr;
}

/**
 * Instantiate a new cluster using the specified builder instance. Instance must be freed by caller.
 *
 * @param builder
 * @param cluster
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_cluster_new(
    void*  builder,
    void** cluster) {
  return nullptr;
}

/**
 * Initiate a session using the specified cluster. Resulting
 * future must be freed by caller.
 *
 * @param cluster
 * @param future output future, must be freed by caller, pass NULL to avoid allocation
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_cluster_connect(
    void*  cluster,
    void** future) {
  return nullptr;
}

/**
 * Initiate a session using the specified cluster, and set the keyspace. Resulting
 * future must be freed by caller.
 *
 * @param cluster
 * @param keyspace
 * @param future output future, must be freed by caller, pass NULL to avoid allocation
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_cluster_connect_keyspace(
    void*  cluster,
    char*  keyspace,
    void** future) {
  return nullptr;
}

/***********************************************************************************
 *
 * Functions which allow for the interaction with futures. Everything that the library
 * does is asynchronous, and we use futures to let the caller know when the task is
 * complete and if that task returned data or resulted in an error.
 *
 ************************************************************************************/

/**
 * Is the specified future ready
 *
 * @param future
 *
 * @return true if ready
 */
int
cql_future_ready(
    void* future) {
  return 0;
}

/**
 * Wait the linked event occurs or error condition is reached
 *
 * @param future
 */
void
cql_future_wait(
    void* future) {
}

/**
 * Wait the linked event occurs, error condition is reached, or time has elapsed.
 *
 * @param future
 * @param wait time in milliseconds
 *
 * @return false if returned due to timeout
 */
int
cql_future_wait_timed(
    void*    future,
    uint64_t wait) {
  return 0;
}

/**
 * If the linked event resulted in an error, get that error
 *
 * @param future
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_future_get_error(
    void* future) {
  return nullptr;
}

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
 * @return error pointer, NULL otherwise
 */
void*
cql_future_get_error_string(
    void*   future,
    char*   output,
    size_t  n,
    size_t* total) {
  return nullptr;
}

/**
 * Get the data returned by the linked event and set the underlying
 * pointer to NULL. Once the data is released ownership is transferred
 * to the caller. Prior to release the life-cycle of the data is bound
 * to the future. This method may only be called once.
 *
 * @param future
 * @param data
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_future_release_data(
    void*  future,
    void** data) {
  return nullptr;
}

/**
 * Shutdown the session instance, output a shutdown future which can
 * be used to determine when the session has been terminated
 *
 * @param session
 */
void
cql_session_shutdown(
    void*  session) {
}

/***********************************************************************************
 *
 * Functions which create ad-hoc queries, prepared statements, bound statements, and
 * allow for the composition of batch statements from queries and bound statements.
 *
 ************************************************************************************/

/**
 * Initialize a query statement, reserving N slots for parameters
 *
 * @param session
 * @param query
 * @param length
 * @param query
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_session_query(
    void*  session,
    char*  statement,
    size_t length,
    size_t params,
    void** query) {
  return nullptr;
}

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
void*
cql_session_prepare(
    void*  session,
    char*  query,
    size_t length,
    void** future) {
  return nullptr;
}

/**
 * Initialize a bound statement from a pre-prepared statement, reserving N slots for
 * parameters
 *
 * @param session
 * @param prepared
 * @param params count
 * @param bound
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_prepared_bind(
    void*  session,
    void*  prepared,
    size_t params,
    void** bound) {
  return nullptr;
}

void*
cql_statement_setopt(
    int    option,
    void*  data,
    size_t datalen) {
  return nullptr;
}

void*
cql_statement_getopt(
    int     option,
    void**  data,
    size_t* datalen) {
  return nullptr;
}

/**
 * Bind a short to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
void*
cql_statement_bind_short(
    void*   statement,
    size_t  index,
    int16_t value) {
  return nullptr;
}

/**
 * Bind an int to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
void*
cql_statement_bind_int(
    void*   statement,
    size_t  index,
    int32_t value) {
  return nullptr;
}

/**
 * Bind a bigint to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
void*
cql_statement_bind_bigint(
    void*   statement,
    size_t  index,
    int64_t value) {
  return nullptr;
}

/**
 * Bind a float to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
void*
cql_statement_bind_float(
    void*  statement,
    size_t index,
    float  value) {
  return nullptr;
}

/**
 * Bind a double to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_statement_bind_double(
    void*  statement,
    size_t index,
    double value) {
  return nullptr;
}

/**
 * Bind a bool to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_statement_bind_bool(
    void*  statement,
    size_t index,
    float  value) {
  return nullptr;
}

/**
 * Bind a time stamp to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_statement_bind_time(
    void*   statement,
    size_t  index,
    int64_t value) {
  return nullptr;
}

/**
 * Bind a UUID to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_statement_bind_uuid(
    void*      statement,
    size_t     index,
    cql_uuid_t value) {
  return nullptr;
}

/**
 * Bind a counter to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_statement_bind_counter(
    void*   statement,
    size_t  index,
    int64_t value) {
  return nullptr;
}

/**
 * Bind a string to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 * @param length
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_statement_bind_string(
    void*   statement,
    size_t  index,
    char*   value,
    size_t  length) {
  return nullptr;
}

/**
 * Bind a blob to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param output
 * @param length
 * @param total
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_statement_bind_blob(
    void*    statement,
    size_t   index,
    uint8_t* output,
    size_t   length,
    size_t*  total) {
  return nullptr;
}

/**
 * Bind a decimal to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param scale
 * @param value
 * @param length
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_statement_bind_decimal(
    void*    statement,
    size_t   index,
    uint32_t scale,
    uint8_t* value,
    size_t   length) {
  return nullptr;
}

/**
 * Bind a varint to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 * @param length
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_statement_bind_varint(
    void*    statement,
    size_t   index,
    uint8_t* value,
    size_t   length) {
  return nullptr;
}

/**
 * Execute a query, bound or batch statement and obtain a future. Future must be freed by
 * caller.
 *
 * @param session
 * @param statement
 * @param future output future, must be freed by caller, pass NULL to avoid allocation
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_statement_exec(
    void*  session,
    void*  statement,
    void** future) {
  return nullptr;
}


/***********************************************************************************
 *
 * Functions dealing with fetching data and meta information from statement results
 *
 ************************************************************************************/


/**
 * Get number of rows for the specified result
 *
 * @param result
 *
 * @return
 */
size_t
cql_result_rowcount(
    void* result) {
  return 0;
}

/**
 * Get number of columns per row for the specified result
 *
 * @param result
 *
 * @return
 */
size_t
cql_result_colcount(
    void* result) {
  return 0;
}

/**
 * Get the type for the column at index for the specified result
 *
 * @param result
 * @param index
 * @param coltype output
 *
 * @return
 */
void*
cql_result_coltype(
    void*   result,
    size_t  index,
    size_t* coltype) {
  return nullptr;
}

/**
 * Get an iterator for the specified result or collection. Iterator must be freed by caller.
 *
 * @param result
 * @param iterator
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_iterator_new(
    void*  value,
    void** iterator) {
  return nullptr;
}

/**
 * Advance the iterator to the next row or collection item.
 *
 * @param iterator
 *
 * @return next row, NULL if no rows remain or error
 */
void*
cql_iterator_next(
    void* iterator) {
  return nullptr;
}

/**
 * Get the column value at index for the specified row.
 *
 * @param row
 * @param index
 * @param value
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_row_getcol(
    void*  row,
    size_t index,
    void** value) {
  return nullptr;
}

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_decode_short(
    void*   source,
    int16_t value) {
  return nullptr;
}

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_decode_int(
    void*   source,
    int32_t value) {
  return nullptr;
}

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_decode_bigint(
    void*   source,
    int64_t value) {
  return nullptr;
}

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_decode_float(
    void*  source,
    float  value) {
  return nullptr;
}

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_decode_double(
    void*  source,
    double  value) {
  return nullptr;
}

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_decode_bool(
    void*  source,
    float  value) {
  return nullptr;
}

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_decode_time(
    void*   source,
    int64_t value) {
  return nullptr;
}

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_decode_uuid(
    void*      source,
    cql_uuid_t value) {
  return nullptr;
}

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_decode_counter(
    void*   source,
    int64_t value) {
  return nullptr;
}

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
 * @return error pointer, NULL otherwise
 */
void*
cql_decode_string(
    void*   source,
    char*   output,
    size_t  n,
    size_t* total) {
  return nullptr;
}

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
 * @return error pointer, NULL otherwise
 */
void*
cql_decode_blob(
    void*    source,
    uint8_t* output,
    size_t   n,
    size_t*  total) {
  return nullptr;
}

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_decode_decimal(
    void*     source,
    uint32_t* scale,
    uint8_t** value,
    size_t*   len) {
  return nullptr;
}

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
 * @return error pointer, NULL otherwise
 */
void*
cql_decode_varint(
    void*    source,
    uint8_t* output,
    size_t   n,
    size_t*  total) {
  return nullptr;
}

/**
 * Get the number of items in a collection. Works for all collection types.
 *
 * @param source collection column
 *
 * @return size, 0 if error
 */
size_t
cql_collection_count(
    void*  source) {
  return 0;
}

/**
 * Get the collection sub-type. Works for collections that have a single sub-type
 * (lists and sets).
 *
 * @param collection
 * @param output
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_collection_subtype(
    void*   collection,
    size_t* output) {
  return nullptr;
}

/**
 * Get the sub-type of the key for a map collection. Works only for maps.
 *
 * @param collection
 * @param output
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_collection_map_key_type(
    void*   collection,
    size_t* output) {
  return nullptr;
}

/**
 * Get the sub-type of the value for a map collection. Works only for maps.
 *
 * @param collection
 * @param output
 *
 * @return
 */
void*
cql_collection_map_value_type(
    void*   collection,
    size_t* output) {
  return nullptr;
}

/**
 * Use an iterator to obtain each pair from a map. Once a pair has been obtained from
 * the iterator use this function to fetch the key portion of the pair
 *
 * @param item
 * @param output
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_map_get_key(
    void*  item,
    void** output) {
  return nullptr;
}

/**
 * Use an iterator to obtain each pair from a map. Once a pair has been obtained from
 * the iterator use this function to fetch the value portion of the pair
 *
 * @param item
 * @param output
 *
 * @return error pointer, NULL otherwise
 */
void*
cql_map_get_value(
    void*  item,
    void** output) {
  return nullptr;
}


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
size_t
cql_error_string(
    int    error,
    char*  output,
    size_t n) {
  return 0;
}

/**
 * Generate a new V1 (time) UUID
 *
 * @param output
 */
void
cql_uuid_v1(
    cql_uuid_t* output) {
}

/**
 * Generate a new V1 (time) UUID for the specified time
 *
 * @param output
 */
void
cql_uuid_v1_for_time(
    uint64_t    time,
    cql_uuid_t* output) {
}

/**
 * Generate a new V4 (random) UUID
 *
 * @param output
 *
 * @return
 */
void
cql_uuid_v4(
    cql_uuid_t* output) {
}

/**
 * Return the corresponding null terminated string for the specified UUID.
 *
 * @param uuid
 * @param output char[37]
 *
 * @return
 */
void*
cql_uuid_string(
    cql_uuid_t uuid,
    char*      output) {
  return nullptr;
}
