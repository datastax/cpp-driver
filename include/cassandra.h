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

typedef unsigned char cass_byte_t;
typedef size_t cass_size_t;

typedef cass_uint8_t cass_uuid_t[16];

typedef struct {
  cass_uint8_t address[16];
  cass_uint8_t address_length;
} cass_inet_t;

typedef struct cass_session_s cass_session_t;
typedef struct cass_statement_s cass_statement_t;
typedef struct cass_batch_s cass_batch_t;
typedef struct cass_future_s cass_future_t;
typedef struct cass_prepared_s cass_prepared_t;
typedef struct cass_result_s cass_result_t;
typedef struct cass_iterator_s cass_iterator_t;
typedef struct cass_row_s cass_row_t;
typedef struct cass_value_s cass_value_t;
typedef struct cass_collection_s cass_collection_t;

typedef enum {
  CASS_LOG_CRITICAL = 0x00,
  CASS_LOG_ERROR    = 0x01,
  CASS_LOG_INFO     = 0x02,
  CASS_LOG_DEBUG    = 0x03
} cass_log_level_t;

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
} cass_consistency_t;

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
} cass_value_type_t;

typedef enum {
  CASS_OPTION_THREADS_IO                 = 1,
  CASS_OPTION_THREADS_CALLBACK           = 2,
  CASS_OPTION_CONTACT_POINT_ADD          = 3,
  CASS_OPTION_PORT                       = 4,
  CASS_OPTION_CQL_VERSION                = 5,
  CASS_OPTION_SCHEMA_AGREEMENT_WAIT      = 6,
  CASS_OPTION_CONTROL_CONNECTION_TIMEOUT = 7,
  CASS_OPTION_COMPRESSION                = 9
} cass_option_t;

typedef enum {
  CASS_COMPRESSION_NONE   = 0,
  CASS_COMPRESSION_SNAPPY = 1,
  CASS_COMPRESSION_LZ4    = 2
} cass_compression_t;

typedef enum {
  CASS_HOST_DISTANCE_LOCAL,
  CASS_HOST_DISTANCE_REMOTE,
  CASS_HOST_DISTANCE_IGNORE
} cass_host_distance_t;


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
} cass_code_t;

typedef enum  {
  CASS_ERROR_SOURCE_NONE        = 0,
  CASS_ERROR_SOURCE_OS          = 1,
  CASS_ERROR_SOURCE_NETWORK     = 2,
  CASS_ERROR_SOURCE_SSL         = 3,
  CASS_ERROR_SOURCE_COMPRESSION = 4,
  CASS_ERROR_SOURCE_SERVER      = 5,
  CASS_ERROR_SOURCE_LIBRARY     = 6
} cass_source_t;

#ifdef __cplusplus
extern "C" {
#endif


/***********************************************************************************
 *
 * Session
 *
 ***********************************************************************************/

/**
 * Initialize a new session. Instance must be freed by caller.
 *
 * @return
 */
CASS_EXPORT cass_session_t*
cass_session_new();

/**
 * Initialize a new session using previous session's configuration.
 * Instance must be freed by caller.
 *
 * @return
 */
CASS_EXPORT cass_session_t*
cass_session_clone(const cass_session_t* session);

/**
 * Free a session instance.
 *
 * @return
 */
CASS_EXPORT void
cass_session_free(cass_session_t* session);

/**
 * Shutdown the session instance, output a shutdown future which can
 * be used to determine when the session has been terminated
 *
 * @param session
 */
CASS_EXPORT cass_code_t
cass_session_shutdown(cass_session_t* session,
                      cass_future_t** future);

/**
 * Set an option on the specified session
 *
 * @param cluster
 * @param option
 * @param data
 * @param data_length
 *
 * @return 0 if successful, otherwise an error occurred
 */
CASS_EXPORT cass_code_t
cass_session_setopt(cass_session_t* session,
                    cass_option_t option,
                    const void* data, cass_size_t data_length);

/**
 * Get the option value for the specified session
 *
 * @param option
 * @param data
 * @param data_length
 *
 * @return 0 if successful, otherwise an error occurred
 */
CASS_EXPORT cass_code_t
cass_session_getopt(const cass_session_t* session,
                    cass_option_t option,
                    void** data, cass_size_t* data_length);

/**
 * Initiate a session using the specified session. Resulting
 * future must be freed by caller.
 *
 * @param cluster
 * @param future output future, must be freed by caller, pass NULL to avoid allocation
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT cass_code_t
cass_session_connect(cass_session_t* session,
                     cass_future_t** future);

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
CASS_EXPORT cass_code_t
cass_session_connect_keyspace(cass_session_t* session,
                              const char* keyspace,
                              cass_future_t** future);

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
CASS_EXPORT cass_code_t
cass_session_prepare(cass_session_t* session,
                     const char* statement, cass_size_t statement_length,
                     cass_future_t** output);

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
CASS_EXPORT cass_code_t
cass_session_exec(cass_session_t* session,
                  cass_statement_t* statement,
                  cass_future_t** future);

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
CASS_EXPORT cass_code_t
cass_session_exec_batch(cass_session_t* session,
                        cass_batch_t* statement,
                        cass_future_t** future);

/***********************************************************************************
 *
 * Future
 *
 ***********************************************************************************/

/**
 * Free a session instance.
 *
 * @return
 */
CASS_EXPORT void
cass_future_free(cass_future_t* future);

/**
 * Is the specified future ready
 *
 * @param future
 *
 * @return true if ready
 */
CASS_EXPORT int
cass_future_ready(cass_future_t* future);

/**
 * Wait the linked event occurs or error condition is reached
 *
 * @param future
 */
CASS_EXPORT void
cass_future_wait(cass_future_t* future);

/**
 * Wait the linked event occurs, error condition is reached, or time has elapsed.
 *
 * @param future
 * @param wait time in microseconds
 *
 * @return false if returned due to timeout
 */
CASS_EXPORT int
cass_future_wait_timed(cass_future_t* future,
                       cass_size_t wait);

/**
 * If the linked event was successful get the session instance
 *
 * @param future
 *
 * @return NULL if unsuccessful, otherwise pointer to CassSession instance
 */
CASS_EXPORT cass_session_t*
cass_future_get_session(cass_future_t* future);

/**
 * If the linked event was successful get the result instance. Result
 * instance must be freed by caller. May only be called once.
 *
 * @param future
 *
 * @return NULL if unsuccessful, otherwise pointer to CassResult instance
 */
CASS_EXPORT const cass_result_t*
cass_future_get_result(cass_future_t* future);

/**
 * If the linked event was successful get the prepared
 * instance. Prepared instance must be freed by caller. May only be
 * called once
 *
 * @param future
 *
 * @return NULL if unsuccessful, otherwise pointer to CassPrepare instance
 */
CASS_EXPORT const cass_prepared_t*
cass_future_get_prepared(cass_future_t* future);

/**
 * Obtain the message from an error structure. This function follows
 * the pattern similar to that of snprintf. The user passes in a
 * pre-allocated builder of size n, to which the decoded value will be
 * copied. The number of bytes written had the builder been
 * sufficiently large will be returned via the output parameter
 * 'copied'. Only when copied is less than n has the builder been fully
 * consumed.
 *
 * @param source
 * @param output
 * @param output_length
 * @param copied
 *
 */
CASS_EXPORT void
cass_future_error_string(const cass_future_t* future,
                         char* output, cass_size_t output_length,
                         cass_size_t* copied);


CASS_EXPORT const char*
cass_future_error_message(const cass_future_t* future);

/**
 * Obtain the code from an error structure.
 *
 * @param error
 *
 * @return error source
 *
 */
CASS_EXPORT cass_source_t
cass_future_error_source(const cass_future_t* future);

/**
 * Obtain the source from an error structure.
 *
 * @param error
 *
 * @return source code
 *
 */
CASS_EXPORT cass_code_t
cass_future_error_code(const cass_future_t* future);


/***********************************************************************************
 *
 * Code
 *
 ***********************************************************************************/

/**
 * Get description for error code
 *
 * @param code
 *
 * @return error description
 */
CASS_EXPORT const char*
cass_code_error_desc(cass_code_t code);

/***********************************************************************************
 *
 * Statement
 *
 ***********************************************************************************/

/**
 * Initialize an ad-hoc query statement
 *
 * @param statement string
 * @param statement_length statement string length
 * @param parameter_count number of bound paramerters
 * @param consistency statement read/write consistency
 * @param output
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT cass_statement_t*
cass_statement_new(const char* statement, cass_size_t statement_length,
                   cass_size_t parameter_count,
                   cass_consistency_t consistency);


CASS_EXPORT void
cass_statement_free(cass_statement_t* statement);

/**
 * Bind an int to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
CASS_EXPORT cass_code_t
cass_statement_bind_int32(cass_statement_t* statement,
                        cass_size_t index,
                        cass_int32_t value);

/**
 * Bind a bigint to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
CASS_EXPORT cass_code_t
cass_statement_bind_int64(cass_statement_t* statement,
                           cass_size_t index,
                           cass_int64_t value);

/**
 * Bind a float to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
CASS_EXPORT cass_code_t
cass_statement_bind_float(cass_statement_t* statement,
                          cass_size_t index,
                          cass_float_t value);

/**
 * Bind a double to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT cass_code_t
cass_statement_bind_double(cass_statement_t* statement,
                           cass_size_t index,
                           cass_double_t value);

/**
 * Bind a bool to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT cass_code_t
cass_statement_bind_bool(cass_statement_t* statement,
                         cass_size_t index,
                         cass_bool_t value);

/**
 * Bind bytes to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 * @param length
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT cass_code_t
cass_statement_bind_string(cass_statement_t* statement,
                           cass_size_t index,
                           const char* value, cass_size_t value_length);

/**
 * Bind bytes to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 * @param length
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT cass_code_t
cass_statement_bind_bytes(cass_statement_t* statement,
                           cass_size_t index,
                           const cass_byte_t* value, cass_size_t value_length);

/**
 * Bind a UUID to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT cass_code_t
cass_statement_bind_uuid(cass_statement_t* statement,
                         cass_size_t index,
                         cass_uuid_t value);


CASS_EXPORT cass_code_t
cass_statement_bind_inet(cass_statement_t* statement,
                         cass_size_t index,
                         cass_inet_t value);

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
CASS_EXPORT cass_code_t
cass_statement_bind_decimal(cass_statement_t* statement,
                            cass_size_t index,
                            cass_uint32_t scale,
                            const cass_byte_t* varint, cass_size_t varint_length);


CASS_EXPORT cass_code_t
cass_statement_bind_custom(cass_statement_t* statement,
                           cass_size_t index,
                           cass_size_t size,
                           cass_byte_t** output);

CASS_EXPORT cass_code_t
cass_statement_bind_collection(cass_statement_t* statement,
                               cass_size_t index,
                               const cass_collection_t* collection,
                               cass_bool_t is_map);


/***********************************************************************************
 *
 * Prepared
 *
 ***********************************************************************************/

CASS_EXPORT void
cass_prepared_free(const cass_prepared_t* prepared);

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
CASS_EXPORT cass_code_t
cass_prepared_bind(const cass_prepared_t* prepared,
                   cass_size_t paramater_count,
                   cass_consistency_t consistency,
                   cass_statement_t** output);

/***********************************************************************************
 *
 * Batch
 *
 ***********************************************************************************/


CASS_EXPORT cass_batch_t*
cass_batch_new(cass_consistency_t consistency);

CASS_EXPORT void
cass_batch_free(cass_batch_t* batch);

CASS_EXPORT cass_code_t
cass_batch_add_statement(cass_batch_t* batch,
                         cass_statement_t* statement);

CASS_EXPORT void
cass_batch_set_timestamp(cass_batch_t* batch,
                         cass_int64_t timestamp);


/***********************************************************************************
 *
 * Collection
 *
 ***********************************************************************************/

CASS_EXPORT cass_collection_t*
cass_collection_new(cass_size_t item_count);

CASS_EXPORT void
cass_collection_free(cass_collection_t* collection);

CASS_EXPORT cass_code_t
cass_collection_append_int32(cass_collection_t* collection,
                             cass_int32_t value);

CASS_EXPORT cass_code_t
cass_collection_append_int64(cass_collection_t* collection,
                              cass_int64_t value);

CASS_EXPORT cass_code_t
cass_collection_append_float(cass_collection_t* collection,
                             cass_float_t value);

CASS_EXPORT cass_code_t
cass_collection_append_double(cass_collection_t* collection,
                              cass_double_t value);

CASS_EXPORT cass_code_t
cass_collection_append_bool(cass_collection_t* collection,
                            cass_bool_t value);

CASS_EXPORT cass_code_t
cass_collection_append_string(cass_collection_t* collection,
                              const char* value, cass_size_t value_length);

CASS_EXPORT cass_code_t
cass_collection_append_bytes(cass_collection_t* collection,
                             const cass_byte_t* value, cass_size_t value_length);

CASS_EXPORT cass_code_t
cass_collection_append_uuid(cass_collection_t* collection,
                            cass_uuid_t value);

CASS_EXPORT cass_code_t
cass_collection_append_inet(cass_collection_t* collection,
                            cass_inet_t value);

CASS_EXPORT cass_code_t
cass_collection_append_decimal(cass_collection_t* collection,
                               cass_int32_t scale,
                               const cass_byte_t* varint, cass_size_t varint_length);

/***********************************************************************************
 *
 * Result
 *
 ***********************************************************************************/

CASS_EXPORT void
cass_result_free(const cass_result_t* result);

/**
 * Get number of rows for the specified result
 *
 * @param result
 *
 * @return
 */
CASS_EXPORT cass_size_t
cass_result_row_count(const cass_result_t* result);

/**
 * Get number of columns per row for the specified result
 *
 * @param result
 *
 * @return
 */
CASS_EXPORT cass_size_t
cass_result_column_count(const cass_result_t* result);

/**
 * Get the type for the column at index for the specified result
 *
 * @param result
 * @param index
 * @param coltype output
 *
 * @return
 */
CASS_EXPORT cass_code_t
cass_result_column_type(const cass_result_t* result,
                        cass_size_t index,
                        cass_value_type_t* output);

/***********************************************************************************
 *
 * Iterator
 *
 ***********************************************************************************/

/**
 * Create new iterator for the specified result.
 *
 * @param result
 *
 * @return
 */
CASS_EXPORT cass_iterator_t*
cass_iterator_from_result(const cass_result_t* result);

/**
 * Create new iterator for the specified row.
 *
 * @param result
 *
 * @return
 */
CASS_EXPORT cass_iterator_t*
cass_iterator_from_row(const cass_row_t* row);

/**
 * Create new iterator for the specified collection.
 *
 * @param result
 *
 * @return
 */
CASS_EXPORT cass_iterator_t*
cass_iterator_from_collection(const cass_value_t* value);

CASS_EXPORT void
cass_iterator_free(cass_iterator_t* iterator);

/**
 * Advance the iterator to the next row or collection item.
 *
 * @param iterator
 *
 * @return next row, NULL if no rows remain or error
 */
CASS_EXPORT cass_bool_t
cass_iterator_next(cass_iterator_t* iterator);

CASS_EXPORT const cass_row_t*
cass_iterator_get_row(cass_iterator_t* iterator);

CASS_EXPORT const cass_value_t*
cass_iterator_get_column(cass_iterator_t *iterator);

CASS_EXPORT const cass_value_t*
cass_iterator_get_value(cass_iterator_t *iterator);


/***********************************************************************************
 *
 * Row
 *
 ***********************************************************************************/

/**
 * Get the column value at index for the specified row.
 *
 * @param row
 * @param index
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT cass_code_t
cass_row_get_column(const cass_row_t*  row,
                    cass_size_t index,
                    const cass_value_t** value);

/***********************************************************************************
 *
 * Value
 *
 ***********************************************************************************/

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT cass_code_t
cass_value_get_int32(const cass_value_t* value,
                   cass_int32_t* output);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT cass_code_t
cass_value_get_int64(const cass_value_t* value,
                     cass_int64_t* output);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT cass_code_t
cass_value_get_float(const cass_value_t* value,
                     cass_float_t*  output);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT cass_code_t
cass_value_get_double(const cass_value_t* value,
                      cass_double_t* output);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT cass_code_t
cass_value_get_bool(const cass_value_t* value,
                    cass_bool_t* output);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT cass_code_t
cass_value_get_uuid(const cass_value_t* value,
                    cass_uuid_t output);


/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value. This function follows the pattern similar to that of snprintf. The user
 * passes in a pre-allocated builder of size n, to which the decoded value will be
 * copied. The number of bytes written had the builder been sufficiently large will be
 * returned via the output parameter 'copied'. Only when copied is less than n has
 * the builder been fully consumed.
 *
 * @param source
 * @param output
 * @param output_length
 * @param copied
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT cass_code_t
cass_value_get_string(const cass_value_t* value,
                      char* output,
                      cass_size_t  output_length,
                      cass_size_t* copied);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value. This function follows the pattern similar to that of snprintf. The user
 * passes in a pre-allocated builder of size n, to which the decoded value will be
 * copied. The number of bytes written had the builder been sufficiently large will be
 * returned via the output parameter 'copied'. Only when copied is less than n has
 * the builder been fully consumed.
 *
 * @param source
 * @param output
 * @param output_length
 * @param copied
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT cass_code_t
cass_value_get_bytes(const cass_value_t* value,
                    cass_byte_t* output,
                    cass_size_t  output_length,
                    cass_size_t* copied);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param source
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT cass_code_t
cass_value_get_decimal(const cass_value_t* value,
                       cass_uint32_t* scale,
                       cass_byte_t* varint, cass_size_t varint_length,
                       cass_size_t* copied);


CASS_EXPORT const cass_byte_t*
cass_value_get_data(const cass_value_t* value);

CASS_EXPORT cass_size_t
cass_value_get_size(const cass_value_t* value);

/**
 * Get the collection sub-type. Works for collections that have a single sub-type
 * (lists and sets).
 *
 * @param collection
 * @param output
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT cass_value_type_t
cass_value_type(const cass_value_t* value);

CASS_EXPORT cass_bool_t
cass_value_is_collection(const cass_value_t* value);

/**
 * Get the number of items in a collection. Works for all collection types.
 *
 * @param source collection column
 *
 * @return size, 0 if error
 */
CASS_EXPORT cass_size_t
cass_value_item_count(const cass_value_t* collection);

CASS_EXPORT cass_value_type_t
cass_value_primary_sub_type(const cass_value_t* collection);

CASS_EXPORT cass_value_type_t
cass_value_secondary_sub_type(const cass_value_t* collection);


/***********************************************************************************
 *
 * UUID
 *
 ************************************************************************************/

/**
 * Generate a new V1 (time) UUID
 *
 * @param output
 */
CASS_EXPORT void
cass_uuid_v1(cass_uuid_t* output);

/**
 * Generate a new V1 (time) UUID for the specified time
 *
 * @param output
 */
CASS_EXPORT void
cass_uuid_v1_for_time(cass_uint64_t time,
                      cass_uuid_t* output);

/**
 * Generate a new V4 (random) UUID
 *
 * @param output
 *
 * @return
 */
CASS_EXPORT void
cass_uuid_v4(cass_uuid_t* output);

/**
 * Return the corresponding null terminated string for the specified UUID.
 *
 * @param uuid
 * @param output char[37]
 *
 * @return
 */
CASS_EXPORT cass_code_t
cass_uuid_string(cass_uuid_t uuid,
                 char* output);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* __CASS_H_INCLUDED__ */
