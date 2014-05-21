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

/**
 * @file include/cassandra.h
 *
 * TBD.
 */


/* TODO(mpenick) handle primitive types for other compilers and platforms */

#define cass_false 0
#define cass_true  1

typedef int cass_bool_t;
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

typedef cass_uint8_t CassUuid[16];

typedef struct CassInet_ {
  cass_uint8_t address[16];
  cass_uint8_t address_length;
} CassInet;

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

typedef struct CassBytes_ {
    const cass_byte_t* data;
    cass_size_t size;
} CassBytes;

typedef struct CassString_ {
    const char* data;
    cass_size_t length;
} CassString;

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

typedef enum CassOption_ {
  CASS_OPTION_THREADS_IO                 = 1,
  CASS_OPTION_THREADS_CALLBACK           = 2,
  CASS_OPTION_CONTACT_POINT_ADD          = 3,
  CASS_OPTION_PORT                       = 4,
  CASS_OPTION_CQL_VERSION                = 5,
  CASS_OPTION_SCHEMA_AGREEMENT_WAIT      = 6,
  CASS_OPTION_CONTROL_CONNECTION_TIMEOUT = 7,
  CASS_OPTION_COMPRESSION                = 9
} CassOption;

typedef enum CassCompression_ {
  CASS_COMPRESSION_NONE   = 0,
  CASS_COMPRESSION_SNAPPY = 1,
  CASS_COMPRESSION_LZ4    = 2
} CassCompression;

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
} CassError;

typedef enum  {
  CASS_ERROR_SOURCE_NONE        = 0,
  CASS_ERROR_SOURCE_OS          = 1,
  CASS_ERROR_SOURCE_NETWORK     = 2,
  CASS_ERROR_SOURCE_SSL         = 3,
  CASS_ERROR_SOURCE_COMPRESSION = 4,
  CASS_ERROR_SOURCE_SERVER      = 5,
  CASS_ERROR_SOURCE_LIBRARY     = 6
} CassErrorSource;

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
CASS_EXPORT CassSession*
cass_session_new();

/**
 * Initialize a new session using previous session's configuration.
 * Instance must be freed by caller.
 *
 * @return
 */
CASS_EXPORT CassSession*
cass_session_clone(const CassSession* session);

/**
 * Free a session instance.
 *
 * @return
 */
CASS_EXPORT void
cass_session_free(CassSession* session);

/**
 * Set an option on the specified session.
 *
 * @param session
 * @param option
 * @param data
 * @param data_length
 *
 * @return 0 if successful, otherwise an error occurred
 */
CASS_EXPORT CassError
cass_session_setopt(CassSession* session,
                    CassOption option,
                    const void* data, 
                    cass_size_t data_length);

/**
 * Get the option value for the specified session.
 *
 * @param session
 * @param option
 * @param data
 * @param data_length
 *
 * @return 0 if successful, otherwise an error occurred
 */
CASS_EXPORT CassError
cass_session_getopt(const CassSession* session,
                    CassOption option,
                    void** data, 
                    cass_size_t* data_length);

/**
 * Shutdown the session instance, output a shutdown future which can
 * be used to determine when the session has been terminated.
 *
 * @param session
 */
CASS_EXPORT CassFuture*
cass_session_shutdown(CassSession* session);

/**
 * Initiate a session using the specified session. Resulting
 * future must be freed by caller.
 *
 * @param session
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassFuture*
cass_session_connect(CassSession* session);

/**
 * Initiate a session using the specified session, and set the keyspace. Resulting
 * future must be freed by caller.
 *
 * @param session
 * @param keyspace
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassFuture*
cass_session_connect_keyspace(CassSession* session,
                              const char* keyspace);

/**
 * Create a prepared statement. Future must be freed by caller.
 *
 * @param session
 * @param statement string
 *
 * @return
 */
CASS_EXPORT CassFuture*
cass_session_prepare(CassSession* session,
                     CassString statement);

/**
 * Execute a query, bound statement and obtain a future. Future must be freed by
 * caller.
 *
 * @param session
 * @param statement
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassFuture*
cass_session_exec(CassSession* session,
                  CassStatement* statement);

/**
 * Execute a batch statement and obtain a future. Future must be freed by
 * caller.
 *
 * @param session
 * @param batch
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassFuture*
cass_session_exec_batch(CassSession* session,
                        CassBatch* batch);

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
cass_future_free(CassFuture* future);

/**
 * Is the specified future ready
 *
 * @param future
 *
 * @return true if ready
 */
CASS_EXPORT int
cass_future_ready(CassFuture* future);

/**
 * Wait the linked event occurs or error condition is reached
 *
 * @param future
 */
CASS_EXPORT void
cass_future_wait(CassFuture* future);

/**
 * Wait the linked event occurs, error condition is reached, or time has elapsed.
 *
 * @param future
 * @param wait time in microseconds
 *
 * @return false if returned due to timeout
 */
CASS_EXPORT cass_bool_t
cass_future_wait_timed(CassFuture* future,
                       cass_duration_t wait);

/**
 * If the linked event was successful get the session instance
 *
 * @param future
 *
 * @return NULL if unsuccessful, otherwise pointer to CassSession instance
 */
CASS_EXPORT CassSession*
cass_future_get_session(CassFuture* future);

/**
 * If the linked event was successful get the result instance. Result
 * instance must be freed by caller. May only be called once.
 *
 * @param future
 *
 * @return NULL if unsuccessful, otherwise pointer to CassResult instance
 */
CASS_EXPORT const CassResult*
cass_future_get_result(CassFuture* future);

/**
 * If the linked event was successful get the prepared
 * instance. Prepared instance must be freed by caller. May only be
 * called once
 *
 * @param future
 *
 * @return NULL if unsuccessful, otherwise pointer to CassPrepare instance
 */
CASS_EXPORT const CassPrepared*
cass_future_get_prepared(CassFuture* future);


CASS_EXPORT CassString
cass_future_error_message(CassFuture* future);

/**
 * Obtain the code from an error structure.
 *
 * @param future
 *
 * @return error source
 *
 */
CASS_EXPORT CassErrorSource
cass_future_error_source(CassFuture* future);

/**
 * Obtain the source from an error structure.
 *
 * @param future
 *
 * @return source code
 *
 */
CASS_EXPORT CassError
cass_future_error_code(CassFuture* future);

/***********************************************************************************
 *
 * Statement
 *
 ***********************************************************************************/

/**
 * Initialize an ad-hoc query statement
 *
 * @param statement string
 * @param parameter_count number of bound paramerters
 * @param consistency statement read/write consistency
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassStatement*
cass_statement_new(CassString statement,
                   cass_size_t parameter_count,
                   CassConsistency consistency);


CASS_EXPORT void
cass_statement_free(CassStatement* statement);

/**
 * Bind an int to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return
 */
CASS_EXPORT CassError
cass_statement_bind_int32(CassStatement* statement,
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
CASS_EXPORT CassError
cass_statement_bind_int64(CassStatement* statement,
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
CASS_EXPORT CassError
cass_statement_bind_float(CassStatement* statement,
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
CASS_EXPORT CassError
cass_statement_bind_double(CassStatement* statement,
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
CASS_EXPORT CassError
cass_statement_bind_bool(CassStatement* statement,
                         cass_size_t index,
                         cass_bool_t value);

/**
 * Bind bytes to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassError
cass_statement_bind_string(CassStatement* statement,
                           cass_size_t index,
                           CassString value);

/**
 * Bind bytes to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassError
cass_statement_bind_bytes(CassStatement* statement,
                           cass_size_t index,
                           CassBytes value);

/**
 * Bind a UUID to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassError
cass_statement_bind_uuid(CassStatement* statement,
                         cass_size_t index,
                         CassUuid value);


CASS_EXPORT CassError
cass_statement_bind_inet(CassStatement* statement,
                         cass_size_t index,
                         CassInet value);

/**
 * Bind a decimal to a query or bound statement at the specified index
 *
 * @param statement
 * @param index
 * @param scale
 * @param varint
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassError
cass_statement_bind_decimal(CassStatement* statement,
                            cass_size_t index,
                            cass_uint32_t scale,
                            CassBytes varint);


CASS_EXPORT CassError
cass_statement_bind_custom(CassStatement* statement,
                           cass_size_t index,
                           cass_size_t size,
                           cass_byte_t** output);

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

CASS_EXPORT void
cass_prepared_free(const CassPrepared* prepared);

/**
 * Initialize a bound statement from a pre-prepared statement
 * parameters
 *
 * @param prepared the previously prepared statement
 * @param parameter_count number of bound paramerters
 * @param consistency statement read/write consistency
 *
 * @return 0 if successful, otherwise error occured
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


CASS_EXPORT CassBatch*
cass_batch_new(CassConsistency consistency);

CASS_EXPORT void
cass_batch_free(CassBatch* batch);

CASS_EXPORT CassError
cass_batch_add_statement(CassBatch* batch,
                         CassStatement* statement);

CASS_EXPORT void
cass_batch_set_timestamp(CassBatch* batch,
                         cass_int64_t timestamp);


/***********************************************************************************
 *
 * Collection
 *
 ***********************************************************************************/

CASS_EXPORT CassCollection*
cass_collection_new(cass_size_t item_count);

CASS_EXPORT void
cass_collection_free(CassCollection* collection);

CASS_EXPORT CassError
cass_collection_append_int32(CassCollection* collection,
                             cass_int32_t value);

CASS_EXPORT CassError
cass_collection_append_int64(CassCollection* collection,
                              cass_int64_t value);

CASS_EXPORT CassError
cass_collection_append_float(CassCollection* collection,
                             cass_float_t value);

CASS_EXPORT CassError
cass_collection_append_double(CassCollection* collection,
                              cass_double_t value);

CASS_EXPORT CassError
cass_collection_append_bool(CassCollection* collection,
                            cass_bool_t value);

CASS_EXPORT CassError
cass_collection_append_string(CassCollection* collection,
                              CassString value);

CASS_EXPORT CassError
cass_collection_append_bytes(CassCollection* collection,
                             CassBytes value);

CASS_EXPORT CassError
cass_collection_append_uuid(CassCollection* collection,
                            CassUuid value);

CASS_EXPORT CassError
cass_collection_append_inet(CassCollection* collection,
                            CassInet value);

CASS_EXPORT CassError
cass_collection_append_decimal(CassCollection* collection,
                               cass_int32_t scale,
                               CassBytes varint);

/***********************************************************************************
 *
 * Result
 *
 ***********************************************************************************/

CASS_EXPORT void
cass_result_free(const CassResult* result);

/**
 * Get number of rows for the specified result
 *
 * @param result
 *
 * @return
 */
CASS_EXPORT cass_size_t
cass_result_row_count(const CassResult* result);

/**
 * Get number of columns per row for the specified result
 *
 * @param result
 *
 * @return
 */
CASS_EXPORT cass_size_t
cass_result_column_count(const CassResult* result);

/**
 * Get the type for the column at index for the specified result
 *
 * @param result
 * @param index
 *
 * @return
 */
CASS_EXPORT CassValueType
cass_result_column_type(const CassResult* result,
                        cass_size_t index);

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
CASS_EXPORT CassIterator*
cass_iterator_from_result(const CassResult* result);

/**
 * Create new iterator for the specified row.
 *
 * @param row
 *
 * @return
 */
CASS_EXPORT CassIterator*
cass_iterator_from_row(const CassRow* row);

/**
 * Create new iterator for the specified collection.
 *
 * @param value
 *
 * @return
 */
CASS_EXPORT CassIterator*
cass_iterator_from_collection(const CassValue* value);

CASS_EXPORT void
cass_iterator_free(CassIterator* iterator);

/**
 * Advance the iterator to the next row or collection item.
 *
 * @param iterator
 *
 * @return next row, NULL if no rows remain or error
 */
CASS_EXPORT cass_bool_t
cass_iterator_next(CassIterator* iterator);

CASS_EXPORT const CassRow*
cass_iterator_get_row(CassIterator* iterator);

CASS_EXPORT const CassValue*
cass_iterator_get_column(CassIterator *iterator);

CASS_EXPORT const CassValue*
cass_iterator_get_value(CassIterator *iterator);


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
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT const CassValue*
cass_row_get_column(const CassRow*  row,
                    cass_size_t index);

/***********************************************************************************
 *
 * Value
 *
 ***********************************************************************************/

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param value
 * @param output
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassError
cass_value_get_int32(const CassValue* value,
                     cass_int32_t* output);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param value
 * @param output
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassError
cass_value_get_int64(const CassValue* value,
                     cass_int64_t* output);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param value
 * @param output
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassError
cass_value_get_float(const CassValue* value,
                     cass_float_t*  output);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param value
 * @param output
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassError
cass_value_get_double(const CassValue* value,
                      cass_double_t* output);

CASS_EXPORT CassError
cass_value_get_bool(const CassValue* value,
                    cass_bool_t* output);

CASS_EXPORT CassError
cass_value_get_uuid(const CassValue* value,
                    CassUuid output);


CASS_EXPORT CassError
cass_value_get_string(const CassValue* value,
                      CassString* output);

CASS_EXPORT CassError
cass_value_get_bytes(const CassValue* value,
                     CassBytes* output);

/**
 * Decode the specified value. Value may be a column, collection item, map key, or map
 * value.
 *
 * @param value
 * @param output_scale
 * @param output_varint
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassError
cass_value_get_decimal(const CassValue* value,
                       cass_uint32_t* output_scale,
                       CassBytes* output_varint);

/**
 * Get the collection sub-type. Works for collections that have a single sub-type
 * (lists and sets).
 *
 * @param value
 *
 * @return 0 if successful, otherwise error occured
 */
CASS_EXPORT CassValueType
cass_value_type(const CassValue* value);

CASS_EXPORT cass_bool_t
cass_value_is_collection(const CassValue* value);

/**
 * Get the number of items in a collection. Works for all collection types.
 *
 * @param collection column
 *
 * @return size, 0 if error
 */
CASS_EXPORT cass_size_t
cass_value_item_count(const CassValue* collection);

CASS_EXPORT CassValueType
cass_value_primary_sub_type(const CassValue* collection);

CASS_EXPORT CassValueType
cass_value_secondary_sub_type(const CassValue* collection);


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
cass_uuid_v1(CassUuid* output);

/**
 * Generate a new V1 (time) UUID for the specified time
 *
 * @param time
 * @param output
 */
CASS_EXPORT void
cass_uuid_v1_for_time(cass_uint64_t time,
                      CassUuid* output);

/**
 * Generate a new V4 (random) UUID
 *
 * @param output
 *
 * @return
 */
CASS_EXPORT void
cass_uuid_v4(CassUuid* output);

/**
 * Return the corresponding null terminated string for the specified UUID.
 *
 * @param uuid
 * @param output char[37]
 *
 * @return
 */
CASS_EXPORT CassError
cass_uuid_string(CassUuid uuid,
                 char* output);

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
cass_code_error_desc(CassError code);

/***********************************************************************************
 *
 * Bytes/String
 *
 ************************************************************************************/

CASS_EXPORT CassBytes
cass_bytes_init(cass_byte_t* data, cass_size_t size);

CASS_EXPORT CassString
cass_string_init(const char* null_terminated);

CASS_EXPORT CassString
cass_string_init2(const char* data, cass_size_t length);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* __CASS_H_INCLUDED__ */
