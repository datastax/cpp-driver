/*
  Copyright (c) 2014-2016 DataStax

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

#ifndef __DSE_H_INCLUDED__
#define __DSE_H_INCLUDED__

#include <cassandra.h>

#if !defined(DSE_STATIC)
#  if (defined(WIN32) || defined(_WIN32))
#    if defined(DSE_BUILDING)
#      define DSE_EXPORT __declspec(dllexport)
#    else
#      define DSE_EXPORT __declspec(dllexport)
#    endif
#  elif (defined(__SUNPRO_C)  || defined(__SUNPRO_CC)) && !defined(DSE_STATIC)
#    define DSE_EXPORT __global
#  elif (defined(__GNUC__) && __GNUC__ >= 4) || defined(__INTEL_COMPILER)
#    define DSE_EXPORT __attribute__ ((visibility("default")))
#  endif
#else
#define DSE_EXPORT
#endif

#if defined(_MSC_VER)
#  define DSE_DEPRECATED(func) __declspec(deprecated) func
#elif defined(__GNUC__) || defined(__INTEL_COMPILER)
#  define DSE_DEPRECATED(func) func __attribute__((deprecated))
#else
#  define DSE_DEPRECATED(func) func
#endif

/**
 * @file include/dse.h
 *
 * C/C++ Driver Extensions for DataStax Enterprise
 */

#define DSE_VERSION_MAJOR 1
#define DSE_VERSION_MINOR 0
#define DSE_VERSION_PATCH 0
#define DSE_VERSION_SUFFIX "alpha"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct DseGraphOptions_ DseGraphOptions;
typedef struct DseGraphStatement_ DseGraphStatement;
typedef struct DseGraphObject_ DseGraphObject;
typedef struct DseGraphArray_ DseGraphArray;
typedef struct DseGraphResultSet_ DseGraphResultSet;

typedef enum DseGraphResultType_ {
  DSE_GRAPH_RESULT_TYPE_NULL,
  DSE_GRAPH_RESULT_TYPE_BOOL,
  DSE_GRAPH_RESULT_TYPE_NUMBER,
  DSE_GRAPH_RESULT_TYPE_STRING,
  DSE_GRAPH_RESULT_TYPE_OBJECT,
  DSE_GRAPH_RESULT_TYPE_ARRAY
} DseGraphResultType;

typedef struct DseGraphResult_ DseGraphResult;

typedef struct DseGraphEdgeResult_ {
  const DseGraphResult* id;
  const DseGraphResult* label;
  const DseGraphResult* type;
  const DseGraphResult* properties;
  const DseGraphResult* in_vertex;
  const DseGraphResult* in_vertex_label;
  const DseGraphResult* out_vertex;
  const DseGraphResult* out_vertex_label;
} DseGraphEdgeResult;

typedef struct DseGraphVertexResult_ {
  const DseGraphResult* id;
  const DseGraphResult* label;
  const DseGraphResult* type;
  const DseGraphResult* properties;
} DseGraphVertexResult;

typedef struct DseGraphPathResult_ {
  const DseGraphResult* labels;
  const DseGraphResult* objects;
} DseGraphPathResult;

/***********************************************************************************
 *
 * Cluster
 *
 ***********************************************************************************/

/**
 *
 */
DSE_EXPORT CassError
cass_cluster_set_dse_gssapi_authenticator(CassCluster* cluster,
                                          const char* service,
                                          const char* principal);

/**
 *
 */
DSE_EXPORT CassError
cass_cluster_set_dse_gssapi_authenticator_n(CassCluster* cluster,
                                            const char* service,
                                            size_t service_length,
                                            const char* principal,
                                            size_t principal_length);

/**
 *
 */
DSE_EXPORT CassError
cass_cluster_set_dse_plaintext_authenticator(CassCluster* cluster,
                                             const char* username,
                                             const char* password);

/**
 *
 */
DSE_EXPORT CassError
cass_cluster_set_dse_plaintext_authenticator_n(CassCluster* cluster,
                                               const char* username,
                                               size_t username_length,
                                               const char* password,
                                               size_t password_length);

/***********************************************************************************
 *
 * Session
 *
 ***********************************************************************************/

DSE_EXPORT CassFuture*
cass_session_execute_dse_graph(CassSession* session,
                               const DseGraphStatement* statement);

/***********************************************************************************
 *
 * Future
 *
 ***********************************************************************************/

DSE_EXPORT DseGraphResultSet*
cass_future_get_dse_graph_resultset(CassFuture* future);

/***********************************************************************************
 *
 * Graph Options
 *
 ***********************************************************************************/

DSE_EXPORT DseGraphOptions*
dse_graph_options_new();

DSE_EXPORT void
dse_graph_options_free(DseGraphOptions* options);

DSE_EXPORT CassError
dse_graph_options_set_graph_language(DseGraphOptions* options,
                                     const char* language);

DSE_EXPORT CassError
dse_graph_options_set_graph_language_n(DseGraphOptions* options,
                                       const char* language, size_t language_length);

DSE_EXPORT CassError
dse_graph_options_set_graph_source(DseGraphOptions* options,
                                   const char* source);

DSE_EXPORT CassError
dse_graph_options_set_graph_source_n(DseGraphOptions* options,
                                     const char* source, size_t source_length);

DSE_EXPORT CassError
dse_graph_options_set_graph_name(DseGraphOptions* options,
                                 const char* name);

DSE_EXPORT CassError
dse_graph_options_set_graph_name_n(DseGraphOptions* options,
                                   const char* name, size_t name_length);

/***********************************************************************************
 *
 * Graph Statement
 *
 ***********************************************************************************/

DSE_EXPORT DseGraphStatement*
dse_graph_statement_new(const char* query,
                        const DseGraphOptions* options);

DSE_EXPORT DseGraphStatement*
dse_graph_statement_new_n(const char* query,
                          size_t query_length,
                          const DseGraphOptions* options);

DSE_EXPORT void
dse_graph_statement_free(DseGraphStatement* statement);

DSE_EXPORT CassError
dse_graph_statement_set_options(DseGraphStatement* statement,
                                const DseGraphOptions* options);

DSE_EXPORT CassError
dse_graph_statement_set_parameters(DseGraphStatement* statement,
                                   const DseGraphObject* parameters);

/***********************************************************************************
 *
 * Graph Object
 *
 ***********************************************************************************/

DSE_EXPORT DseGraphObject*
dse_graph_object_new();

DSE_EXPORT void
dse_graph_object_free(DseGraphObject* object);

DSE_EXPORT void
dse_graph_object_reset(DseGraphObject* object);

DSE_EXPORT void
dse_graph_object_finish(DseGraphObject* object);

DSE_EXPORT CassError
dse_graph_object_add_null(DseGraphObject* object,
                          const char* name);

DSE_EXPORT CassError
dse_graph_object_add_null_n(DseGraphObject* object,
                            const char* name,
                            size_t name_length);

DSE_EXPORT CassError
dse_graph_object_add_bool(DseGraphObject* object,
                          const char* name,
                          cass_bool_t value);

DSE_EXPORT CassError
dse_graph_object_add_bool_n(DseGraphObject* object,
                            const char* name,
                            size_t name_length,
                            cass_bool_t value);

DSE_EXPORT CassError
dse_graph_object_add_int32(DseGraphObject* object,
                           const char* name,
                           cass_int32_t value);

DSE_EXPORT CassError
dse_graph_object_add_int32_n(DseGraphObject* object,
                             const char* name,
                             size_t name_length,
                             cass_int32_t value);

DSE_EXPORT CassError
dse_graph_object_add_int64(DseGraphObject* object,
                           const char* name,
                           cass_int64_t value);

DSE_EXPORT CassError
dse_graph_object_add_int64_n(DseGraphObject* object,
                             const char* name,
                             size_t name_length,
                             cass_int64_t value);

DSE_EXPORT CassError
dse_graph_object_add_double(DseGraphObject* object,
                            const char* name,
                            cass_double_t value);

DSE_EXPORT CassError
dse_graph_object_add_double_n(DseGraphObject* object,
                              const char* name,
                              size_t name_length,
                              cass_double_t value);

DSE_EXPORT CassError
dse_graph_object_add_string(DseGraphObject* object,
                            const char* name,
                            const char* value);

DSE_EXPORT CassError
dse_graph_object_add_string_n(DseGraphObject* object,
                              const char* name,
                              size_t name_length,
                              const char* value,
                              size_t value_length);

DSE_EXPORT CassError
dse_graph_object_add_object(DseGraphObject* object,
                            const char* name,
                            const DseGraphObject* value);

DSE_EXPORT CassError
dse_graph_object_add_object_n(DseGraphObject* object,
                              const char* name,
                              size_t name_length,
                              const DseGraphObject* value);

DSE_EXPORT CassError
dse_graph_object_add_array(DseGraphObject* object,
                           const char* name,
                           const DseGraphArray* value);

DSE_EXPORT CassError
dse_graph_object_add_array_n(DseGraphObject* object,
                             const char* name,
                             size_t name_length,
                             const DseGraphArray* value);

/***********************************************************************************
 *
 * Graph Array
 *
 ***********************************************************************************/

DSE_EXPORT DseGraphArray*
dse_graph_array_new();

DSE_EXPORT void
dse_graph_array_free(DseGraphArray* array);

DSE_EXPORT void
dse_graph_array_reset(DseGraphObject* array);

DSE_EXPORT void
dse_graph_array_finish(DseGraphObject* array);

DSE_EXPORT CassError
dse_graph_array_add_null(DseGraphArray* array);

DSE_EXPORT CassError
dse_graph_array_add_bool(DseGraphArray* array,
                          cass_bool_t value);

DSE_EXPORT CassError
dse_graph_array_add_int32(DseGraphArray* array,
                           cass_int32_t value);

DSE_EXPORT CassError
dse_graph_array_add_int64(DseGraphArray* array,
                           cass_int64_t value);

DSE_EXPORT CassError
dse_graph_array_add_double(DseGraphArray* array,
                            cass_double_t value);

DSE_EXPORT CassError
dse_graph_array_add_string(DseGraphArray* array,
                            const char* value);

DSE_EXPORT CassError
dse_graph_array_add_string_n(DseGraphArray* array,
                            const char* value,
                             size_t value_length);

DSE_EXPORT CassError
dse_graph_array_add_object(DseGraphArray* array,
                           const DseGraphObject* value);

DSE_EXPORT CassError
dse_graph_array_add_array(DseGraphArray* array,
                          const DseGraphArray* value);

/***********************************************************************************
 *
 * Graph Result Set
 *
 ***********************************************************************************/

DSE_EXPORT void
dse_graph_resultset_free(DseGraphResultSet* resultset);

DSE_EXPORT size_t
dse_graph_resultset_count(DseGraphResultSet* resultset);

DSE_EXPORT const DseGraphResult*
dse_graph_resultset_next(DseGraphResultSet* resultset);

/***********************************************************************************
 *
 * Graph Result
 *
 ***********************************************************************************/

DSE_EXPORT DseGraphResultType
dse_graph_result_type(const DseGraphResult* result);

DSE_EXPORT cass_bool_t
dse_graph_result_is_bool(const DseGraphResult* result);

DSE_EXPORT cass_bool_t
dse_graph_result_is_int32(const DseGraphResult* result);

DSE_EXPORT cass_bool_t
dse_graph_result_is_int64(const DseGraphResult* result);

DSE_EXPORT cass_bool_t
dse_graph_result_is_double(const DseGraphResult* result);

DSE_EXPORT cass_bool_t
dse_graph_result_is_string(const DseGraphResult* result);

DSE_EXPORT cass_bool_t
dse_graph_result_is_object(const DseGraphResult* result);

DSE_EXPORT cass_bool_t
dse_graph_result_is_array(const DseGraphResult* result);

DSE_EXPORT cass_bool_t
dse_graph_result_get_bool(const DseGraphResult* result);

DSE_EXPORT cass_int32_t
dse_graph_result_get_int32(const DseGraphResult* result);

DSE_EXPORT cass_int64_t
dse_graph_result_get_int64(const DseGraphResult* result);

DSE_EXPORT cass_double_t
dse_graph_result_get_double(const DseGraphResult* result);

DSE_EXPORT const char*
dse_graph_result_get_string(const DseGraphResult* result,
                            size_t* length);

DSE_EXPORT CassError
dse_graph_result_as_edge(const DseGraphResult* result,
                         DseGraphEdgeResult* edge);

DSE_EXPORT CassError
dse_graph_result_as_vertex(const DseGraphResult* result,
                           DseGraphVertexResult* vertex);

DSE_EXPORT CassError
dse_graph_result_as_path(const DseGraphResult* result,
                         DseGraphPathResult* path);

DSE_EXPORT size_t
dse_graph_result_member_count(const DseGraphResult* result);

DSE_EXPORT const char*
dse_graph_result_member_key(const DseGraphResult* result,
                            size_t index,
                            size_t* length);

DSE_EXPORT const DseGraphResult*
dse_graph_result_member_value(const DseGraphResult* result,
                              size_t index);

DSE_EXPORT size_t
dse_graph_result_element_count(const DseGraphResult* result);

DSE_EXPORT const DseGraphResult*
dse_graph_result_element(const DseGraphResult* result,
                         size_t index);

/***********************************************************************************
 *
 * GSSAPI Authentication
 *
 ***********************************************************************************/

/**
 *
 */
typedef void (*DseGssapiAuthenticatorLockCallback)(void* data);

/**
 *
 */
typedef void (*DseGssapiAuthenticatorUnockCallback)(void* data);

/**
 *
 */
DSE_EXPORT CassError
dse_gssapi_authenticator_set_lock_callbacks(DseGssapiAuthenticatorLockCallback lock_callback,
                                            DseGssapiAuthenticatorUnockCallback unlock_callback,
                                            void* data);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* __DSE_H_INCLUDED__ */
