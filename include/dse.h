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

#define DSE_POINT_TYPE       "org.apache.cassandra.db.marshal.PointType"
#define DSE_CIRCLE_TYPE      "org.apache.cassandra.db.marshal.CircleType"
#define DSE_LINE_STRING_TYPE "org.apache.cassandra.db.marshal.LineStringType"
#define DSE_POLYGON_TYPE     "org.apache.cassandra.db.marshal.PolygonType"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @struct DseLineString
 */
typedef struct DseLineString_ DseLineString;

/**
 * @struct DseLineStringIterator
 */
typedef struct DseLineStringIterator_ DseLineStringIterator;

/**
 * @struct DsePolygon
 */
typedef struct DsePolygon_ DsePolygon;

/**
 * @struct DsePolygonIterator
 */
typedef struct DsePolygonIterator_ DsePolygonIterator;

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
 * Statement
 *
 ***********************************************************************************/

/**
 * Binds point to a query or bound statement at the specified index.
 *
 * @public @memberof CassStatement
 *
 * @param[in] statement
 * @param[in] index
 * @param[in] x
 * @param[in] y
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_statement_bind_dse_point(CassStatement* statement,
                              size_t index,
                              cass_double_t x, cass_double_t y);

/**
 * Binds a point to all the values with the specified name.
 *
 * This can only be used with statements created by
 * cass_prepared_bind() when using Cassandra 2.0 or earlier.
 *
 * @public @memberof CassStatement
 *
 * @param[in] statement
 * @param[in] name
 * @param[in] x
 * @param[in] y
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_statement_bind_dse_point_by_name(CassStatement* statement,
                                      const char* name,
                                      cass_double_t x, cass_double_t y);

/**
 * Same as cass_statement_bind_dse_point_by_name(), but with lengths for string
 * parameters.
 *
 * @public @memberof CassStatement
 *
 * @param[in] statement
 * @param[in] name
 * @param[in] name_length
 * @param[in] x
 * @param[in] y
 * @return same as cass_statement_bind_dse_point_by_name()
 *
 * @see cass_statement_bind_dse_point_by_name()
 */
DSE_EXPORT CassError
cass_statement_bind_dse_point_by_name_n(CassStatement* statement,
                                        const char* name, size_t name_length,
                                        cass_double_t x, cass_double_t y);

/**
 * Binds circle to a query or bound statement at the specified index.
 *
 * @public @memberof CassStatement
 *
 * @param[in] statement
 * @param[in] index
 * @param[in] x
 * @param[in] y
 * @param[in] radius
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_statement_bind_dse_circle(CassStatement* statement,
                               size_t index,
                               cass_double_t x, cass_double_t y,
                               cass_double_t radius);

/**
 * Binds a circle to all the values with the specified name.
 *
 * This can only be used with statements created by
 * cass_prepared_bind() when using Cassandra 2.0 or earlier.
 *
 * @public @memberof CassStatement
 *
 * @param[in] statement
 * @param[in] name
 * @param[in] x
 * @param[in] y
 * @param[in] radius
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_statement_bind_dse_circle_by_name(CassStatement* statement,
                                       const char* name,
                                       cass_double_t x, cass_double_t y,
                                       cass_double_t radius);

/**
 * Same as cass_statement_bind_dse_circle_by_name(), but with lengths for string
 * parameters.
 *
 * @public @memberof CassStatement
 *
 * @param[in] statement
 * @param[in] name
 * @param[in] name_length
 * @param[in] x
 * @param[in] y
 * @param[in] radius
 * @return same as cass_statement_bind_dse_circle_by_name()
 *
 * @see cass_statement_bind_dse_circle_by_name()
 */
DSE_EXPORT CassError
cass_statement_bind_dse_circle_by_name_n(CassStatement* statement,
                                         const char* name, size_t name_length,
                                         cass_double_t x, cass_double_t y,
                                         cass_double_t radius);

/**
 * Binds line string to a query or bound statement at the specified index.
 *
 * @public @memberof CassStatement
 *
 * @param[in] statement
 * @param[in] index
 * @param[in] line_string
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_statement_bind_dse_line_string(CassStatement* statement,
                                    size_t index,
                                    const DseLineString* line_string);

/**
 * Binds a line string to all the values with the specified name.
 *
 * This can only be used with statements created by
 * cass_prepared_bind() when using Cassandra 2.0 or earlier.
 *
 * @public @memberof CassStatement
 *
 * @param[in] statement
 * @param[in] name
 * @param[in] line_string
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_statement_bind_dse_line_string_by_name(CassStatement* statement,
                                            const char* name,
                                            const DseLineString* line_string);

/**
 * Same as cass_statement_bind_dse_line_string_by_name(), but with lengths for string
 * parameters.
 *
 * @public @memberof CassStatement
 *
 * @param[in] statement
 * @param[in] name
 * @param[in] name_length
 * @param[in] line_string
 * @return same as cass_statement_bind_dse_line_string_by_name()
 *
 * @see cass_statement_bind_dse_line_string_by_name()
 */
DSE_EXPORT CassError
cass_statement_bind_dse_line_string_by_name_n(CassStatement* statement,
                                              const char* name, size_t name_length,
                                              const DseLineString* line_string);

/**
 * Binds polygon to a query or bound statement at the specified index.
 *
 * @public @memberof CassStatement
 *
 * @param[in] statement
 * @param[in] index
 * @param[in] polygon
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_statement_bind_dse_polygon(CassStatement* statement,
                                size_t index,
                                const DsePolygon* polygon);

/**
 * Binds a polygon to all the values with the specified name.
 *
 * This can only be used with statements created by
 * cass_prepared_bind() when using Cassandra 2.0 or earlier.
 *
 * @public @memberof CassStatement
 *
 * @param[in] statement
 * @param[in] name
 * @param[in] polygon
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_statement_bind_dse_polygon_by_name(CassStatement* statement,
                                        const char* name,
                                        const DsePolygon* polygon);

/**
 * Same as cass_statement_bind_dse_polygon_by_name(), but with lengths for string
 * parameters.
 *
 * @public @memberof CassStatement
 *
 * @param[in] statement
 * @param[in] name
 * @param[in] name_length
 * @param[in] polygon
 * @return same as cass_statement_bind_dse_polygon_by_name()
 *
 * @see cass_statement_bind_dse_polygon_by_name()
 */
DSE_EXPORT CassError
cass_statement_bind_dse_polygon_by_name_n(CassStatement* statement,
                                          const char* name, size_t name_length,
                                          const DsePolygon* polygon);

/***********************************************************************************
 *
 * Value
 *
 ***********************************************************************************/

/**
 * Gets a point for the specified value.
 *
 * @public @memberof CassValue
 *
 * @param[in] value
 * @param[out] x
 * @param[out] y
 * @return CASS_OK if successful, otherwise error occurred
 */
DSE_EXPORT CassError
cass_value_get_dse_point(const CassValue* value,
                         cass_double_t* x, cass_double_t* y);

/**
 * Gets a circle for the specified value.
 *
 * @public @memberof CassValue
 *
 * @param[in] value
 * @param[out] x
 * @param[out] y
 * @param[out] radius
 * @return CASS_OK if successful, otherwise error occurred
 */
DSE_EXPORT CassError
cass_value_get_dse_circle(const CassValue* value,
                          cass_double_t* x, cass_double_t* y,
                          cass_double_t* radius);

/***********************************************************************************
 *
 * Line String
 *
 ***********************************************************************************/

/**
 * Creates a new line string.
 *
 * @public @memberof DseLineString
 *
 * @return Returns a line string that must be freed.
 *
 * @see dse_line_string_free()
 */
DSE_EXPORT DseLineString*
dse_line_string_new();

/**
 * Frees a line string instance.
 *
 * @public @memberof DseLineString
 *
 * @param[in] line_string
 */
DSE_EXPORT void
dse_line_string_free(DseLineString* line_string);

/**
 * Resets a line string so that it can be reused.
 *
 * @public @memberof DseLineString
 *
 * @param[in] line_string
 */
DSE_EXPORT void
dse_line_string_reset(DseLineString* line_string);

/**
 * Reserves enough memory to contain the provide number of points.
 *
 * @public @memberof DseLineString
 *
 * @param[in] line_string
 * @param[in] num_points
 */
DSE_EXPORT void
dse_line_string_reserve(DseLineString* line_string,
                        cass_uint32_t num_points);

/**
 * Adds a point to the line string.
 *
 * @public @memberof DseLineString
 *
 * @param[in] line_string
 * @param[in] x
 * @param[in] y
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_line_string_add_point(DseLineString* line_string,
                          cass_double_t x, cass_double_t y);

/**
 * Finishes the contruction of a line string.
 *
 * @public @memberof DseLineString
 *
 * @param[in] line_string
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_line_string_finish(DseLineString* line_string);

/***********************************************************************************
 *
 * Line String Iterator
 *
 ***********************************************************************************/

/**
 * Creates a new line string iterator.
 *
 * @public @memberof DseLineStringIterator
 *
 * @return Returns an iterator that must be freed.
 *
 * @see dse_line_string_iterator_free()
 */
DSE_EXPORT DseLineStringIterator*
dse_line_string_iterator_new();

/**
 * Frees a line string iterator instance.
 *
 * @public @memberof DseLineStringIterator
 *
 * @param[in] iterator
 */
DSE_EXPORT void
dse_line_string_iterator_free(DseLineStringIterator* iterator);

/**
 * Resets a line string iterator so that it can be reused.
 *
 * @public @memberof DseLineStringIterator
 *
 * @param[in] iterator
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_line_string_iterator_reset(DseLineStringIterator* iterator,
                               const CassValue* value);

/**
 * Gets the number of points in the line string.
 *
 * @public @memberof DseLineStringIterator
 *
 * @param[in] iterator
 * @return The number of points in the line string.
 */
DSE_EXPORT cass_uint32_t
dse_line_string_iterator_num_points(const DseLineStringIterator* iterator);

/**
 * Gets the next point in the line string.
 *
 * @public @memberof DseLineStringIterator
 *
 * @param[in] iterator
 * @param[out] x
 * @param[out] y
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_line_string_iterator_next_point(DseLineStringIterator* iterator,
                                    cass_double_t* x, cass_double_t* y);

/***********************************************************************************
 *
 * Polygon
 *
 ***********************************************************************************/

/**
 * Creates a new polygon iterator.
 *
 * @public @memberof DsePolygon
 *
 * @return Returns an polygon that must be freed.
 *
 * @see dse_polygon_iterator_free()
 */
DSE_EXPORT DsePolygon*
dse_polygon_new();

/**
 * Frees a polygon instance.
 *
 * @public @memberof DsePolygon
 *
 * @param[in] polygon
 */
DSE_EXPORT void
dse_polygon_free(DsePolygon* polygon);

/**
 * Resets a polygon so that it can be reused.
 *
 * @public @memberof DsePolygon
 *
 * @param[in] polygon
 */
DSE_EXPORT void
dse_polygon_reset(DsePolygon* polygon);

/**
 * Reserves enough memory to contain the provided number rings and points.
 *
 * @public @memberof DsePolygon
 *
 * @param[in] polygon
 * @param[in] num_rings
 * @param[in] total_num_points
 */
DSE_EXPORT void
dse_polygon_reserve(DsePolygon* polygon,
                    cass_uint32_t num_rings,
                    cass_uint32_t total_num_points);

/**
 * Starts a new ring.
 *
 * <b>Note:</b> This will finish the previous ring.
 *
 * @public @memberof DsePolygon
 *
 * @param[in] polygon
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_polygon_start_ring(DsePolygon* polygon);

/**
 * Adds a point to the current ring.
 *
 * @public @memberof DsePolygon
 *
 * @param[in] polygon
 * @param[in] x
 * @param[in] y
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_polygon_add_point(DsePolygon* polygon,
                      cass_double_t x, cass_double_t y);

/**
 * Finishes the contruction of a polygon.
 *
 * @public @memberof DsePolygon
 *
 * @param[in] polygon
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_polygon_finish(DsePolygon* polygon);

/***********************************************************************************
 *
 * Polygon Iterator
 *
 ***********************************************************************************/

/**
 * Creates a new polygon iterator.
 *
 * @public @memberof DsePolygonIterator
 *
 * @return Returns an iterator that must be freed.
 *
 * @see dse_polygon_iterator_free()
 */
DSE_EXPORT DsePolygonIterator*
dse_polygon_iterator_new();

/**
 * Frees a polygon iterator instance.
 *
 * @public @memberof DsePolygonIterator
 *
 * @param[in] iterator
 */
DSE_EXPORT void
dse_polygon_iterator_free(DsePolygonIterator* iterator);

/**
 * Resets a polygon iterator so that it can be reused.
 *
 * @public @memberof DsePolygonIterator
 *
 * @param[in] iterator
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_polygon_iterator_reset(DsePolygonIterator* iterator,
                           const CassValue* value);

/**
 * Gets the number rings in the polygon.
 *
 * @public @memberof DsePolygonIterator
 *
 * @param[in] iterator
 * @return The number of rings in the polygon.
 */
DSE_EXPORT cass_uint32_t
dse_polygon_iterator_num_rings(const DsePolygonIterator* iterator);

/**
 * Gets the number of points for the current ring.
 *
 * @public @memberof DsePolygonIterator
 *
 * @param[in] iterator
 * @param[out] num_points
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_polygon_iterator_next_num_points(DsePolygonIterator* iterator,
                                     cass_uint32_t* num_points);

/**
 * Gets the next point in the current ring.
 *
 * @public @memberof DsePolygonIterator
 *
 * @param[in] iterator
 * @param[out] x
 * @param[out] y
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_polygon_iterator_next_point(DsePolygonIterator* iterator,
                                cass_double_t* x, cass_double_t* y);

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
