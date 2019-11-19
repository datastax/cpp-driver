/*
  Copyright (c) DataStax, Inc.

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

#define DSE_EXPORT CASS_EXPORT
#define DSE_DEPRECATED CASS_DEPRECATED

/**
 * @file include/dse.h
 *
 * C/C++ DataStax Enterprise Driver
 */

#define DSE_VERSION_MAJOR CASS_VERSION_MAJOR
#define DSE_VERSION_MINOR CASS_VERSION_MINOR
#define DSE_VERSION_PATCH CASS_VERSION_PATCH
#define DSE_VERSION_SUFFIX CASS_VERSION_SUFFIX

#ifdef __cplusplus
extern "C" {
#endif

/***********************************************************************************
 *
 * DateRange
 *
 ***********************************************************************************/

typedef enum DseDateRangePrecision_ {
  DSE_DATE_RANGE_PRECISION_UNBOUNDED = 0xFF,
  DSE_DATE_RANGE_PRECISION_YEAR = 0,
  DSE_DATE_RANGE_PRECISION_MONTH = 1,
  DSE_DATE_RANGE_PRECISION_DAY = 2,
  DSE_DATE_RANGE_PRECISION_HOUR = 3,
  DSE_DATE_RANGE_PRECISION_MINUTE = 4,
  DSE_DATE_RANGE_PRECISION_SECOND = 5,
  DSE_DATE_RANGE_PRECISION_MILLISECOND = 6
} DseDateRangePrecision;

/**
 * The lower bound, upper bound, or single value of a DseDateRange.
 *
 * @struct DseDateRangeBound
 */
typedef struct DseDateRangeBound_ {
  DseDateRangePrecision precision;
  cass_int64_t time_ms;
} DseDateRangeBound;

/**
 * A DateRange object in DSE.
 *
 * @struct DseDateRange
 */
typedef struct DseDateRange_ {
  cass_bool_t is_single_date;
  DseDateRangeBound lower_bound; /* Lower bound is also used for a single date */
  DseDateRangeBound upper_bound;
} DseDateRange;

/**
 * Creates a new DseDateRangeBound with the given attributes.
 *
 * @public @memberof DseDateRangeBound
 *
 * @param[in] precision
 * @param[in] time_ms
 *
 * @return A date range bound
 */
DSE_EXPORT DseDateRangeBound
dse_date_range_bound_init(DseDateRangePrecision precision, cass_int64_t time_ms);

/**
 * Creates a new DseDateRangeBound that represents an open bound.
 *
 * @public @memberof DseDateRangeBound
 *
 * @return A date range bound
 */
DSE_EXPORT DseDateRangeBound
dse_date_range_bound_unbounded();

/**
 * Checks if the given DseDateRangeBound is an unbound value.
 *
 * @public @memberof DseDateRangeBound
 *
 * @param[in] bound
 *
 * @return cass_true if the bound is actually unbounded.
 */
DSE_EXPORT cass_bool_t
dse_date_range_bound_is_unbounded(DseDateRangeBound bound);

/**
 * Initializes a DseDateRange with a lower and upper bound.
 *
 * @public @memberof DseDateRange
 *
 * @param[out] range
 * @param[in] lower_bound
 * @param[in] upper_bound
 *
 * @return Returns the date-range object
 */
DSE_EXPORT DseDateRange*
dse_date_range_init(DseDateRange* range,
                    DseDateRangeBound lower_bound,
                    DseDateRangeBound upper_bound);

/**
 * Initializes a DseDateRange with a single date
 *
 * @public @memberof DseDateRange
 *
 * @param[out] range
 * @param[in] date
 *
 * @return Returns the date-range object
 */
DSE_EXPORT DseDateRange*
dse_date_range_init_single_date(DseDateRange* range, DseDateRangeBound date);

/***********************************************************************************
 *
 * Geospatial Types
 *
 ***********************************************************************************/

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
 * Enables GSSAPI authentication for DSE clusters secured with the
 * `DseAuthenticator`.
 *
 * @public @memberof CassCluster
 *
 * @param[in] cluster
 * @param[in] service
 * @param[in] principal
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_cluster_set_dse_gssapi_authenticator(CassCluster* cluster,
                                          const char* service,
                                          const char* principal);

/**
 * Same as cass_cluster_set_dse_gssapi_authenticator(), but with lengths for string
 * parameters.
 *
 * @public @memberof CassCluster
 *
 * @param[in] cluster
 * @param[in] service
 * @param[in] service_length
 * @param[in] principal
 * @param[in] principal_length
 * @return same as cass_cluster_set_dse_gssapi_authenticator()
 */
DSE_EXPORT CassError
cass_cluster_set_dse_gssapi_authenticator_n(CassCluster* cluster,
                                            const char* service,
                                            size_t service_length,
                                            const char* principal,
                                            size_t principal_length);

/**
 * Enables GSSAPI authentication with proxy authorization for DSE clusters secured with the
 * `DseAuthenticator`.
 *
 * @public @memberof CassCluster
 *
 * @param[in] cluster
 * @param[in] service
 * @param[in] principal
 * @param[in] authorization_id
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_cluster_set_dse_gssapi_authenticator_proxy(CassCluster* cluster,
                                                const char* service,
                                                const char* principal,
                                                const char* authorization_id);

/**
 * Same as cass_cluster_set_dse_gssapi_authenticator_proxy(), but with lengths for string
 * parameters.
 *
 * @public @memberof CassCluster
 *
 * @param[in] cluster
 * @param[in] service
 * @param[in] service_length
 * @param[in] principal
 * @param[in] principal_length
 * @param[in] authorization_id
 * @param[in] authorization_id_length
 * @return same as cass_cluster_set_dse_gssapi_authenticator_proxy()
 */
DSE_EXPORT CassError
cass_cluster_set_dse_gssapi_authenticator_proxy_n(CassCluster* cluster,
                                                  const char* service,
                                                  size_t service_length,
                                                  const char* principal,
                                                  size_t principal_length,
                                                  const char* authorization_id,
                                                  size_t authorization_id_length);

/**
 * Enables plaintext authentication for DSE clusters secured with the
 * `DseAuthenticator`.
 *
 * @public @memberof CassCluster
 *
 * @param[in] cluster
 * @param[in] username
 * @param[in] password
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_cluster_set_dse_plaintext_authenticator(CassCluster* cluster,
                                             const char* username,
                                             const char* password);

/**
 * Same as cass_cluster_set_dse_plaintext_authenticator(), but with lengths for string
 * parameters.
 *
 * @public @memberof CassCluster
 *
 * @param[in] cluster
 * @param[in] username
 * @param[in] username_length
 * @param[in] password
 * @param[in] password_length
 * @return same as cass_cluster_set_dse_plaintext_authenticator()
 */
DSE_EXPORT CassError
cass_cluster_set_dse_plaintext_authenticator_n(CassCluster* cluster,
                                               const char* username,
                                               size_t username_length,
                                               const char* password,
                                               size_t password_length);

/**
 * Enables plaintext authentication with proxy authorization for DSE clusters secured with the
 * `DseAuthenticator`.
 *
 * @public @memberof CassCluster
 *
 * @param[in] cluster
 * @param[in] username
 * @param[in] password
 * @param[in] authorization_id
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_cluster_set_dse_plaintext_authenticator_proxy(CassCluster* cluster,
                                                   const char* username,
                                                   const char* password,
                                                   const char* authorization_id);

/**
 * Same as cass_cluster_set_dse_plaintext_authenticator_proxy(), but with lengths for string
 * parameters.
 *
 * @public @memberof CassCluster
 *
 * @param[in] cluster
 * @param[in] username
 * @param[in] username_length
 * @param[in] password
 * @param[in] password_length
 * @param[in] authorization_id
 * @param[in] authorization_id_length
 * @return same as cass_cluster_set_dse_plaintext_authenticator_proxy()
 */
DSE_EXPORT CassError
cass_cluster_set_dse_plaintext_authenticator_proxy_n(CassCluster* cluster,
                                                     const char* username,
                                                     size_t username_length,
                                                     const char* password,
                                                     size_t password_length,
                                                     const char* authorization_id,
                                                     size_t authorization_id_length);

/***********************************************************************************
 *
 * Batch
 *
 ***********************************************************************************/

/**
 * Sets the name of the user to execute the batch as.
 *
 * @public @memberof CassBatch
 *
 * @param[in] batch
 * @param[in] name
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_batch_set_execute_as(CassBatch* batch,
                          const char* name);

/**
 * Same as cass_batch_set_execute_as(), but with lengths for string
 * parameters.
 *
 * @public @memberof CassBatch
 *
 * @param[in] batch
 * @param[in] name
 * @param[in] name_length
 * @return same as cass_batch_set_execute_as()
 *
 * @see cass_batch_set_execute_as()
 */
DSE_EXPORT CassError
cass_batch_set_execute_as_n(CassBatch* batch,
                            const char* name, size_t name_length);

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

/**
 * Binds a date-range to a query or bound statement at the specified index.
 *
 * @public @memberof CassStatement
 *
 * @param[in] statement
 * @param[in] index
 * @param[in] range
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_statement_bind_dse_date_range(CassStatement* statement,
                                   size_t index,
                                   const DseDateRange* range);

/**
 * Binds a date-range to all the values with the specified name.
 *
 * @public @memberof CassStatement
 *
 * @param[in] statement
 * @param[in] name
 * @param[in] range
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_statement_bind_dse_date_range_by_name(CassStatement* statement,
                                           const char* name,
                                           const DseDateRange* range);

/**
 * Same as cass_statement_bind_dse_date_range_by_name(), but with lengths for string
 * parameters.
 *
 * @public @memberof CassStatement
 *
 * @param[in] statement
 * @param[in] name
 * @param[in] name_length
 * @param[in] range
 * @return same as cass_statement_bind_dse_date_range_by_name()
 *
 * @see cass_statement_bind_dse_date_range_by_name()
 */
DSE_EXPORT CassError
cass_statement_bind_dse_date_range_by_name_n(CassStatement* statement,
                                             const char* name, size_t name_length,
                                             const DseDateRange* range);

/**
 * Sets the name of the user to execute the statement as.
 *
 * @public @memberof CassStatement
 *
 * @param[in] statement
 * @param[in] name
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_statement_set_execute_as(CassStatement* statement,
                              const char* name);

/**
 * Same as cass_statement_set_execute_as(), but with lengths for string
 * parameters.
 *
 * @public @memberof CassStatement
 *
 * @param[in] statement
 * @param[in] name
 * @param[in] name_length
 * @return same as cass_statement_set_execute_as()
 *
 * @see cass_statement_set_execute_as()
 */
DSE_EXPORT CassError
cass_statement_set_execute_as_n(CassStatement* statement,
                                const char* name, size_t name_length);

/***********************************************************************************
 *
 * Collection
 *
 ***********************************************************************************/

/**
 * Appends a point to the collection.
 *
 * @public @memberof CassCollection
 *
 * @param[in] collection
 * @param[in] x
 * @param[in] y
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_collection_append_dse_point(CassCollection* collection,
                                 cass_double_t x, cass_double_t y);

/**
 * Appends a line string to the collection.
 *
 * @public @memberof CassCollection
 *
 * @param[in] collection
 * @param[in] line_string
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_collection_append_dse_line_string(CassCollection* collection,
                                       const DseLineString* line_string);

/**
 * Appends a polygon to the collection.
 *
 * @public @memberof CassCollection
 *
 * @param[in] collection
 * @param[in] polygon
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_collection_append_dse_polygon(CassCollection* collection,
                                   const DsePolygon* polygon);


/**
 * Appends a DateRange to the collection.
 *
 * @public @memberof CassCollection
 *
 * @param[in] collection
 * @param[in] range
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_collection_append_dse_date_range(CassCollection* collection,
                                      const DseDateRange* range);

/***********************************************************************************
 *
 * Tuple
 *
 ***********************************************************************************/

/**
 * Sets a point in a tuple at the specified index.
 *
 * @public @memberof CassTuple
 *
 * @param[in] tuple
 * @param[in] index
 * @param[in] x
 * @param[in] y
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_tuple_set_dse_point(CassTuple* tuple,
                         size_t index,
                         cass_double_t x, cass_double_t y);

/**
 * Sets a line string in a tuple at the specified index.
 *
 * @public @memberof CassTuple
 *
 * @param[in] tuple
 * @param[in] index
 * @param[in] line_string
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_tuple_set_dse_line_string(CassTuple* tuple,
                               size_t index,
                               const DseLineString* line_string);

/**
 * Sets a polygon in a tuple at the specified index.
 *
 * @public @memberof CassTuple
 *
 * @param[in] tuple
 * @param[in] index
 * @param[in] polygon
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_tuple_set_dse_polygon(CassTuple* tuple,
                           size_t index,
                           const DsePolygon* polygon);

/**
 * Sets a DateRange in a tuple at the specified index.
 *
 * @public @memberof CassTuple
 *
 * @param[in] tuple
 * @param[in] index
 * @param[in] range
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_tuple_set_dse_date_range(CassTuple* tuple,
                              size_t index,
                              const DseDateRange* range);

/***********************************************************************************
 *
 * User defined type
 *
 ***********************************************************************************/

/**
 * Sets a point in a user defined type at the specified index.
 *
 * @public @memberof CassUserType
 *
 * @param[in] user_type
 * @param[in] index
 * @param[in] x
 * @param[in] y
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_user_type_set_dse_point(CassUserType* user_type,
                             size_t index,
                             cass_double_t x, cass_double_t y);

/**
 * Sets a point in a user defined type at the specified name.
 *
 * @public @memberof CassUserType
 *
 * @param[in] user_type
 * @param[in] name
 * @param[in] x
 * @param[in] y
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_user_type_set_dse_point_by_name(CassUserType* user_type,
                                     const char* name,
                                     cass_double_t x, cass_double_t y);

/**
 * Same as cass_user_type_set_dse_point_by_name(), but with lengths for string
 * parameters.
 *
 * @public @memberof CassUserType
 *
 * @param[in] user_type
 * @param[in] name
 * @param[in] name_length
 * @param[in] x
 * @param[in] y
 * @return same as cass_user_type_set_dse_point_by_name()
 *
 * @see cass_user_type_set_dse_point_by_name()
 */
DSE_EXPORT CassError
cass_user_type_set_dse_point_by_name_n(CassUserType* user_type,
                                       const char* name,
                                       size_t name_length,
                                       cass_double_t x, cass_double_t y);

/**
 * Sets a line string in a user defined type at the specified index.
 *
 * @public @memberof CassUserType
 *
 * @param[in] user_type
 * @param[in] index
 * @param[in] line_string
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_user_type_set_dse_line_string(CassUserType* user_type,
                                   size_t index,
                                   const DseLineString* line_string);

/**
 * Sets a line string in a user defined type at the specified name.
 *
 * @public @memberof CassUserType
 *
 * @param[in] user_type
 * @param[in] name
 * @param[in] line_string
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_user_type_set_dse_line_string_by_name(CassUserType* user_type,
                                           const char* name,
                                           const DseLineString* line_string);

/**
 * Same as cass_user_type_set_dse_line_string_by_name(), but with lengths for string
 * parameters.
 *
 * @public @memberof CassUserType
 *
 * @param[in] user_type
 * @param[in] name
 * @param[in] name_length
 * @param[in] line_string
 * @return same as cass_user_type_set_dse_line_string_by_name()
 *
 * @see cass_user_type_set_dse_line_string_by_name()
 */
DSE_EXPORT CassError
cass_user_type_set_dse_line_string_by_name_n(CassUserType* user_type,
                                             const char* name,
                                             size_t name_length,
                                             const DseLineString* line_string);

/**
 * Sets a polygon in a user defined type at the specified index.
 *
 * @public @memberof CassUserType
 *
 * @param[in] user_type
 * @param[in] index
 * @param[in] polygon
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_user_type_set_dse_polygon(CassUserType* user_type,
                               size_t index,
                               const DsePolygon* polygon);

/**
 * Sets a polygon in a user defined type at the specified name.
 *
 * @public @memberof CassUserType
 *
 * @param[in] user_type
 * @param[in] name
 * @param[in] polygon
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_user_type_set_dse_polygon_by_name(CassUserType* user_type,
                                       const char* name,
                                       const DsePolygon* polygon);

/**
 * Same as cass_user_type_set_dse_polygon_by_name(), but with lengths for string
 * parameters.
 *
 * @public @memberof CassUserType
 *
 * @param[in] user_type
 * @param[in] name
 * @param[in] name_length
 * @param[in] polygon
 * @return same as cass_user_type_set_dse_polygon_by_name()
 *
 * @see cass_user_type_set_dse_polygon_by_name()
 */
DSE_EXPORT CassError
cass_user_type_set_dse_polygon_by_name_n(CassUserType* user_type,
                                         const char* name,
                                         size_t name_length,
                                         const DsePolygon* polygon);

/**
 * Sets a DateRange in a user defined type at the specified index.
 *
 * @public @memberof CassUserType
 *
 * @param[in] user_type
 * @param[in] index
 * @param[in] range
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_user_type_set_dse_date_range(CassUserType* user_type,
                                  size_t index,
                                  const DseDateRange* range);

/**
 * Sets DateRange in a user defined type at the specified name.
 *
 * @public @memberof CassUserType
 *
 * @param[in] user_type
 * @param[in] name
 * @param[in] range
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
cass_user_type_set_dse_date_range_by_name(CassUserType* user_type,
                                          const char* name,
                                          const DseDateRange* range);

/**
 * Same as cass_user_type_set_dse_date_range_by_name(), but with lengths for string
 * parameters.
 *
 * @public @memberof CassUserType
 *
 * @param[in] user_type
 * @param[in] name
 * @param[in] name_length
 * @param[in] range
 * @return same as cass_user_type_set_dse_date_range_by_name()
 *
 * @see cass_user_type_set_dse_date_range_by_name()
 */
DSE_EXPORT CassError
cass_user_type_set_dse_date_range_by_name_n(CassUserType* user_type,
                                            const char* name,
                                            size_t name_length,
                                            const DseDateRange* range);


/***********************************************************************************
 *
 * Value
 *
 ***********************************************************************************/

/**
 * Gets a date-range for the specified value.
 *
 * @public @memberof CassValue
 *
 * @param[in] value
 * @param[out] range
 * @return CASS_OK if successful, otherwise error occurred
 */
DSE_EXPORT CassError
cass_value_get_dse_date_range(const CassValue* value,
                              DseDateRange* range);

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

/***********************************************************************************
 *
 * Point
 *
 ***********************************************************************************/

/**
 * Parse the WKT representation of a point and extract the x,y coordinates.
 *
 * @param[in] wkt WKT representation of the point
 * @param[out] x
 * @param[out] y
 * @return CASS_OK if successful, otherwise error occurred
 */
DSE_EXPORT CassError
dse_point_from_wkt(const char* wkt, cass_double_t* x, cass_double_t* y);

/**
 * Same as dse_point_from_wkt(), but with lengths for string parameters.
 *
 * @param[in] wkt WKT representation of the point
 * @param[in] wkt_length length of the wkt string
 * @param[out] x
 * @param[out] y
 * @return CASS_OK if successful, otherwise error occurred
 */
DSE_EXPORT CassError
dse_point_from_wkt_n(const char* wkt, size_t wkt_length, cass_double_t* x, cass_double_t* y);

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
 * Reserves enough memory to contain the provided number of points. This can
 * be use to reduce memory allocations, but it is not required.
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
 * Resets a line string iterator so that it can be reused to process a
 * binary representation.
 *
 * @public @memberof DseLineStringIterator
 *
 * @param[in] iterator the iterator to reset
 * @param[in] value binary representation of the line string
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_line_string_iterator_reset(DseLineStringIterator* iterator,
                               const CassValue* value);

/**
 * Resets a line string iterator so that it can be reused to parse WKT.
 *
 * @public @memberof DseLineStringIterator
 *
 * @param[in] iterator the iterator to reset
 * @param[in] wkt WKT representation of the line string
 * @note The wkt string must remain allocated throughout the lifetime
 *       of the iterator since the iterator traverses the string without
 *       copying it.
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_line_string_iterator_reset_with_wkt(DseLineStringIterator* iterator,
                                        const char* wkt);

/**
 * Same as dse_line_string_iterator_reset_with_wkt(), but with lengths
 * for string parameters.
 *
 * @public @memberof DseLineStringIterator
 *
 * @param[in] iterator the iterator to reset
 * @param[in] wkt WKT representation (string) of the line string
 * @param[in] wkt_length length of wkt string
 * @note The wkt string must remain allocated throughout the lifetime
 *       of the iterator since the iterator traverses the string without
 *       copying it.
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_line_string_iterator_reset_with_wkt_n(DseLineStringIterator* iterator,
                                          const char* wkt,
                                          size_t wkt_length);

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
 * Reserves enough memory to contain the provided number rings and points. This can
 * be use to reduce memory allocations, but it is not required.
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
 * Resets a polygon iterator so that it can be reused to process a
 * binary representation.
 *
 * @public @memberof DsePolygonIterator
 *
 * @param[in] iterator the iterator to reset
 * @param[in] value binary representation of the polygon
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_polygon_iterator_reset(DsePolygonIterator* iterator,
                           const CassValue* value);

/**
 * Resets a polygon iterator so that it can be reused to parse WKT.
 *
 * @public @memberof DsePolygonIterator
 *
 * @param[in] iterator the iterator to reset
 * @param[in] wkt WKT representation of the polygon
 * @note The wkt string must remain allocated throughout the lifetime
 *       of the iterator since the iterator traverses the string without
 *       copying it.
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_polygon_iterator_reset_with_wkt(DsePolygonIterator* iterator,
                                    const char* wkt);

/**
 * Same as dse_polygon_iterator_reset_with_wkt(), but with lengths for
 * string parameters.
 *
 * @public @memberof DsePolygonIterator
 *
 * @param[in] iterator the iterator to reset
 * @param[in] wkt WKT representation (string) of the polygon
 * @param[in] wkt_length length of wkt string
 * @note The wkt string must remain allocated throughout the lifetime
 *       of the iterator since the iterator traverses the string without
 *       copying it.
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_polygon_iterator_reset_with_wkt_n(DsePolygonIterator* iterator,
                                      const char* wkt,
                                      size_t wkt_length);

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
 * GSSAPI lock callback.
 */
typedef void (*DseGssapiAuthenticatorLockCallback)(void* data);

/**
 *
 * GSSAPI unlock callback.
 */
typedef void (*DseGssapiAuthenticatorUnlockCallback)(void* data);

/**
 * Set lock callbacks for GSSAPI authentication. This is used to protect
 * Kerberos libraries that are not thread-safe.
 *
 * @param[in] lock_callback
 * @param[in] unlock_callback
 * @param[in] data
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_gssapi_authenticator_set_lock_callbacks(DseGssapiAuthenticatorLockCallback lock_callback,
                                            DseGssapiAuthenticatorUnlockCallback unlock_callback,
                                            void* data);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* __DSE_H_INCLUDED__ */
