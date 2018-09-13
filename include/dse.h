/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __DSE_H_INCLUDED__
#define __DSE_H_INCLUDED__

#if !defined(DSE_STATIC)
#  if (defined(DSE_BUILDING) && !defined(CASS_BUILDING))
#    define CASS_BUILDING
#  endif
#elif !defined(CASS_STATIC)
#  define CASS_STATIC
#endif

#include <dse/cassandra.h>

#if !defined(DSE_STATIC)
#  if (defined(WIN32) || defined(_WIN32))
#    if defined(DSE_BUILDING)
#      define DSE_EXPORT __declspec(dllexport)
#    else
#      define DSE_EXPORT __declspec(dllimport)
#    endif
#  elif (defined(__SUNPRO_C)  || defined(__SUNPRO_CC)) && !defined(DSE_STATIC)
#    define DSE_EXPORT __global
#  elif (defined(__GNUC__) && __GNUC__ >= 4) || defined(__INTEL_COMPILER)
#    define DSE_EXPORT __attribute__ ((visibility("default")))
#  endif
#else
#  define DSE_EXPORT
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
 * C/C++ DataStax Enterprise Driver
 */

#define DSE_VERSION_MAJOR 1
#define DSE_VERSION_MINOR 6
#define DSE_VERSION_PATCH 0
#define DSE_VERSION_SUFFIX ""

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
 * Graph
 *
 ***********************************************************************************/

/**
 * Graph options for executing graph queries.
 *
 * @struct DseGraphOptions
 */
typedef struct DseGraphOptions_ DseGraphOptions;

/**
 * Graph statement for executing graph queries. This represents a graph query
 * string, graph options and graph values used to execute a graph query.
 *
 * @struct DseGraphStatement
 */
typedef struct DseGraphStatement_ DseGraphStatement;

/**
 * Graph object builder for constructing a collection of member pairs.
 *
 * @struct DseGraphObject
 */
typedef struct DseGraphObject_ DseGraphObject;

/**
 * Graph array builder for constructing an array of elements.
 *
 * @struct DseGraphArray
 */
typedef struct DseGraphArray_ DseGraphArray;

/**
 * Graph result set
 *
 * @struct DseGraphResultSet
 */
typedef struct DseGraphResultSet_ DseGraphResultSet;

/**
 * Graph result types
 */
typedef enum DseGraphResultType_ {
  DSE_GRAPH_RESULT_TYPE_NULL,
  DSE_GRAPH_RESULT_TYPE_BOOL,
  DSE_GRAPH_RESULT_TYPE_NUMBER,
  DSE_GRAPH_RESULT_TYPE_STRING,
  DSE_GRAPH_RESULT_TYPE_OBJECT,
  DSE_GRAPH_RESULT_TYPE_ARRAY
} DseGraphResultType;

/**
 * Graph result
 *
 * @struct DseGraphResult
 */
typedef struct DseGraphResult_ DseGraphResult;

/**
 * Graph edge
 *
 * @struct DseGraphEdgeResult
 */
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

/**
 * Graph vertex
 *
 * @struct DseGraphVertexResult
 */
typedef struct DseGraphVertexResult_ {
  const DseGraphResult* id;
  const DseGraphResult* label;
  const DseGraphResult* type;
  const DseGraphResult* properties;
} DseGraphVertexResult;

/**
 * Graph path
 *
 * @struct DseGraphPathResult
 */
typedef struct DseGraphPathResult_ {
  const DseGraphResult* labels;
  const DseGraphResult* objects;
} DseGraphPathResult;

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
 * Creates a new cluster with DSE specific settings.
 *
 * @public @memberof CassCluster
 *
 * @return Returns a cluster that must be freed.
 *
 * @see cass_cluster_free()
 */
DSE_EXPORT CassCluster*
cass_cluster_new_dse();

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
 * Session
 *
 ***********************************************************************************/

/**
 * Execute a graph statement.
 *
 * @public @memberof CassSession
 *
 * @param[in] session
 * @param[in] statement
 * @return A future that must be freed.
 *
 * @see cass_future_get_dse_graph_resultset()
 */
DSE_EXPORT CassFuture*
cass_session_execute_dse_graph(CassSession* session,
                               const DseGraphStatement* statement);

/***********************************************************************************
 *
 * Future
 *
 ***********************************************************************************/

/**
 * Gets the graph result set of a successful future. If the future is not ready this method will
 * wait for the future to be set.
 *
 * @public @memberof CassFuture
 *
 * @param[in] future
 * @return DseGraphResultSet instance if successful, otherwise NULL for error. The return instance
 * must be freed using dse_graph_resultset_free().
 *
 * @see cass_session_execute_dse_graph()
 */
DSE_EXPORT DseGraphResultSet*
cass_future_get_dse_graph_resultset(CassFuture* future);

/***********************************************************************************
 *
 * Graph Options
 *
 ***********************************************************************************/

/**
 * Creates a new instance of graph options.
 *
 * @public @memberof DseGraphOptions
 *
 * @return Returns a instance of graph options that must be freed.
 *
 * @see dse_graph_options_free()
 */
DSE_EXPORT DseGraphOptions*
dse_graph_options_new();

/**
 * Creates a new instance of graph options from an existing object.
 *
 * @public @memberof DseGraphOptions
 *
 * @return Returns a instance of graph options that must be freed.
 *
 * @see dse_graph_options_free()
 */
DSE_EXPORT DseGraphOptions*
dse_graph_options_new_from_existing(const DseGraphOptions* options);

/**
 * Frees a graph options instance.
 *
 * @public @memberof DseGraphOptions
 *
 * @param[in] options
 */
DSE_EXPORT void
dse_graph_options_free(DseGraphOptions* options);

/**
 * Set the graph language to be used in graph queries.
 *
 * Default: gremlin-groovy
 *
 * @public @memberof DseGraphOptions
 *
 * @param[in] options
 * @param[in] language
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_options_set_graph_language(DseGraphOptions* options,
                                     const char* language);

/**
 * Same as dse_graph_options_set_graph_language(), but with lengths for string
 * parameters.
 *
 * @public @memberof DseGraphOptions
 *
 * @param[in] options
 * @param[in] language
 * @param[in] language_length
 * @return same as dse_graph_options_set_graph_language()
 */
DSE_EXPORT CassError
dse_graph_options_set_graph_language_n(DseGraphOptions* options,
                                       const char* language, size_t language_length);

/**
 * Set the graph traversal source name to be used in graph queries.
 *
 * Default: default
 *
 * @public @memberof DseGraphOptions
 *
 * @param[in] options
 * @param[in] source
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_options_set_graph_source(DseGraphOptions* options,
                                   const char* source);

/**
 * Same as dse_graph_options_set_graph_source(), but with lengths for string
 * parameters.
 *
 * @public @memberof DseGraphOptions
 *
 * @param[in] options
 * @param[in] source
 * @param[in] source_length
 * @return same as dse_graph_options_set_graph_source()
 */
DSE_EXPORT CassError
dse_graph_options_set_graph_source_n(DseGraphOptions* options,
                                     const char* source, size_t source_length);

/**
 * Set the graph name to be used in graph queries. This is optional and the
 * name is left unset if this function is not called.
 *
 * @public @memberof DseGraphOptions
 *
 * @param[in] options
 * @param[in] name
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_options_set_graph_name(DseGraphOptions* options,
                                 const char* name);

/**
 * Same as dse_graph_options_set_graph_name(), but with lengths for string
 * parameters.
 *
 * @public @memberof DseGraphOptions
 *
 * @param[in] options
 * @param[in] name
 * @param[in] name_length
 * @return same as dse_graph_options_set_graph_name()
 */
DSE_EXPORT CassError
dse_graph_options_set_graph_name_n(DseGraphOptions* options,
                                   const char* name, size_t name_length);

/**
 * Set the read consistency used by graph queries.
 *
 * @public @memberof DseGraphOptions
 *
 * @param[in] options
 * @param[in] consistency
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_options_set_read_consistency(DseGraphOptions* options,
                                       CassConsistency consistency);

/**
 * Set the write consistency used by graph queries.
 *
 * @public @memberof DseGraphOptions
 *
 * @param[in] options
 * @param[in] consistency
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_options_set_write_consistency(DseGraphOptions* options,
                                        CassConsistency consistency);

/**
 * Set the request timeout used by graph queries. Only use this if you want
 * graph queries to wait less than the server's default timeout (defined in
 * "dse.yaml")
 *
 * <b>Default:</b> 0 (wait for the coordinator to response or timeout)
 *
 * @public @memberof DseGraphOptions
 *
 * @param[in] options
 * @param[in] timeout_ms
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_options_set_request_timeout(DseGraphOptions* options,
                                      cass_int64_t timeout_ms);

/***********************************************************************************
 *
 * Graph Statement
 *
 ***********************************************************************************/

/**
 * Creates a new instance of graph statement.
 *
 * @public @memberof DseGraphStatement
 *
 * @param[in] query
 * @param[in] options Optional. Use NULL for a system query with the
 * default graph language and source.
 * @return Returns a instance of graph statement that must be freed.
 */
DSE_EXPORT DseGraphStatement*
dse_graph_statement_new(const char* query,
                        const DseGraphOptions* options);

/**
 * Same as dse_graph_statement_new(), but with lengths for string
 * parameters.
 *
 * @public @memberof DseGraphStatement
 *
 * @param[in] query
 * @param[in] query_length
 * @param[in] options Optional. Use NULL for a system query with the
 * default graph language and source.
 * @return same as dse_graph_statement_new()
 */
DSE_EXPORT DseGraphStatement*
dse_graph_statement_new_n(const char* query,
                          size_t query_length,
                          const DseGraphOptions* options);

/**
 * Frees a graph statement instance.
 *
 * @public @memberof DseGraphStatement
 *
 * @param[in] statement
 */
DSE_EXPORT void
dse_graph_statement_free(DseGraphStatement* statement);

/**
 * Bind the values to a graph query.
 *
 * @public @memberof DseGraphStatement
 *
 * @param[in] statement
 * @param[in] values
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_statement_bind_values(DseGraphStatement* statement,
                                const DseGraphObject* values);


/**
 * Sets the graph statement's timestamp.
 *
 * @public @memberof DseGraphStatement
 *
 * @param[in] statement
 * @param[in] timestamp
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_statement_set_timestamp(DseGraphStatement* statement,
                                  cass_int64_t timestamp);

/***********************************************************************************
 *
 * Graph Object
 *
 ***********************************************************************************/

/**
 * Creates a new instance of graph object.
 *
 * @public @memberof DseGraphObject
 */
DSE_EXPORT DseGraphObject*
dse_graph_object_new();

/**
 * Frees a graph object instance.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 */
DSE_EXPORT void
dse_graph_object_free(DseGraphObject* object);

/**
 * Reset a graph object. This function must be called after previously finishing
 * an object (dse_graph_object_finish()). This can be used to resuse an
 * instance of DseGraphObject to create multiple objects.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 */
DSE_EXPORT void
dse_graph_object_reset(DseGraphObject* object);

/**
 * Finish a graph object. This function must be called before adding an object to
 * another object, array or binding to a statement.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 */
DSE_EXPORT void
dse_graph_object_finish(DseGraphObject* object);

/**
 * Add null to an object with the specified name.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_object_add_null(DseGraphObject* object,
                          const char* name);

/**
 * Same as dse_graph_object_add_null(), but with lengths for string
 * parameters.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] name_length
 * @return same as dse_graph_object_add_null()
 */
DSE_EXPORT CassError
dse_graph_object_add_null_n(DseGraphObject* object,
                            const char* name,
                            size_t name_length);

/**
 * Add boolean to an object with the specified name.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_object_add_bool(DseGraphObject* object,
                          const char* name,
                          cass_bool_t value);

/**
 * Same as dse_graph_object_add_bool(), but with lengths for string
 * parameters.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] name_length
 * @param[in] value
 * @return same as dse_graph_object_add_bool()
 */
DSE_EXPORT CassError
dse_graph_object_add_bool_n(DseGraphObject* object,
                            const char* name,
                            size_t name_length,
                            cass_bool_t value);

/**
 * Add integer (32-bit) to an object with the specified name.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_object_add_int32(DseGraphObject* object,
                           const char* name,
                           cass_int32_t value);

/**
 * Same as dse_graph_object_add_int32(), but with lengths for string
 * parameters.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] name_length
 * @param[in] value
 * @return same as dse_graph_object_add_int32()
 */
DSE_EXPORT CassError
dse_graph_object_add_int32_n(DseGraphObject* object,
                             const char* name,
                             size_t name_length,
                             cass_int32_t value);

/**
 * Add integer (64-bit) to an object with the specified name.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_object_add_int64(DseGraphObject* object,
                           const char* name,
                           cass_int64_t value);

/**
 * Same as dse_graph_object_add_int64(), but with lengths for string
 * parameters.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] name_length
 * @param[in] value
 * @return same as dse_graph_object_add_int64()
 */
DSE_EXPORT CassError
dse_graph_object_add_int64_n(DseGraphObject* object,
                             const char* name,
                             size_t name_length,
                             cass_int64_t value);

/**
 * Add double to an object with the specified name.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_object_add_double(DseGraphObject* object,
                            const char* name,
                            cass_double_t value);

/**
 * Same as dse_graph_object_add_double(), but with lengths for string
 * parameters.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] name_length
 * @param[in] value
 * @return same as dse_graph_object_add_double()
 */
DSE_EXPORT CassError
dse_graph_object_add_double_n(DseGraphObject* object,
                              const char* name,
                              size_t name_length,
                              cass_double_t value);

/**
 * Add string to an object with the specified name.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_object_add_string(DseGraphObject* object,
                            const char* name,
                            const char* value);

/**
 * Same as dse_graph_object_add_string(), but with lengths for string
 * parameters.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] name_length
 * @param[in] value
 * @param[in] value_length
 * @return same as dse_graph_object_add_string()
 */
DSE_EXPORT CassError
dse_graph_object_add_string_n(DseGraphObject* object,
                              const char* name,
                              size_t name_length,
                              const char* value,
                              size_t value_length);

/**
 * Add object to an object with the specified name.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_object_add_object(DseGraphObject* object,
                            const char* name,
                            const DseGraphObject* value);

/**
 * Same as dse_graph_object_add_object(), but with lengths for string
 * parameters.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] name_length
 * @param[in] value
 * @return same as dse_graph_object_add_object()
 */
DSE_EXPORT CassError
dse_graph_object_add_object_n(DseGraphObject* object,
                              const char* name,
                              size_t name_length,
                              const DseGraphObject* value);

/**
 * Add array to an object with the specified name.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_object_add_array(DseGraphObject* object,
                           const char* name,
                           const DseGraphArray* value);

/**
 * Same as dse_graph_object_add_array(), but with lengths for string
 * parameters.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] name_length
 * @param[in] value
 * @return same as dse_graph_object_add_array()
 */
DSE_EXPORT CassError
dse_graph_object_add_array_n(DseGraphObject* object,
                             const char* name,
                             size_t name_length,
                             const DseGraphArray* value);

/**
 * Add point geometric type to an object with the specified name.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] x
 * @param[in] y
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_object_add_point(DseGraphObject* object,
                           const char* name,
                           cass_double_t x, cass_double_t y);

/**
 * Same as dse_graph_object_add_point(), but with lengths for string
 * parameters.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] name_length
 * @param[in] x
 * @param[in] y
 * @return same as dse_graph_object_add_point()
 */
DSE_EXPORT CassError
dse_graph_object_add_point_n(DseGraphObject* object,
                             const char* name,
                             size_t name_length,
                             cass_double_t x, cass_double_t y);

/**
 * Add line string geometric type to an object with the specified name.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_object_add_line_string(DseGraphObject* object,
                                 const char* name,
                                 const DseLineString* value);

/**
 * Same as dse_graph_object_add_line_string(), but with lengths for string
 * parameters.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] name_length
 * @param[in] value
 * @return same as dse_graph_object_add_line_string()
 */
DSE_EXPORT CassError
dse_graph_object_add_line_string_n(DseGraphObject* object,
                                   const char* name,
                                   size_t name_length,
                                   const DseLineString* value);

/**
 * Add polygon geometric type to an object with the specified name.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_object_add_polygon(DseGraphObject* object,
                             const char* name,
                             const DsePolygon* value);

/**
 * Same as dse_graph_object_add_polygon(), but with lengths for string
 * parameters.
 *
 * @public @memberof DseGraphObject
 *
 * @param[in] object
 * @param[in] name
 * @param[in] name_length
 * @param[in] value
 * @return same as dse_graph_object_add_polygon()
 */
DSE_EXPORT CassError
dse_graph_object_add_polygon_n(DseGraphObject* object,
                               const char* name,
                               size_t name_length,
                               const DsePolygon* value);

/***********************************************************************************
 *
 * Graph Array
 *
 ***********************************************************************************/

/**
 * Creates a new instance of graph array.
 *
 * @public @memberof DseGraphArray
 */
DSE_EXPORT DseGraphArray*
dse_graph_array_new();

/**
 * Frees a graph array instance.
 *
 * @public @memberof DseGraphArray
 *
 * @param[in] array
 */
DSE_EXPORT void
dse_graph_array_free(DseGraphArray* array);

/**
 * Reset a graph array. This function must be called after previously finishing
 * an array (dse_graph_array_finish()). This can be used to resuse an
 * instance of DseGraphArray to create multiple arrays.
 *
 * @public @memberof DseGraphArray
 *
 * @param[in] array
 */
DSE_EXPORT void
dse_graph_array_reset(DseGraphArray* array);

/**
 * Finish a graph array. This function must be called before adding an array to
 * another object, array or binding to a statement.
 *
 * @public @memberof DseGraphArray
 *
 * @param[in] array
 */
DSE_EXPORT void
dse_graph_array_finish(DseGraphArray* array);

/**
 * Add null to an array.
 *
 * @public @memberof DseGraphArray
 *
 * @param[in] array
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_array_add_null(DseGraphArray* array);

/**
 * Add boolean to an array.
 *
 * @public @memberof DseGraphArray
 *
 * @param[in] array
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_array_add_bool(DseGraphArray* array,
                         cass_bool_t value);

/**
 * Add integer (32-bit) to an array.
 *
 * @public @memberof DseGraphArray
 *
 * @param[in] array
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_array_add_int32(DseGraphArray* array,
                          cass_int32_t value);

/**
 * Add integer (64-bit) to an array.
 *
 * @public @memberof DseGraphArray
 *
 * @param[in] array
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_array_add_int64(DseGraphArray* array,
                          cass_int64_t value);

/**
 * Add double to an array.
 *
 * @public @memberof DseGraphArray
 *
 * @param[in] array
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_array_add_double(DseGraphArray* array,
                           cass_double_t value);

/**
 * Add string to an array.
 *
 * @public @memberof DseGraphArray
 *
 * @param[in] array
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_array_add_string(DseGraphArray* array,
                           const char* value);

/**
 * Same as dse_graph_array_add_string(), but with lengths for string
 * parameters.
 *
 * @public @memberof DseGraphArray
 *
 * @param[in] array
 * @param[in] value
 * @param[in] value_length
 * @return same as dse_graph_array_add_string()
 */
DSE_EXPORT CassError
dse_graph_array_add_string_n(DseGraphArray* array,
                             const char* value,
                             size_t value_length);

/**
 * Add object to an array.
 *
 * @public @memberof DseGraphArray
 *
 * @param[in] array
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_array_add_object(DseGraphArray* array,
                           const DseGraphObject* value);

/**
 * Add array to an array.
 *
 * @public @memberof DseGraphArray
 *
 * @param[in] array
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_array_add_array(DseGraphArray* array,
                          const DseGraphArray* value);

/**
 * Add point geometric type to an array.
 *
 * @public @memberof DseGraphArray
 *
 * @param[in] array
 * @param[in] x
 * @param[in] y
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_array_add_point(DseGraphArray* array,
                          cass_double_t x, cass_double_t y);

/**
 * Add line string geometric type to an array.
 *
 * @public @memberof DseGraphArray
 *
 * @param[in] array
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_array_add_line_string(DseGraphArray* array,
                                const DseLineString* value);

/**
 * Add polygon geometric type to an array.
 *
 * @public @memberof DseGraphArray
 *
 * @param[in] array
 * @param[in] value
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_array_add_polygon(DseGraphArray* array,
                            const DsePolygon* value);

/***********************************************************************************
 *
 * Graph Result Set
 *
 ***********************************************************************************/

/**
 * Frees a graph result set instance.
 *
 * @public @memberof DseGraphResultSet
 *
 * @param[in] resultset
 */
DSE_EXPORT void
dse_graph_resultset_free(DseGraphResultSet* resultset);

/**
 * Returns the number of results in the result set.
 *
 * @public @memberof DseGraphResultSet
 *
 * @param[in] resultset
 * @return The number of results in the result set.
 */
DSE_EXPORT size_t
dse_graph_resultset_count(DseGraphResultSet* resultset);

/**
 * Returns the next result in the result set.
 *
 * @public @memberof DseGraphResultSet
 *
 * @param[in] resultset
 * @return The next result.
 */
DSE_EXPORT const DseGraphResult*
dse_graph_resultset_next(DseGraphResultSet* resultset);

/***********************************************************************************
 *
 * Graph Result
 *
 ***********************************************************************************/

/**
 * Returns the type of the result.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @return The type of the current result.
 */
DSE_EXPORT DseGraphResultType
dse_graph_result_type(const DseGraphResult* result);

/**
 * Returns true if the result is null.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @return True if the result is null, otherwise false.
 */
DSE_EXPORT cass_bool_t
dse_graph_result_is_null(const DseGraphResult* result);

/**
 * Returns true if the result is a boolean.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @return True if the result is boolean, otherwise false.
 */
DSE_EXPORT cass_bool_t
dse_graph_result_is_bool(const DseGraphResult* result);

/**
 * Returns true if the result is an integer (32-bit).
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @return True if the result is a number that be held in an integer (32-bit),
 * otherwise false.
 */
DSE_EXPORT cass_bool_t
dse_graph_result_is_int32(const DseGraphResult* result);

/**
 * Returns true if the result is an integer (64-bit).
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @return True if the result is a number that be held in an integer (64-bit),
 * otherwise false.
 */
DSE_EXPORT cass_bool_t
dse_graph_result_is_int64(const DseGraphResult* result);

/**
 * Returns true if the result is a double.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @return True if the result is a number that be held in a double,
 * otherwise false.
 */
DSE_EXPORT cass_bool_t
dse_graph_result_is_double(const DseGraphResult* result);

/**
 * Returns true if the result is a string.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @return True if the result is a string, otherwise false.
 */
DSE_EXPORT cass_bool_t
dse_graph_result_is_string(const DseGraphResult* result);

/**
 * Returns true if the result is an object.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @return True if the result is a object, otherwise false.
 */
DSE_EXPORT cass_bool_t
dse_graph_result_is_object(const DseGraphResult* result);

/**
 * Returns true if the result is an array.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @return True if the result is a array, otherwise false.
 */
DSE_EXPORT cass_bool_t
dse_graph_result_is_array(const DseGraphResult* result);

/**
 * Get the boolean value from the result.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @return The boolean value.
 */
DSE_EXPORT cass_bool_t
dse_graph_result_get_bool(const DseGraphResult* result);

/**
 * Get the integer (32-bit) value from the result.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @return The integer (32-bit) value.
 */
DSE_EXPORT cass_int32_t
dse_graph_result_get_int32(const DseGraphResult* result);

/**
 * Get the integer (64-bit) value from the result.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @return The integer (64-bit) value.
 */
DSE_EXPORT cass_int64_t
dse_graph_result_get_int64(const DseGraphResult* result);

/**
 * Get the double value from the result.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @return The double value.
 */
DSE_EXPORT cass_double_t
dse_graph_result_get_double(const DseGraphResult* result);

/**
 * Get the string value from the result.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @param[out] length
 * @return The string value.
 */
DSE_EXPORT const char*
dse_graph_result_get_string(const DseGraphResult* result,
                            size_t* length);

/**
 * Return an object as an graph edge.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @param[out] edge
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_result_as_edge(const DseGraphResult* result,
                         DseGraphEdgeResult* edge);

/**
 * Return an object as an graph vertex.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @param[out] vertex
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_result_as_vertex(const DseGraphResult* result,
                           DseGraphVertexResult* vertex);

/**
 * Return an object as an graph path.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @param[out] path
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_result_as_path(const DseGraphResult* result,
                         DseGraphPathResult* path);

/**
 * Return an object as the point geometric type.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @param[out] x
 * @param[out] y
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_result_as_point(const DseGraphResult* result,
                          cass_double_t* x, cass_double_t* y);

/**
 * Return an object as the line string geometric type.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @param[out] line_string
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_result_as_line_string(const DseGraphResult* result,
                                DseLineStringIterator* line_string);

/**
 * Return an object as the polygon geometric type.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @param[out] polygon
 * @return CASS_OK if successful, otherwise an error occurred.
 */
DSE_EXPORT CassError
dse_graph_result_as_polygon(const DseGraphResult* result,
                            DsePolygonIterator* polygon);

/**
 * Returns the number of members in an object result.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @return The number of members in an object result.
 */
DSE_EXPORT size_t
dse_graph_result_member_count(const DseGraphResult* result);

/**
 * Return the string key of an object at the specified index.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @param[in] index
 * @param[out] length
 * @return The string key of the member.
 */
DSE_EXPORT const char*
dse_graph_result_member_key(const DseGraphResult* result,
                            size_t index,
                            size_t* length);

/**
 * Return the result value of an object at the specified index.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @param[in] index
 *
 * @return The result value of the member.
 */
DSE_EXPORT const DseGraphResult*
dse_graph_result_member_value(const DseGraphResult* result,
                              size_t index);

/**
 * Returns the number of elements in an array result.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @return The number of elements in array result.
 */
DSE_EXPORT size_t
dse_graph_result_element_count(const DseGraphResult* result);

/**
 * Returns the result value of an array at the specified index.
 *
 * @public @memberof DseGraphResult
 *
 * @param[in] result
 * @param[in] index
 * @return The result value.
 */
DSE_EXPORT const DseGraphResult*
dse_graph_result_element(const DseGraphResult* result,
                         size_t index);

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
