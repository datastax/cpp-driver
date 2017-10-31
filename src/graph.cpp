/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "graph.hpp"

#include "wkt.hpp"

#include <map_iterator.hpp>
#include <request_handler.hpp>
#include <serialization.hpp> // cass::encode_int64()
#include <session.hpp>
#include <string_ref.hpp>
#include <query_request.hpp>
#include <value.hpp>
#include <logger.hpp>

#include <assert.h>
#include <iomanip>
#include <sstream>

using cass::Logger;

namespace {

static const DseGraphResult* find_member(const DseGraphResult* result,
                                         const char* name, size_t expected_index) {
  if (expected_index < result->MemberCount()) {
    const cass::json::Value::Member& member = result->MemberBegin()[expected_index];
    if (member.name == name) {
      return DseGraphResult::to(&member.value);
    }
  }
  cass::json::Value::ConstMemberIterator i = result->FindMember(name);
  return i != result->MemberEnd() ? DseGraphResult::to(&i->value) : NULL;
}

struct GraphAnalyticsRequest {
  GraphAnalyticsRequest(cass::Session* session,
                        cass::ResponseFuture* future,
                        const cass::Statement* statement)
    : session(session)
    , future(future)
    , statement(statement) { }

  cass::Session* session;
  cass::SharedRefPtr<cass::ResponseFuture> future;
  cass::SharedRefPtr<const cass::Statement> statement;
};

void graph_analytics_callback(CassFuture* future, void* data) {
  GraphAnalyticsRequest* request = static_cast<GraphAnalyticsRequest*>(data);

  cass::ResponseFuture* response_future = static_cast<cass::ResponseFuture*>(future->from());
  cass::Future::Error* error = response_future->error();
  if (error != NULL) {
    request->future->set_error_with_address(response_future->address(),
                                            error->code, error->message);
  } else  {
    request->future->set_response(response_future->address(),
                                  response_future->response());
  }
  cass::Memory::deallocate(request);
}

void graph_analytics_lookup_callback(CassFuture* future, void* data) {
  GraphAnalyticsRequest* request = static_cast<GraphAnalyticsRequest*>(data);

  cass::ResponseFuture* response_future = static_cast<cass::ResponseFuture*>(future->from());
  cass::ResultResponse* response = static_cast<cass::ResultResponse*>(response_future->response().get());

  cass::Address preferred_address;
  bool use_preferred_address = response->row_count() > 0;

  if (use_preferred_address) {
    const cass::Value* value = response->first_row().get_by_name("result");
    if (value == NULL ||
        !value->is_map() ||
        !cass::is_string_type(value->primary_value_type()) ||
        !cass::is_string_type(value->secondary_value_type())) {
      LOG_ERROR("The 'result' column is either not present or is not the "
                "expected type 'map<text, text>' in analytics master lookup "
                "response.");
      use_preferred_address = false;
    } else {
      cass::StringRef location;
      cass::MapIterator iterator(value);
      while(iterator.next()) {
        if (iterator.key()->to_string_ref() == "location") {
          location = iterator.value()->to_string_ref();
          location = location.substr(0, location.find(":"));
        }
      }

      if (!cass::Address::from_string(location.to_string(),
                                      request->session->config().port(),
                                      &preferred_address)) {
        LOG_ERROR("The 'location' map entry's value is not a valid address in "
                  "analytics master lookup response.");
        use_preferred_address = false;
      }
    }
  }

  if (!use_preferred_address) {
    LOG_INFO("Unable to determine the master node's address for the "
             "analytics query. Using a coordinator node to route request...");
  }

  cass::Future::Ptr request_future(
        request->session->execute(request->statement,
                                  use_preferred_address ? &preferred_address : NULL));
  request_future->set_callback(graph_analytics_callback, data);
}

} // namepsace

extern "C" {

CassFuture* cass_session_execute_dse_graph(CassSession* session,
                                           const DseGraphStatement* statement) {
  if (statement->graph_source() == DSE_GRAPH_ANALYTICS_SOURCE) {
    cass::ResponseFuture* future = cass::Memory::allocate<cass::ResponseFuture>();

    cass::Future::Ptr request_future(
          session->execute(
            cass::Request::ConstPtr(
              cass::Memory::allocate<cass::QueryRequest>(DSE_LOOKUP_ANALYTICS_GRAPH_SERVER))));
    request_future->set_callback(graph_analytics_lookup_callback,
                                 cass::Memory::allocate<GraphAnalyticsRequest>(session,
                                                                               future,
                                                                               statement->wrapped()->from()));

    future->inc_ref();
    return CassFuture::to(future);
  } else {
    return cass_session_execute(session, statement->wrapped());
  }
}

DseGraphResultSet* cass_future_get_dse_graph_resultset(CassFuture* future) {
  const CassResult* result = cass_future_get_result(future);
  if (result == NULL) return NULL;
  return DseGraphResultSet::to(cass::Memory::allocate<dse::GraphResultSet>(result));
}

DseGraphOptions* dse_graph_options_new() {
  return DseGraphOptions::to(cass::Memory::allocate<dse::GraphOptions>());
}

DseGraphOptions* dse_graph_options_new_from_existing(const DseGraphOptions* options) {
  return DseGraphOptions::to(options->clone());
}

void dse_graph_options_free(DseGraphOptions* options) {
  cass::Memory::deallocate(options->from());
}

CassError dse_graph_options_set_graph_language(DseGraphOptions* options,
                                               const char* language) {
  options->set_graph_language(language);
  return CASS_OK;
}

CassError dse_graph_options_set_graph_language_n(DseGraphOptions* options,
                                                 const char* language, size_t language_length) {
  options->set_graph_language(cass::String(language, language_length));
  return CASS_OK;
}

CassError dse_graph_options_set_graph_source(DseGraphOptions* options,
                                             const char* source) {
  options->set_graph_source(source);
  return CASS_OK;
}

CassError dse_graph_options_set_graph_source_n(DseGraphOptions* options,
                                               const char* source, size_t source_length) {
  options->set_graph_source(cass::String(source, source_length));
  return CASS_OK;
}

CassError dse_graph_options_set_graph_name(DseGraphOptions* options,
                                           const char* name) {
  options->set_graph_name(name);
  return CASS_OK;
}

CassError dse_graph_options_set_graph_name_n(DseGraphOptions* options,
                                             const char* name, size_t name_length) {
  options->set_graph_name(cass::String(name, name_length));
  return CASS_OK;
}

CassError dse_graph_options_set_read_consistency(DseGraphOptions* options,
                                                 CassConsistency consistency) {
  options->set_graph_read_consistency(consistency);
  return CASS_OK;
}

CassError dse_graph_options_set_write_consistency(DseGraphOptions* options,
                                                  CassConsistency consistency) {
  options->set_graph_write_consistency(consistency);
  return CASS_OK;
}

CassError dse_graph_options_set_request_timeout(DseGraphOptions* options,
                                                cass_int64_t timeout_ms) {
  if (timeout_ms < 0) return CASS_ERROR_LIB_BAD_PARAMS;
  options->set_request_timeout_ms(timeout_ms);
  return CASS_OK;
}

DseGraphStatement* dse_graph_statement_new(const char* query,
                                           const DseGraphOptions* options) {
  return dse_graph_statement_new_n(query, SAFE_STRLEN(query),
                                   options);
}

DseGraphStatement* dse_graph_statement_new_n(const char* query,
                                             size_t query_length,
                                             const DseGraphOptions* options) {
  return DseGraphStatement::to(cass::Memory::allocate<dse::GraphStatement>(query, query_length,
                                                                           options));
}

void dse_graph_statement_free(DseGraphStatement* statement) {
  cass::Memory::deallocate(statement->from());
}

CassError dse_graph_statement_bind_values(DseGraphStatement* statement,
                                          const DseGraphObject* values) {
  if (values != NULL && !values->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  return statement->bind_values(values);
}

CassError dse_graph_statement_set_timestamp(DseGraphStatement* statement,
                                            cass_int64_t timestamp) {
  return statement->set_timestamp(timestamp);
}

DseGraphObject* dse_graph_object_new() {
  return DseGraphObject::to(cass::Memory::allocate<dse::GraphObject>());
}

void dse_graph_object_free(DseGraphObject* object) {
  cass::Memory::deallocate(object->from());
}

void dse_graph_object_reset(DseGraphObject* object) {
  object->reset();
}

void dse_graph_object_finish(DseGraphObject* object) {
  object->finish();
}

CassError dse_graph_object_add_null(DseGraphObject* object,
                                    const char* name) {
  return dse_graph_object_add_null_n(object,
                                     name, SAFE_STRLEN(name));
}

CassError dse_graph_object_add_null_n(DseGraphObject* object,
                                      const char* name,
                                      size_t name_length) {
  if (object->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  object->add_key(name, name_length);
  object->add_null();
  return CASS_OK;
}

CassError dse_graph_object_add_bool(DseGraphObject* object,
                                     const char* name,
                                     cass_bool_t value) {
  return dse_graph_object_add_bool_n(object,
                                      name, SAFE_STRLEN(name),
                                      value);
}

CassError dse_graph_object_add_bool_n(DseGraphObject* object,
                                      const char* name,
                                      size_t name_length,
                                      cass_bool_t value) {
  if (object->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  object->add_key(name, name_length);
  object->add_bool(value);
  return CASS_OK;
}

CassError dse_graph_object_add_int32(DseGraphObject* object,
                                     const char* name,
                                     cass_int32_t value) {
  return dse_graph_object_add_int32_n(object,
                                      name, SAFE_STRLEN(name),
                                      value);
}

CassError dse_graph_object_add_int32_n(DseGraphObject* object,
                                       const char* name,
                                       size_t name_length,
                                       cass_int32_t value) {
  if (object->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  object->add_key(name, name_length);
  object->add_int32(value);
  return CASS_OK;
}

CassError dse_graph_object_add_int64(DseGraphObject* object,
                                     const char* name,
                                     cass_int64_t value) {
  return dse_graph_object_add_int64_n(object,
                                      name, SAFE_STRLEN(name),
                                      value);
}

CassError dse_graph_object_add_int64_n(DseGraphObject* object,
                                       const char* name,
                                       size_t name_length,
                                       cass_int64_t value) {
  if (object->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  object->add_key(name, name_length);
  object->add_int64(value);
  return CASS_OK;
}

CassError dse_graph_object_add_double(DseGraphObject* object,
                                      const char* name,
                                      cass_double_t value) {
  return dse_graph_object_add_double_n(object,
                                       name, SAFE_STRLEN(name),
                                       value);
}

CassError dse_graph_object_add_double_n(DseGraphObject* object,
                                        const char* name,
                                        size_t name_length,
                                        cass_double_t value) {
  if (object->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  object->add_key(name, name_length);
  object->add_double(value);
  return CASS_OK;
}

CassError dse_graph_object_add_string(DseGraphObject* object,
                                      const char* name,
                                      const char* value) {
  return dse_graph_object_add_string_n(object,
                                       name, SAFE_STRLEN(name),
                                       value, SAFE_STRLEN(value));
}

CassError dse_graph_object_add_string_n(DseGraphObject* object,
                                        const char* name, size_t name_length,
                                        const char* value, size_t value_length) {
  if (object->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  object->add_key(name, name_length);
  object->add_string(value, value_length);
  return CASS_OK;
}

CassError dse_graph_object_add_object(DseGraphObject* object,
                                      const char* name,
                                      const DseGraphObject* value) {
  return dse_graph_object_add_object_n(object,
                                       name, SAFE_STRLEN(name),
                                       value);
}

CassError dse_graph_object_add_object_n(DseGraphObject* object,
                                        const char* name,
                                        size_t name_length,
                                        const DseGraphObject* value) {
  if (object->is_complete() || !value->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  object->add_key(name, name_length);
  object->add_writer(value, rapidjson::kObjectType);
  return CASS_OK;
}

CassError dse_graph_object_add_array(DseGraphObject* object,
                                     const char* name,
                                     const DseGraphArray* value) {
  return dse_graph_object_add_array_n(object,
                                      name, SAFE_STRLEN(name),
                                      value);
}

CassError dse_graph_object_add_array_n(DseGraphObject* object,
                                       const char* name,
                                       size_t name_length,
                                       const DseGraphArray* value) {
  if (object->is_complete() || !value->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  object->add_key(name, name_length);
  object->add_writer(value, rapidjson::kArrayType);
  return CASS_OK;
}

CassError dse_graph_object_add_point(DseGraphObject* object,
                                     const char* name,
                                     cass_double_t x, cass_double_t y) {
  return dse_graph_object_add_point_n(object,
                                      name, SAFE_STRLEN(name),
                                      x, y);
}

CassError dse_graph_object_add_point_n(DseGraphObject* object,
                                       const char* name,
                                       size_t name_length,
                                       cass_double_t x, cass_double_t y) {
  if (object->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  object->add_key(name, name_length);
  object->add_point(x, y);
  return CASS_OK;
}

CassError dse_graph_object_add_line_string(DseGraphObject* object,
                                           const char* name,
                                           const DseLineString* value) {
  return dse_graph_object_add_line_string_n(object,
                                            name, SAFE_STRLEN(name),
                                            value);
}

CassError dse_graph_object_add_line_string_n(DseGraphObject* object,
                                             const char* name,
                                             size_t name_length,
                                             const DseLineString* value) {
  if (object->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  object->add_key(name, name_length);
  object->add_line_string(value->from());
  return CASS_OK;
}

CassError dse_graph_object_add_polygon(DseGraphObject* object,
                                       const char* name,
                                       const DsePolygon* value) {
  return dse_graph_object_add_polygon_n(object,
                                        name, SAFE_STRLEN(name),
                                        value);
}

CassError dse_graph_object_add_polygon_n(DseGraphObject* object,
                                         const char* name,
                                         size_t name_length,
                                         const DsePolygon* value) {
  if (object->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  object->add_key(name, name_length);
  object->add_polygon(value);
  return CASS_OK;
}

DseGraphArray* dse_graph_array_new() {
  return DseGraphArray::to(cass::Memory::allocate<DseGraphArray>());
}

void dse_graph_array_free(DseGraphArray* array) {
  cass::Memory::deallocate(array->from());
}

void dse_graph_array_reset(DseGraphArray* array) {
  array->reset();
}

void dse_graph_array_finish(DseGraphArray* array) {
  array->finish();
}

CassError dse_graph_array_add_null(DseGraphArray* array) {
  if (array->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  array->add_null();
  return CASS_OK;
}

CassError dse_graph_array_add_bool(DseGraphArray* array,
                                   cass_bool_t value) {
  if (array->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  array->add_bool(value);
  return CASS_OK;
}

CassError dse_graph_array_add_int32(DseGraphArray* array,
                                    cass_int32_t value) {
  if (array->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  array->add_int32(value);
  return CASS_OK;
}

CassError dse_graph_array_add_int64(DseGraphArray* array,
                                    cass_int64_t value) {
  if (array->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  array->add_int64(value);
  return CASS_OK;
}

CassError dse_graph_array_add_double(DseGraphArray* array,
                                     cass_double_t value) {
  if (array->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  array->add_double(value);
  return CASS_OK;
}

CassError dse_graph_array_add_string(DseGraphArray* array,
                                     const char* value) {
  return dse_graph_array_add_string_n(array, value, SAFE_STRLEN(value));
}

CassError dse_graph_array_add_string_n(DseGraphArray* array,
                                       const char* value,
                                       size_t value_length) {
  if (array->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  array->add_string(value, value_length);
  return CASS_OK;
}

CassError dse_graph_array_add_object(DseGraphArray* array,
                                     const DseGraphObject* value) {
  if (array->is_complete() || !value->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  array->add_writer(value, rapidjson::kObjectType);
  return CASS_OK;
}

CassError dse_graph_array_add_array(DseGraphArray* array,
                                    const DseGraphArray* value) {
  if (array->is_complete() || !value->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  array->add_writer(value, rapidjson::kArrayType);
  return CASS_OK;
}

CassError dse_graph_array_add_point(DseGraphArray* array,
                                    cass_double_t x, cass_double_t y) {
  if (array->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  array->add_point(x, y);
  return CASS_OK;
}

CassError dse_graph_array_add_line_string(DseGraphArray* array,
                                          const DseLineString* value) {
  if (array->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  array->add_line_string(value->from());
  return CASS_OK;
}

CassError dse_graph_array_add_polygon(DseGraphArray* array,
                                      const DsePolygon* value) {
  if (array->is_complete()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  array->add_polygon(value->from());
  return CASS_OK;
}

void dse_graph_resultset_free(DseGraphResultSet* resultset) {
  cass::Memory::deallocate(resultset->from());
}

size_t dse_graph_resultset_count(DseGraphResultSet* resultset) {
  return resultset->count();
}

const DseGraphResult* dse_graph_resultset_next(DseGraphResultSet* resultset) {
  return DseGraphResult::to(resultset->next());
}

DseGraphResultType dse_graph_result_type(const DseGraphResult* result) {
  switch (result->GetType()) {
    case rapidjson::kNullType: return DSE_GRAPH_RESULT_TYPE_NULL;
    case rapidjson::kFalseType: // Intentional fallthrough
    case rapidjson::kTrueType: return DSE_GRAPH_RESULT_TYPE_BOOL;
    case rapidjson::kNumberType: return DSE_GRAPH_RESULT_TYPE_NUMBER;
    case rapidjson::kStringType: return DSE_GRAPH_RESULT_TYPE_STRING;
    case rapidjson::kObjectType: return DSE_GRAPH_RESULT_TYPE_OBJECT;
    case rapidjson::kArrayType: return DSE_GRAPH_RESULT_TYPE_ARRAY;
  }

  // Path should never be executed
  assert(false);
  return DSE_GRAPH_RESULT_TYPE_NULL;
}

cass_bool_t dse_graph_result_is_null(const DseGraphResult* result) {
  return result->IsNull() ? cass_true : cass_false;
}

cass_bool_t dse_graph_result_is_bool(const DseGraphResult* result) {
  return result->IsBool() ? cass_true : cass_false;
}

cass_bool_t dse_graph_result_is_int32(const DseGraphResult* result) {
  return result->IsInt() ? cass_true : cass_false;
}

cass_bool_t dse_graph_result_is_int64(const DseGraphResult* result) {
  return result->IsInt64() ? cass_true : cass_false;
}

cass_bool_t dse_graph_result_is_double(const DseGraphResult* result) {
  return result->IsDouble() ? cass_true : cass_false;
}

cass_bool_t dse_graph_result_is_string(const DseGraphResult* result) {
  return result->IsString() ? cass_true : cass_false;
}

cass_bool_t dse_graph_result_is_object(const DseGraphResult* result) {
  return result->IsObject() ? cass_true : cass_false;
}

cass_bool_t dse_graph_result_is_array(const DseGraphResult* result) {
  return result->IsArray() ? cass_true : cass_false;
}

cass_bool_t dse_graph_result_get_bool(const DseGraphResult* result) {
  return result->GetBool() ? cass_true : cass_false;
}

cass_int32_t dse_graph_result_get_int32(const DseGraphResult* result) {
  return result->GetInt();
}

cass_int64_t dse_graph_result_get_int64(const DseGraphResult* result) {
  return result->GetInt64();
}

cass_double_t dse_graph_result_get_double(const DseGraphResult* result) {
  return result->GetDouble();
}

const char* dse_graph_result_get_string(const DseGraphResult* result,
                                        size_t* length) {
  if (length != NULL) {
    *length = result->GetStringLength();
  }
  return result->GetString();
}

#define CHECK_FIND_MEMBER(dest, name, expected_index) do { \
  const DseGraphResult* src = find_member(result, name, expected_index); \
  if (src != NULL) { \
  (dest) = src; \
} else { \
  return CASS_ERROR_LIB_BAD_PARAMS; \
} \
} while (0)

CassError dse_graph_result_as_edge(const DseGraphResult* result,
                                   DseGraphEdgeResult* edge) {
  if (!result->IsObject()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }

  CHECK_FIND_MEMBER(edge->id,               "id",         0);
  CHECK_FIND_MEMBER(edge->label,            "label",      1);
  CHECK_FIND_MEMBER(edge->type,             "type",       2);
  CHECK_FIND_MEMBER(edge->in_vertex_label,  "inVLabel",   3);
  CHECK_FIND_MEMBER(edge->out_vertex_label, "outVLabel",  4);
  CHECK_FIND_MEMBER(edge->in_vertex,        "inV",        5);
  CHECK_FIND_MEMBER(edge->out_vertex,       "outV",       6);
  CHECK_FIND_MEMBER(edge->properties,       "properties", 7);

  return CASS_OK;
}

CassError dse_graph_result_as_vertex(const DseGraphResult* result,
                                      DseGraphVertexResult* vertex) {
  if (!result->IsObject()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }

  CHECK_FIND_MEMBER(vertex->id,         "id",         0);
  CHECK_FIND_MEMBER(vertex->label,      "label",      1);
  CHECK_FIND_MEMBER(vertex->type,       "type",       2);
  CHECK_FIND_MEMBER(vertex->properties, "properties", 3);

  return CASS_OK;
}

CassError dse_graph_result_as_path(const DseGraphResult* result,
                                    DseGraphPathResult* path) {
  if (!result->IsObject()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }

  CHECK_FIND_MEMBER(path->labels,  "labels",  0);
  CHECK_FIND_MEMBER(path->objects, "objects", 1);

  return CASS_OK;
}

#undef CHECK_FIND_MEMBER

size_t dse_graph_result_member_count(const DseGraphResult* result) {
  return result->MemberCount();
}

const char* dse_graph_result_member_key(const DseGraphResult* result,
                                        size_t index,
                                        size_t* length) {
 const cass::json::Value& key = result->MemberBegin()[index].name;
 if (length != NULL) {
   *length = key.GetStringLength();
 }
 return key.GetString();
}

const DseGraphResult* dse_graph_result_member_value(const DseGraphResult* result,
                                                    size_t index) {
  return DseGraphResult::to(&result->MemberBegin()[index].value);
}

size_t dse_graph_result_element_count(const DseGraphResult* result) {
  return result->Size();
}

const DseGraphResult* dse_graph_result_element(const DseGraphResult* result,
                                               size_t index) {
  return DseGraphResult::to(&result->Begin()[index]);
}

CassError dse_graph_result_as_point(const DseGraphResult* result,
                                    cass_double_t* x, cass_double_t* y) {
  if (!result->IsString()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }

  return dse_point_from_wkt_n(result->GetString(), result->GetStringLength(), x, y);
}

CassError dse_graph_result_as_line_string(const DseGraphResult* result,
                                          DseLineStringIterator* line_string) {
  if (!result->IsString()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  return line_string->reset_text(result->GetString(), result->GetStringLength());
}

CassError dse_graph_result_as_polygon(const DseGraphResult* result,
                                      DsePolygonIterator* polygon) {
  if (!result->IsString()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  return polygon->reset_text(result->GetString(), result->GetStringLength());
}

} // extern "C"

namespace dse {

GraphOptions* GraphOptions::clone() const {
  GraphOptions* options = cass::Memory::allocate<GraphOptions>();

  if (!graph_language_.empty()) {
    options->set_graph_language(graph_language_);
  }

  if (!graph_source_.empty()) {
    options->set_graph_source(graph_source_);
  }

  if (!graph_name_.empty()) {
    options->set_graph_name(graph_name_);
  }

  if (read_consistency_ != CASS_CONSISTENCY_UNKNOWN) {
    options->set_graph_read_consistency(read_consistency_);
  }

  if (write_consistency_ != CASS_CONSISTENCY_UNKNOWN) {
    options->set_graph_write_consistency(write_consistency_);
  }

  options->request_timeout_ms_ = request_timeout_ms_;

  return options;
}

void GraphOptions::set_request_timeout_ms(int64_t timeout_ms) {
  request_timeout_ms_ = timeout_ms;
  if (timeout_ms > 0) {
    cass::String value(sizeof(timeout_ms), 0);
    cass::encode_int64(&value[0], timeout_ms);
    cass_custom_payload_set_n(payload_,
                              DSE_GRAPH_REQUEST_TIMEOUT,
                              sizeof(DSE_GRAPH_REQUEST_TIMEOUT) - 1,
                              reinterpret_cast<const cass_byte_t *>(value.data()),
                              value.size());
  } else {
    cass_custom_payload_remove_n(payload_,
                                 DSE_GRAPH_REQUEST_TIMEOUT,
                                 sizeof(DSE_GRAPH_REQUEST_TIMEOUT) - 1);
  }
}

const GraphResult* GraphResultSet::next() {
  if (cass_iterator_next(rows_)) {
    const CassRow* row = cass_iterator_get_row(rows_);
    if (row == NULL) return NULL;

    const CassValue* value = cass_row_get_column_by_name(row, "gremlin");
    if (value == NULL) return NULL;

    const char* json;
    size_t length;
    cass_value_get_string(value, &json, &length);

    // A copy is needed to make this null-terminated, but it
    // allows for insitu parsing.
    json_.assign(json, length);
    if (document_.ParseInsitu(&json_[0]).HasParseError()) {
      return NULL;
    }

    cass::json::Value::ConstMemberIterator i = document_.FindMember("result");
    return i != document_.MemberEnd() ? &i->value : NULL;
  }
  return NULL;
}

void GraphWriter::add_point(cass_double_t x, cass_double_t y) {
  cass::OStringStream ss;
  ss.precision(WKT_MAX_DIGITS);
  ss << "POINT (" << x << " " << y << ")";
  String(ss.str().c_str());
}

} // namespace dse
