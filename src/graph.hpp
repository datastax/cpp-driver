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

#ifndef __DSE_GRAPH_HPP_INCLUDED__
#define __DSE_GRAPH_HPP_INCLUDED__

#include "dse.h"

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

#include "scoped_ptr.hpp"

#include <assert.h>
#include <string>
#include <vector>

#define DSE_GRAPH_OPTION_LANGUAGE_KEY "graph-language"
#define DSE_GRAPH_OPTION_SOURCE_KEY   "graph-source"
#define DSE_GRAPH_OPTION_NAME_KEY     "graph-name"

#define DSE_GRAPH_DEFAULT_LANGUAGE "gremlin-groovy"
#define DSE_GRAPH_DEFAULT_SOURCE "default"


namespace dse {

class GraphStatement;

class GraphOptions {
public:
  GraphOptions()
    : graph_language_(DSE_GRAPH_DEFAULT_LANGUAGE)
    , graph_source_(DSE_GRAPH_DEFAULT_SOURCE) { }

  void set_graph_language(const std::string& graph_language) {
    graph_language_ = graph_language;
  }

  void set_graph_source(const std::string& graph_source) {
    graph_source_ = graph_source;
  }

  void set_graph_name(const std::string& graph_name) {
    graph_name_ = graph_name;
  }

  CassCustomPayload* build_payload(bool is_system_query) const {
    CassCustomPayload* payload = cass_custom_payload_new();
    cass_custom_payload_set_n(payload,
                              DSE_GRAPH_OPTION_LANGUAGE_KEY, sizeof(DSE_GRAPH_OPTION_LANGUAGE_KEY) - 1,
                              reinterpret_cast<const cass_byte_t*>(graph_language_.data()), graph_language_.size());
    cass_custom_payload_set_n(payload,
                              DSE_GRAPH_OPTION_SOURCE_KEY, sizeof(DSE_GRAPH_OPTION_SOURCE_KEY) - 1,
                              reinterpret_cast<const cass_byte_t*>(graph_source_.data()), graph_source_.size());
    if (!graph_name_.empty() && !is_system_query) {
      cass_custom_payload_set_n(payload,
                                DSE_GRAPH_OPTION_NAME_KEY, sizeof(DSE_GRAPH_OPTION_NAME_KEY) - 1,
                                reinterpret_cast<const cass_byte_t*>(graph_name_.data()), graph_name_.size());
    }
    return payload;
  }

private:
  std::string graph_language_;
  std::string graph_source_;
  std::string graph_name_;
};

class GraphWriter : private rapidjson::Writer<rapidjson::StringBuffer> {
public:
  GraphWriter()
    : rapidjson::Writer<rapidjson::StringBuffer>(buffer_) { }

  const char* data() const { return buffer_.GetString(); }
  size_t length() const { return buffer_.GetSize(); }

  bool is_complete() const { return IsComplete(); }

  void add_null() { Null(); }
  void add_bool(cass_bool_t value) { Bool(value); }
  void add_int32(cass_int32_t value) { Int(value); }
  void add_int64(cass_int32_t value) { Int64(value); }
  void add_double(cass_double_t value) { Double(value); }
  void add_string(const char* string, size_t length) { String(string, length); }
  void add_key(const char* key, size_t length) { Key(key, length); }

  bool add_writer(const GraphWriter* writer) {
    size_t length = writer->buffer_.GetSize();
    memcpy(os_->Push(length), writer->buffer_.GetString(), length);
    return true;
  }

  void reset() {
    buffer_.Clear();
    Reset(buffer_);
  }

protected:
  void start_object() { StartObject(); }
  void end_object() { EndObject(); }

  void start_array() { StartArray(); }
  void end_array() { EndArray(); }

private:
  rapidjson::StringBuffer buffer_;
};

class GraphObject : public GraphWriter {
public:
  GraphObject() {
    start_object();
  }

  void reset() {
    GraphWriter::reset();
    start_object();
  }

  void finish() {
    if (!is_complete()) end_object();
  }
};

class GraphArray : public GraphWriter {
public:
  GraphArray() {
    start_array();
  }

  void reset() {
    GraphWriter::reset();
    start_array();
  }

  void finish() {
    if (!is_complete()) end_array();
  }
};

class GraphStatement {
public:
  GraphStatement(const char* query, size_t length,
                 const GraphOptions* options)
    : query_(query, length)
    , has_parameters_(false)
    , is_system_query_(false)
    , wrapped_(cass_statement_new_n(query, length, 0))
    , payload_(NULL) {
    set_options(options);
  }

  ~GraphStatement() {
    cass_statement_free(wrapped_);
    cass_custom_payload_free(payload_);
  }

  const CassStatement* wrapped() const { return wrapped_; }

  void set_options(const GraphOptions* options) {
    if (payload_ != NULL) cass_custom_payload_free(payload_);
    if (options != NULL) {
      payload_ = options->build_payload(is_system_query_);
    } else {
      GraphOptions default_options;
      payload_ = default_options.build_payload(is_system_query_);
    }
    cass_statement_set_custom_payload(wrapped_, payload_);
  }

  CassError set_parameters(const GraphObject* parameters) {
    if (!has_parameters_) {
      if (wrapped_ != NULL) cass_statement_free(wrapped_);
      wrapped_ = cass_statement_new_n(query_.data(), query_.size(), 1);
      cass_statement_set_custom_payload(wrapped_, payload_);
    }
    return cass_statement_bind_string_n(wrapped_, 0,
                                        parameters->data(),
                                        parameters->length());
  }

private:
  std::string query_;
  bool has_parameters_;
  bool is_system_query_;
  CassStatement* wrapped_;
  CassCustomPayload* payload_;
};

typedef rapidjson::Value GraphResult;

class GraphResultSet {
public:
  GraphResultSet(const CassResult* result)
    : rows_(cass_iterator_from_result(result))
    , result_(result) { }

  ~GraphResultSet() {
    cass_iterator_free(rows_);
    cass_result_free(result_);
  }

  size_t count() const {
    return cass_result_row_count(result_);
  }

  const GraphResult* next();

private:
  rapidjson::Document document;
  std::string json_;
  CassIterator* rows_;
  const CassResult* result_;
};

} // namespace dse

#endif
