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

#include <rapidjson/document.h>

#include "buffer.hpp"
#include "row.hpp"
#include "schema_metadata.hpp"
#include "value.hpp"

//#include "types.hpp"

//extern "C" {

//cass_bool_t cass_get_something() {
//}

//} // extern "C"

namespace cass {

KeyspaceMetadata::KeyspaceMetadata(const boost::string_ref& name)
    : name_(name.begin(), name.end()) {}

KeyspaceMetadata* KeyspaceMetadata::from_schema_row(const Row* schema_row) {
  KeyspaceMetadata* ksm = NULL;
  const Value* v = schema_row->get_by_name("keyspace_name");
  if (v != NULL && !v->is_null()) {
    ksm = new KeyspaceMetadata(boost::string_ref(v->buffer().data(), v->buffer().size()));

    v = schema_row->get_by_name("durable_writes");
    if (v != NULL && !v->is_null()) ksm->durable_writes_ = static_cast<bool>(v->buffer().data()[0]);

    v = schema_row->get_by_name("strategy_class");
    if (v != NULL && !v->is_null()) ksm->strategy_class_.assign(v->buffer().data(), v->buffer().size());

    v = schema_row->get_by_name("strategy_options");
    if (v != NULL && !v->is_null()) ksm->options_from_json(v->buffer());
  }
  return ksm;
}

void KeyspaceMetadata::options_from_json(const BufferPiece& options_json) {
  char* buffer = new char[options_json.size() + 1];
  memcpy(buffer, options_json.data(), options_json.size());
  buffer[options_json.size()] = '\0';

  rapidjson::Document d;
  d.ParseInsitu(buffer);
  if (!d.HasParseError()) {
    for (rapidjson::Value::ConstMemberIterator i = d.MemberBegin(); i != d.MemberEnd(); ++i) {
      if (i->value.IsString()) {// options are held as string/string inside C*
        strategy_options_[i->name.GetString()] = i->value.GetString();
      }
    }
  } else {
    //TODO: log to global logging
  }
  delete[] buffer;
}

ColumnFamilyMetadata* ColumnFamilyMetadata::from_schema_row(const Row* schema_row) {
  //keyspace_name | columnfamily_name | bloom_filter_fp_chance | caching   | column_aliases | comment | compaction_strategy_class                                       | compaction_strategy_options | comparator                                                                              | compression_parameters                                                   | default_time_to_live | default_validator                         | dropped_columns | gc_grace_seconds | index_interval | key_aliases | key_validator                             | local_read_repair_chance | max_compaction_threshold | memtable_flush_period_in_ms | min_compaction_threshold | populate_io_cache_on_flush | read_repair_chance | replicate_on_write | speculative_retry | subcomparator | type     | value_alias
  return NULL;
}

ColumnMetadata* ColumnMetadata::from_schema_row(const Row* schema_row) {
  // keyspace_name | columnfamily_name | column_name | component_index | index_name | index_options | index_type | type          | validator
  return NULL;
}

} // namespace cass


