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

#include "prepared.hpp"

#include "execute_request.hpp"
#include "external.hpp"
#include "logger.hpp"

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

extern "C" {

void cass_prepared_free(const CassPrepared* prepared) { prepared->dec_ref(); }

CassStatement* cass_prepared_bind(const CassPrepared* prepared) {
  ExecuteRequest* execute = new ExecuteRequest(prepared);
  execute->inc_ref();
  return CassStatement::to(execute);
}

CassError cass_prepared_parameter_name(const CassPrepared* prepared, size_t index,
                                       const char** name, size_t* name_length) {
  const SharedRefPtr<ResultMetadata>& metadata(prepared->result()->metadata());
  if (index >= metadata->column_count()) {
    return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
  }
  const ColumnDefinition def = metadata->get_column_definition(index);
  *name = def.name.data();
  *name_length = def.name.size();
  return CASS_OK;
}

const CassDataType* cass_prepared_parameter_data_type(const CassPrepared* prepared, size_t index) {
  const SharedRefPtr<ResultMetadata>& metadata(prepared->result()->metadata());
  if (index >= metadata->column_count()) {
    return NULL;
  }
  return CassDataType::to(metadata->get_column_definition(index).data_type.get());
}

const CassDataType* cass_prepared_parameter_data_type_by_name(const CassPrepared* prepared,
                                                              const char* name) {
  return cass_prepared_parameter_data_type_by_name_n(prepared, name, SAFE_STRLEN(name));
}

const CassDataType* cass_prepared_parameter_data_type_by_name_n(const CassPrepared* prepared,
                                                                const char* name,
                                                                size_t name_length) {

  const SharedRefPtr<ResultMetadata>& metadata(prepared->result()->metadata());

  IndexVec indices;
  if (metadata->get_indices(StringRef(name, name_length), &indices) == 0) {
    return NULL;
  }
  return CassDataType::to(metadata->get_column_definition(indices[0]).data_type.get());
}

} // extern "C"

Prepared::Prepared(const ResultResponse::Ptr& result,
                   const PrepareRequest::ConstPtr& prepare_request,
                   const Metadata::SchemaSnapshot& schema_metadata)
    : result_(result)
    , id_(result->prepared_id().to_string())
    , query_(prepare_request->query())
    , keyspace_(prepare_request->keyspace())
    , request_settings_(prepare_request->settings()) {
  assert(result->protocol_version() > 0 && "The protocol version should be set");
  if (result->protocol_version() >= CASS_PROTOCOL_VERSION_V4) {
    key_indices_ = result->pk_indices();
  } else {
    const KeyspaceMetadata* keyspace = schema_metadata.get_keyspace(result->keyspace().to_string());
    if (keyspace != NULL) {
      const TableMetadata* table = keyspace->get_table(result->table().to_string());
      if (table != NULL) {
        const ColumnMetadata::Vec& partition_key = table->partition_key();
        IndexVec indices;
        for (ColumnMetadata::Vec::const_iterator i = partition_key.begin(),
                                                 end = partition_key.end();
             i != end; ++i) {
          const ColumnMetadata::Ptr& column(*i);
          if (column && result->metadata()->get_indices(column->name(), &indices) > 0) {
            key_indices_.push_back(indices[0]);
          } else {
            LOG_WARN("Unable to find key column '%s' in prepared query",
                     column ? column->name().c_str() : "<null>");
            key_indices_.clear();
            break;
          }
        }
      }
    }
  }
}
