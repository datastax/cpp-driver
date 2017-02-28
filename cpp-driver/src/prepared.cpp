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

#include "prepared.hpp"

#include "execute_request.hpp"
#include "logger.hpp"
#include "external.hpp"

extern "C" {

void cass_prepared_free(const CassPrepared* prepared) {
  prepared->dec_ref();
}

CassStatement* cass_prepared_bind(const CassPrepared* prepared) {
  cass::ExecuteRequest* execute = new cass::ExecuteRequest(prepared);
  execute->inc_ref();
  return CassStatement::to(execute);
}

CassError cass_prepared_parameter_name(const CassPrepared* prepared,
                                       size_t index,
                                       const char** name,
                                       size_t* name_length) {
  const cass::SharedRefPtr<cass::ResultMetadata>& metadata(prepared->result()->metadata());
  if (index >= metadata->column_count()) {
    return CASS_ERROR_LIB_INDEX_OUT_OF_BOUNDS;
  }
  const cass::ColumnDefinition def = metadata->get_column_definition(index);
  *name = def.name.data();
  *name_length = def.name.size();
  return CASS_OK;
}

const CassDataType* cass_prepared_parameter_data_type(const CassPrepared* prepared,
                                                      size_t index) {
  const cass::SharedRefPtr<cass::ResultMetadata>& metadata(prepared->result()->metadata());
  if (index >= metadata->column_count()) {
    return NULL;
  }
  return CassDataType::to(metadata->get_column_definition(index).data_type.get());
}

const CassDataType* cass_prepared_parameter_data_type_by_name(const CassPrepared* prepared,
                                                              const char* name) {
  return cass_prepared_parameter_data_type_by_name_n(prepared,
                                                     name, strlen(name));
}

const CassDataType* cass_prepared_parameter_data_type_by_name_n(const CassPrepared* prepared,
                                                                const char* name,
                                                                size_t name_length) {

  const cass::SharedRefPtr<cass::ResultMetadata>& metadata(prepared->result()->metadata());

  cass::IndexVec indices;
  if (metadata->get_indices(cass::StringRef(name, name_length), &indices) == 0) {
    return NULL;
  }
  return CassDataType::to(metadata->get_column_definition(indices[0]).data_type.get());
}

} // extern "C"

namespace cass {

Prepared::Prepared(const ResultResponse::Ptr& result,
                   const std::string& statement,
                   const Metadata::SchemaSnapshot& schema_metadata)
  : result_(result)
  , id_(result->prepared().to_string())
  , statement_(statement) {
  if (schema_metadata.protocol_version() >= 4) {
    key_indices_ = result->pk_indices();
  } else {
    const KeyspaceMetadata* keyspace = schema_metadata.get_keyspace(result->keyspace().to_string());
    if (keyspace != NULL) {
      const TableMetadata* table = keyspace->get_table(result->table().to_string());
      if (table != NULL) {
        const ColumnMetadata::Vec& partition_key = table->partition_key();
        IndexVec indices;
        for (ColumnMetadata::Vec::const_iterator i = partition_key.begin(),
             end = partition_key.end(); i != end; ++i) {
          if (result->metadata()->get_indices(StringRef((*i)->name()), &indices) > 0) {
            key_indices_.push_back(indices[0]);
          } else {
            LOG_WARN("Unable to find key column '%s' in prepared query", (*i)->name().c_str());
            key_indices_.clear();
            break;
          }
        }
      }
    }
  }
}

} // namespace cass
