/*
  Copyright (c) 2014-2015 DataStax

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

#include "cluster_metadata.hpp"

namespace cass {

ClusterMetadata::ClusterMetadata() {
  uv_mutex_init(&schema_mutex_);
}

ClusterMetadata::~ClusterMetadata() {
  uv_mutex_destroy(&schema_mutex_);
}

void ClusterMetadata::clear() {
  schema_.clear();
  token_map_.clear();
}

void ClusterMetadata::update_keyspaces(ResultResponse* result) {
  Schema::KeyspacePointerMap keyspaces;
  {
    ScopedMutex l(&schema_mutex_);
    keyspaces = schema_.update_keyspaces(result);
  }
  for (Schema::KeyspacePointerMap::const_iterator i = keyspaces.begin(); i != keyspaces.end(); ++i) {
    token_map_.update_keyspace(i->first, *i->second);
  }
}

void ClusterMetadata::update_tables(ResultResponse* table_result, ResultResponse* col_result) {
  ScopedMutex l(&schema_mutex_);
  schema_.update_tables(table_result, col_result);
}

void ClusterMetadata::drop_keyspace(const std::string& keyspace_name) {
  schema_.drop_keyspace(keyspace_name);
  token_map_.drop_keyspace(keyspace_name);
}

Schema* ClusterMetadata::copy_schema() const {
  ScopedMutex l(&schema_mutex_);
  return new Schema(schema_);
}

} // namespace cass
