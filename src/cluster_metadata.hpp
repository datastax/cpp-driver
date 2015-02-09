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

#ifndef __CASS_CLUSTER_METADATA_HPP_INCLUDED__
#define __CASS_CLUSTER_METADATA_HPP_INCLUDED__

#include "token_map.hpp"
#include "schema_metadata.hpp"

namespace cass {

class ClusterMetadata {
public:
  ClusterMetadata();
  ~ClusterMetadata();

  void clear();
  void update_keyspaces(ResultResponse* result);
  void update_tables(ResultResponse* table_result, ResultResponse* col_result);
  void set_partitioner(const std::string& partitioner_class) { token_map_.set_partitioner(partitioner_class); }
  void update_host(SharedRefPtr<Host>& host, const TokenStringList& tokens) { token_map_.update_host(host, tokens); }
  void build() { token_map_.build(); }
  void drop_keyspace(const std::string& keyspace_name);
  void drop_table(const std::string& keyspace_name, const std::string& table_name) { schema_.drop_table(keyspace_name, table_name); }
  void remove_host(SharedRefPtr<Host>& host) { token_map_.remove_host(host); }

  const Schema& schema() const { return schema_; }
  Schema* copy_schema() const;// synchronized copy for API

  void set_protocol_version(int version) { schema_.set_protocol_version(version); }

  const TokenMap& token_map() const { return token_map_; }

private:
  Schema schema_;
  TokenMap token_map_;

  // Used to synch schema updates and copies
  mutable uv_mutex_t schema_mutex_;
};

} // namespace cass

#endif
