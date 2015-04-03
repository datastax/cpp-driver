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

#include "testing.hpp"

#include "address.hpp"
#include "get_time.hpp"
#include "logger.hpp"
#include "murmur3.hpp"
#include "result_response.hpp"
#include "schema_metadata.hpp"
#include "types.hpp"

namespace cass {

std::string get_host_from_future(CassFuture* future) {
  if (future->type() != cass::CASS_FUTURE_TYPE_RESPONSE) {
    return "";
  }
  cass::ResponseFuture* response_future =
      static_cast<cass::ResponseFuture*>(future->from());
  return response_future->get_host_address().to_string();
}

unsigned get_connect_timeout_from_cluster(CassCluster* cluster) {
  return cluster->config().connect_timeout_ms();
}

int get_port_from_cluster(CassCluster* cluster) {
  return cluster->config().port();
}

std::string get_contact_points_from_cluster(CassCluster* cluster) {
  std::string str;

  const cass::Config::ContactPointList& contact_points
      = cluster->config().contact_points();

  for (cass::Config::ContactPointList::const_iterator it = contact_points.begin(),
       end = contact_points.end();
       it != end; ++it) {
    if (str.size() > 0) {
      str.push_back(',');
    }
    str.append(*it);
  }

  return str;
}

CassSchemaMeta* get_schema_meta_from_keyspace(const CassSchema* schema, const std::string& keyspace) {
  CassSchemaMeta* foundSchemaMeta = NULL;

  if (schema) {
    foundSchemaMeta = reinterpret_cast<CassSchemaMeta*>(const_cast<SchemaMetadata*>(schema->from()->get(keyspace)));
  }

  return foundSchemaMeta;
}

int64_t create_murmur3_hash_from_string(const std::string &value) {
  return MurmurHash3_x64_128(value.data(), value.size(), 0);
}

bool is_log_flushed() {
  return Logger::is_flushed();
}

uint64_t get_time_since_epoch_in_ms() {
  return cass::get_time_since_epoch_ms();
}

uint64_t get_host_latency_average(CassSession* session, std::string ip_address, int port) {
  Address address;
  if (Address::from_string(ip_address, port, &address)) {
    return session->get_host(address)->get_current_average().average;
  }
  return 0;
}

} // namespace cass
