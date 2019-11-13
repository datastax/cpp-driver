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

#include "testing.hpp"

#include "address.hpp"
#include "cluster_config.hpp"
#include "external.hpp"
#include "future.hpp"
#include "get_time.hpp"
#include "logger.hpp"
#include "metadata.hpp"
#include "murmur3.hpp"
#include "request_handler.hpp"
#include "result_response.hpp"
#include "session.hpp"
#include "statement.hpp"

namespace datastax { namespace internal { namespace testing {

using namespace core;

String get_host_from_future(CassFuture* future) {
  if (future->type() != Future::FUTURE_TYPE_RESPONSE) {
    return "";
  }
  ResponseFuture* response_future = static_cast<ResponseFuture*>(future->from());
  return response_future->address().hostname_or_address();
}

StringVec get_attempted_hosts_from_future(CassFuture* future) {
  if (future->type() != Future::FUTURE_TYPE_RESPONSE) {
    return StringVec();
  }
  StringVec attempted_hosts;
  ResponseFuture* response_future = static_cast<ResponseFuture*>(future->from());

  AddressVec attempted_addresses = response_future->attempted_addresses();
  for (AddressVec::iterator it = attempted_addresses.begin(); it != attempted_addresses.end();
       ++it) {
    attempted_hosts.push_back(it->to_string());
  }
  std::sort(attempted_hosts.begin(), attempted_hosts.end());
  return attempted_hosts;
}

unsigned get_connect_timeout_from_cluster(CassCluster* cluster) {
  return cluster->config().connect_timeout_ms();
}

int get_port_from_cluster(CassCluster* cluster) { return cluster->config().port(); }

String get_contact_points_from_cluster(CassCluster* cluster) {
  String str;

  const AddressVec& contact_points = cluster->config().contact_points();

  for (AddressVec::const_iterator it = contact_points.begin(), end = contact_points.end();
       it != end; ++it) {
    if (str.size() > 0) {
      str.push_back(',');
    }
    str.append(it->hostname_or_address());
  }

  return str;
}

int64_t create_murmur3_hash_from_string(const String& value) {
  return MurmurHash3_x64_128(value.data(), value.size(), 0);
}

uint64_t get_time_since_epoch_in_ms() { return internal::get_time_since_epoch_ms(); }

uint64_t get_host_latency_average(CassSession* session, String ip_address, int port) {
  Address address(ip_address, port);
  if (address.is_valid()) {
    Host::Ptr host(session->cluster()->find_host(address));
    return host ? host->get_current_average().average : 0;
  }
  return 0;
}

CassConsistency get_consistency(const CassStatement* statement) {
  return statement->from()->consistency();
}

CassConsistency get_serial_consistency(const CassStatement* statement) {
  return statement->from()->serial_consistency();
}

uint64_t get_request_timeout_ms(const CassStatement* statement) {
  return statement->from()->request_timeout_ms();
}

const CassRetryPolicy* get_retry_policy(const CassStatement* statement) {
  return CassRetryPolicy::to(statement->from()->retry_policy().get());
}

String get_server_name(CassFuture* future) {
  if (future->type() != Future::FUTURE_TYPE_RESPONSE) {
    return "";
  }
  return static_cast<ResponseFuture*>(future->from())->address().server_name();
}

void set_record_attempted_hosts(CassStatement* statement, bool enable) {
  static_cast<Statement*>(statement->from())->set_record_attempted_addresses(enable);
}

}}} // namespace datastax::internal::testing
