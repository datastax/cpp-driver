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

#include "driver_utils.hpp"

#include "address.hpp"
#include "cluster_config.hpp"
#include "driver_info.hpp"
#include "future.hpp"
#include "murmur3.hpp"
#include "request_handler.hpp"
#include "statement.hpp"

using namespace datastax::internal;
using namespace datastax::internal::core;

std::vector<std::string> test::driver::internals::Utils::attempted_hosts(CassFuture* future) {
  std::vector<std::string> attempted_hosts;
  if (future) {
    Future* cass_future = static_cast<Future*>(future);
    if (cass_future->type() == Future::FUTURE_TYPE_RESPONSE) {
      ResponseFuture* response = static_cast<ResponseFuture*>(cass_future);
      AddressVec attempted_addresses = response->attempted_addresses();
      for (AddressVec::iterator iterator = attempted_addresses.begin();
           iterator != attempted_addresses.end(); ++iterator) {
        attempted_hosts.push_back(iterator->to_string().c_str());
      }
      std::sort(attempted_hosts.begin(), attempted_hosts.end());
    }
  }
  return attempted_hosts;
}

unsigned int test::driver::internals::Utils::connect_timeout(CassCluster* cluster) {
  return cluster->config().connect_timeout_ms();
}

std::string test::driver::internals::Utils::contact_points(CassCluster* cluster) {
  std::string contact_points;
  const AddressVec& contact_points_list = cluster->config().contact_points();
  for (AddressVec::const_iterator it = contact_points_list.begin(); it != contact_points_list.end();
       ++it) {
    if (contact_points.size() > 0) {
      contact_points.push_back(',');
    }
    contact_points.append((*it).hostname_or_address().c_str());
  }
  return contact_points;
}

std::string test::driver::internals::Utils::driver_name() {
  return datastax::internal::driver_name();
}

std::string test::driver::internals::Utils::driver_version() {
  return datastax::internal::driver_version();
}

std::string test::driver::internals::Utils::host(CassFuture* future) {
  if (future) {
    Future* cass_future = static_cast<Future*>(future);
    if (cass_future->type() == Future::FUTURE_TYPE_RESPONSE) {
      return static_cast<ResponseFuture*>(cass_future)->address().hostname_or_address().c_str();
    }
  }
  return "";
}

std::string test::driver::internals::Utils::server_name(CassFuture* future) {
  if (future) {
    Future* cass_future = static_cast<Future*>(future);
    if (cass_future->type() == Future::FUTURE_TYPE_RESPONSE) {
      return static_cast<ResponseFuture*>(cass_future)->address().server_name().c_str();
    }
  }
  return "";
}

int64_t test::driver::internals::Utils::murmur3_hash(const std::string& value) {
  return MurmurHash3_x64_128(value.data(), value.size(), 0);
}

int test::driver::internals::Utils::port(CassCluster* cluster) { return cluster->config().port(); }

void test::driver::internals::Utils::set_record_attempted_hosts(CassStatement* statement,
                                                                bool enable) {
  if (statement) {
    static_cast<Statement*>(statement)->set_record_attempted_addresses(enable);
  }
}
