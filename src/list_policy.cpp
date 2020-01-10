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

#include "list_policy.hpp"

#include "logger.hpp"

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

void ListPolicy::init(const Host::Ptr& connected_host, const HostMap& hosts, Random* random,
                      const String& local_dc) {
  HostMap valid_hosts;
  for (HostMap::const_iterator i = hosts.begin(), end = hosts.end(); i != end; ++i) {
    const Host::Ptr& host = i->second;
    if (is_valid_host(host)) {
      valid_hosts.insert(HostPair(i->first, host));
    }
  }

  if (valid_hosts.empty()) {
    LOG_ERROR("No valid hosts available for list policy");
  }

  ChainedLoadBalancingPolicy::init(connected_host, valid_hosts, random, local_dc);
}

CassHostDistance ListPolicy::distance(const Host::Ptr& host) const {
  if (is_valid_host(host)) {
    return child_policy_->distance(host);
  }
  return CASS_HOST_DISTANCE_IGNORE;
}

QueryPlan* ListPolicy::new_query_plan(const String& keyspace, RequestHandler* request_handler,
                                      const TokenMap* token_map) {
  return child_policy_->new_query_plan(keyspace, request_handler, token_map);
}

void ListPolicy::on_host_added(const Host::Ptr& host) {
  if (is_valid_host(host)) {
    child_policy_->on_host_added(host);
  }
}

void ListPolicy::on_host_up(const Host::Ptr& host) {
  if (is_valid_host(host)) {
    child_policy_->on_host_up(host);
  }
}
