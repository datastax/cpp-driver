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

#include "host_targeting_policy.hpp"

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

void HostTargetingPolicy::init(const SharedRefPtr<Host>& connected_host, const core::HostMap& hosts,
                               Random* random) {
  for (core::HostMap::const_iterator it = hosts.begin(), end = hosts.end(); it != end; ++it) {
    hosts_[it->first] = it->second;
  }
  ChainedLoadBalancingPolicy::init(connected_host, hosts, random);
}

QueryPlan* HostTargetingPolicy::new_query_plan(const String& keyspace,
                                               RequestHandler* request_handler,
                                               const TokenMap* token_map) {
  QueryPlan* child_plan = child_policy_->new_query_plan(keyspace, request_handler, token_map);
  if (request_handler == NULL || !request_handler->preferred_address().is_valid()) {
    return child_plan;
  }

  HostMap::const_iterator it = hosts_.find(request_handler->preferred_address());
  if (it == hosts_.end() || !is_host_up(it->first)) {
    return child_plan;
  }

  return new HostTargetingQueryPlan(it->second, child_plan);
}

void HostTargetingPolicy::on_host_added(const SharedRefPtr<Host>& host) {
  hosts_[host->address()] = host;
  ChainedLoadBalancingPolicy::on_host_added(host);
}

void HostTargetingPolicy::on_host_removed(const SharedRefPtr<Host>& host) {
  hosts_.erase(host->address());
  ChainedLoadBalancingPolicy::on_host_removed(host);
}

SharedRefPtr<Host> HostTargetingPolicy::HostTargetingQueryPlan::compute_next() {
  if (first_) {
    first_ = false;
    return preferred_host_;
  } else {
    Host::Ptr next = child_plan_->compute_next();
    if (next && next->address() == preferred_host_->address()) {
      return child_plan_->compute_next();
    }
    return next;
  }
}
