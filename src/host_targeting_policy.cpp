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

namespace cass {

void HostTargetingPolicy::init(const SharedRefPtr<Host>& connected_host,
                               const cass::HostMap& hosts,
                               Random* random) {
  for (cass::HostMap::const_iterator i = hosts.begin(),
       end = hosts.end(); i != end; ++i) {
    available_hosts_[i->first] = i->second;
  }
  ChainedLoadBalancingPolicy::init(connected_host, hosts, random);
}

QueryPlan* HostTargetingPolicy::new_query_plan(const std::string& keyspace,
                                               RequestHandler* request_handler,
                                               const TokenMap* token_map) {
  QueryPlan* child_plan = child_policy_->new_query_plan(keyspace,
                                                        request_handler,
                                                        token_map);
  if (request_handler == NULL ||
      !request_handler->preferred_address().is_valid()) {
    return child_plan;
  }

  HostMap::const_iterator i = available_hosts_.find(request_handler->preferred_address());
  if (i == available_hosts_.end()) {
    return child_plan;
  }

  return new HostTargetingQueryPlan(i->second, child_plan);
}

void HostTargetingPolicy::on_add(const SharedRefPtr<Host>& host) {
  available_hosts_[host->address()] = host;
  ChainedLoadBalancingPolicy::on_add(host);
}

void HostTargetingPolicy::on_remove(const SharedRefPtr<Host>& host) {
  available_hosts_.erase(host->address());
  ChainedLoadBalancingPolicy::on_remove(host);
}

void HostTargetingPolicy::on_up(const SharedRefPtr<Host>& host) {
  available_hosts_[host->address()] = host;
  ChainedLoadBalancingPolicy::on_up(host);
}

void HostTargetingPolicy::on_down(const SharedRefPtr<Host>& host) {
  available_hosts_.erase(host->address());
  ChainedLoadBalancingPolicy::on_down(host);
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

} // namespace cass
