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

#include "round_robin_policy.hpp"

#include <algorithm>
#include <iterator>

namespace cass {

void RoundRobinPolicy::init(const Host::Ptr& connected_host,
                            const HostMap& hosts,
                            Random* random) {
  hosts_->reserve(hosts.size());
  std::transform(hosts.begin(), hosts.end(), std::back_inserter(*hosts_), GetHost());
  if (random != NULL) {
    index_ = random->next(std::max(static_cast<size_t>(1), hosts.size()));
  }
}

CassHostDistance RoundRobinPolicy::distance(const Host::Ptr& host) const {
  return CASS_HOST_DISTANCE_LOCAL;
}

QueryPlan* RoundRobinPolicy::new_query_plan(const std::string& connected_keyspace,
                                            RequestHandler* request_handler,
                                            const TokenMap* token_map) {
  return new RoundRobinQueryPlan(hosts_, index_++);
}

void RoundRobinPolicy::on_add(const Host::Ptr& host) {
  add_host(hosts_, host);
}

void RoundRobinPolicy::on_remove(const Host::Ptr& host) {
  remove_host(hosts_, host);
}

void RoundRobinPolicy::on_up(const Host::Ptr& host) {
  on_add(host);
}

void RoundRobinPolicy::on_down(const Host::Ptr& host) {
  on_remove(host);
}

Host::Ptr RoundRobinPolicy::RoundRobinQueryPlan::compute_next() {
  while (remaining_ > 0) {
    --remaining_;
    const Host::Ptr& host((*hosts_)[index_++ % hosts_->size()]);
    if (host->is_up()) {
      return host;
    }
  }
  return Host::Ptr();
}

} // namespace cass
