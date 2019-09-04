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

#include "round_robin_policy.hpp"
#include "scoped_lock.hpp"

#include <algorithm>
#include <iterator>

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

RoundRobinPolicy::RoundRobinPolicy()
    : hosts_(new HostVec())
    , index_(0) {
  uv_rwlock_init(&available_rwlock_);
}

RoundRobinPolicy::~RoundRobinPolicy() { uv_rwlock_destroy(&available_rwlock_); }

void RoundRobinPolicy::init(const Host::Ptr& connected_host, const HostMap& hosts, Random* random,
                            const String& local_dc) {
  available_.resize(hosts.size());
  std::transform(hosts.begin(), hosts.end(), std::inserter(available_, available_.begin()),
                 GetAddress());
  hosts_->reserve(hosts.size());
  std::transform(hosts.begin(), hosts.end(), std::back_inserter(*hosts_), GetHost());

  if (random != NULL) {
    index_ = random->next(std::max(static_cast<size_t>(1), hosts.size()));
  }
}

CassHostDistance RoundRobinPolicy::distance(const Host::Ptr& host) const {
  return CASS_HOST_DISTANCE_LOCAL;
}

QueryPlan* RoundRobinPolicy::new_query_plan(const String& keyspace, RequestHandler* request_handler,
                                            const TokenMap* token_map) {
  return new RoundRobinQueryPlan(this, hosts_, index_++);
}

bool RoundRobinPolicy::is_host_up(const Address& address) const {
  ScopedReadLock rl(&available_rwlock_);
  return available_.count(address) > 0;
}

void RoundRobinPolicy::on_host_added(const Host::Ptr& host) { add_host(hosts_, host); }

void RoundRobinPolicy::on_host_removed(const Host::Ptr& host) { on_host_down(host->address()); }

void RoundRobinPolicy::on_host_up(const Host::Ptr& host) {
  add_host(hosts_, host);

  ScopedWriteLock wl(&available_rwlock_);
  available_.insert(host->address());
}

void RoundRobinPolicy::on_host_down(const Address& address) {
  if (remove_host(hosts_, address)) {
    LOG_DEBUG("Attempted to remove or mark host %s as DOWN, but it doesn't exist",
              address.to_string().c_str());
  }

  ScopedWriteLock wl(&available_rwlock_);
  available_.erase(address);
}

Host::Ptr RoundRobinPolicy::RoundRobinQueryPlan::compute_next() {
  while (remaining_ > 0) {
    --remaining_;
    const Host::Ptr& host((*hosts_)[index_++ % hosts_->size()]);
    if (policy_->is_host_up(host->address())) {
      return host;
    }
  }
  return Host::Ptr();
}
