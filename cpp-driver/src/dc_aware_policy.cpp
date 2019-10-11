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

#include "dc_aware_policy.hpp"

#include "logger.hpp"
#include "request_handler.hpp"
#include "scoped_lock.hpp"

#include <algorithm>

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

DCAwarePolicy::DCAwarePolicy(const String& local_dc, size_t used_hosts_per_remote_dc,
                             bool skip_remote_dcs_for_local_cl)
    : local_dc_(local_dc)
    , used_hosts_per_remote_dc_(used_hosts_per_remote_dc)
    , skip_remote_dcs_for_local_cl_(skip_remote_dcs_for_local_cl)
    , local_dc_live_hosts_(new HostVec())
    , index_(0) {
  uv_rwlock_init(&available_rwlock_);
  if (used_hosts_per_remote_dc_ > 0 || !skip_remote_dcs_for_local_cl) {
    LOG_WARN("Remote multi-dc settings have been deprecated and will be removed"
             " in the next major release");
  }
}

DCAwarePolicy::~DCAwarePolicy() { uv_rwlock_destroy(&available_rwlock_); }

void DCAwarePolicy::init(const Host::Ptr& connected_host, const HostMap& hosts, Random* random,
                         const String& local_dc) {
  if (local_dc_.empty()) { // Only override if no local DC was specified.
    local_dc_ = local_dc;
  }

  if (local_dc_.empty() && connected_host && !connected_host->dc().empty()) {
    LOG_INFO("Using '%s' for the local data center "
             "(if this is incorrect, please provide the correct data center)",
             connected_host->dc().c_str());
    local_dc_ = connected_host->dc();
  }

  available_.resize(hosts.size());
  std::transform(hosts.begin(), hosts.end(), std::inserter(available_, available_.begin()),
                 GetAddress());

  for (HostMap::const_iterator i = hosts.begin(), end = hosts.end(); i != end; ++i) {
    on_host_added(i->second);
  }
  if (random != NULL) {
    index_ = random->next(std::max(static_cast<size_t>(1), hosts.size()));
  }
}

CassHostDistance DCAwarePolicy::distance(const Host::Ptr& host) const {
  if (local_dc_.empty() || host->dc() == local_dc_) {
    return CASS_HOST_DISTANCE_LOCAL;
  }

  const CopyOnWriteHostVec& hosts = per_remote_dc_live_hosts_.get_hosts(host->dc());
  size_t num_hosts = std::min(hosts->size(), used_hosts_per_remote_dc_);
  for (size_t i = 0; i < num_hosts; ++i) {
    if ((*hosts)[i]->address() == host->address()) {
      return CASS_HOST_DISTANCE_REMOTE;
    }
  }

  return CASS_HOST_DISTANCE_IGNORE;
}

QueryPlan* DCAwarePolicy::new_query_plan(const String& keyspace, RequestHandler* request_handler,
                                         const TokenMap* token_map) {
  CassConsistency cl =
      request_handler != NULL ? request_handler->consistency() : CASS_DEFAULT_CONSISTENCY;
  return new DCAwareQueryPlan(this, cl, index_++);
}

bool DCAwarePolicy::is_host_up(const Address& address) const {
  ScopedReadLock rl(&available_rwlock_);
  return available_.count(address) > 0;
}

void DCAwarePolicy::on_host_added(const Host::Ptr& host) {
  const String& dc = host->dc();
  if (local_dc_.empty() && !dc.empty()) {
    LOG_INFO("Using '%s' for local data center "
             "(if this is incorrect, please provide the correct data center)",
             host->dc().c_str());
    local_dc_ = dc;
  }

  if (dc == local_dc_) {
    add_host(local_dc_live_hosts_, host);
  } else {
    per_remote_dc_live_hosts_.add_host_to_dc(dc, host);
  }
}

void DCAwarePolicy::on_host_removed(const Host::Ptr& host) {
  const String& dc = host->dc();
  if (dc == local_dc_) {
    remove_host(local_dc_live_hosts_, host);
  } else {
    per_remote_dc_live_hosts_.remove_host_from_dc(host->dc(), host);
  }

  ScopedWriteLock wl(&available_rwlock_);
  available_.erase(host->address());
}

void DCAwarePolicy::on_host_up(const Host::Ptr& host) {
  on_host_added(host);

  ScopedWriteLock wl(&available_rwlock_);
  available_.insert(host->address());
}

void DCAwarePolicy::on_host_down(const Address& address) {
  if (!remove_host(local_dc_live_hosts_, address) &&
      !per_remote_dc_live_hosts_.remove_host(address)) {
    LOG_DEBUG("Attempted to mark host %s as DOWN, but it doesn't exist",
              address.to_string().c_str());
  }

  ScopedWriteLock wl(&available_rwlock_);
  available_.erase(address);
}

bool DCAwarePolicy::skip_remote_dcs_for_local_cl() const {
  ScopedReadLock rl(&available_rwlock_);
  return skip_remote_dcs_for_local_cl_;
}

size_t DCAwarePolicy::used_hosts_per_remote_dc() const {
  ScopedReadLock rl(&available_rwlock_);
  return used_hosts_per_remote_dc_;
}

const String& DCAwarePolicy::local_dc() const {
  ScopedReadLock rl(&available_rwlock_);
  return local_dc_;
}

void DCAwarePolicy::PerDCHostMap::add_host_to_dc(const String& dc, const Host::Ptr& host) {
  ScopedWriteLock wl(&rwlock_);
  Map::iterator i = map_.find(dc);
  if (i == map_.end()) {
    CopyOnWriteHostVec hosts(new HostVec());
    hosts->push_back(host);
    map_.insert(Map::value_type(dc, hosts));
  } else {
    add_host(i->second, host);
  }
}

void DCAwarePolicy::PerDCHostMap::remove_host_from_dc(const String& dc, const Host::Ptr& host) {
  ScopedWriteLock wl(&rwlock_);
  Map::iterator i = map_.find(dc);
  if (i != map_.end()) {
    core::remove_host(i->second, host);
  }
}

bool DCAwarePolicy::PerDCHostMap::remove_host(const Address& address) {
  ScopedWriteLock wl(&rwlock_);
  for (Map::iterator i = map_.begin(), end = map_.end(); i != end; ++i) {
    if (core::remove_host(i->second, address)) {
      return true;
    }
  }
  return false;
}

const CopyOnWriteHostVec& DCAwarePolicy::PerDCHostMap::get_hosts(const String& dc) const {
  ScopedReadLock rl(&rwlock_);
  Map::const_iterator i = map_.find(dc);
  if (i == map_.end()) return no_hosts_;

  return i->second;
}

void DCAwarePolicy::PerDCHostMap::copy_dcs(KeySet* dcs) const {
  ScopedReadLock rl(&rwlock_);
  for (Map::const_iterator i = map_.begin(), end = map_.end(); i != end; ++i) {
    dcs->insert(i->first);
  }
}

// Helper functions to prevent copy (Notice: "const CopyOnWriteHostVec&")

static const Host::Ptr& get_next_host(const CopyOnWriteHostVec& hosts, size_t index) {
  return (*hosts)[index % hosts->size()];
}

static const Host::Ptr& get_next_host_bounded(const CopyOnWriteHostVec& hosts, size_t index,
                                              size_t bound) {
  return (*hosts)[index % std::min(hosts->size(), bound)];
}

static size_t get_hosts_size(const CopyOnWriteHostVec& hosts) { return hosts->size(); }

DCAwarePolicy::DCAwareQueryPlan::DCAwareQueryPlan(const DCAwarePolicy* policy, CassConsistency cl,
                                                  size_t start_index)
    : policy_(policy)
    , cl_(cl)
    , hosts_(policy_->local_dc_live_hosts_)
    , local_remaining_(get_hosts_size(hosts_))
    , remote_remaining_(0)
    , index_(start_index) {}

Host::Ptr DCAwarePolicy::DCAwareQueryPlan::compute_next() {
  while (local_remaining_ > 0) {
    --local_remaining_;
    const Host::Ptr& host(get_next_host(hosts_, index_++));
    if (policy_->is_host_up(host->address())) {
      return host;
    }
  }

  if (policy_->skip_remote_dcs_for_local_cl_ && is_dc_local(cl_)) {
    return Host::Ptr();
  }

  if (!remote_dcs_) {
    remote_dcs_.reset(new PerDCHostMap::KeySet());
    policy_->per_remote_dc_live_hosts_.copy_dcs(remote_dcs_.get());
  }

  while (true) {
    while (remote_remaining_ > 0) {
      --remote_remaining_;
      const Host::Ptr& host(
          get_next_host_bounded(hosts_, index_++, policy_->used_hosts_per_remote_dc_));
      if (policy_->is_host_up(host->address())) {
        return host;
      }
    }

    if (remote_dcs_->empty()) {
      break;
    }

    PerDCHostMap::KeySet::iterator i = remote_dcs_->begin();
    hosts_ = policy_->per_remote_dc_live_hosts_.get_hosts(*i);
    remote_remaining_ = std::min(get_hosts_size(hosts_), policy_->used_hosts_per_remote_dc_);
    remote_dcs_->erase(i);
  }

  return Host::Ptr();
}
