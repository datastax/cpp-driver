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

#include "dc_aware_policy.hpp"

#include "logger.hpp"

#include "scoped_lock.hpp"

namespace cass {


static const CopyOnWriteHostVec NO_HOSTS(new HostVec());

void DCAwarePolicy::init(const SharedRefPtr<Host>& connected_host, const HostMap& hosts) {
  if (local_dc_.empty() && connected_host && !connected_host->dc().empty()) {
    LOG_INFO("Using '%s' for the local data center "
             "(if this is incorrect, please provide the correct data center)",
             connected_host->dc().c_str());
    local_dc_ = connected_host->dc();
  }

  for (HostMap::const_iterator i = hosts.begin(),
       end = hosts.end(); i != end; ++i) {
    on_add(i->second);
  }
}

CassHostDistance DCAwarePolicy::distance(const SharedRefPtr<Host>& host) const {
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

QueryPlan* DCAwarePolicy::new_query_plan(const std::string& connected_keyspace,
                                        const Request* request,
                                        const TokenMap& token_map) {
  CassConsistency cl = request != NULL ? request->consistency() : CASS_CONSISTENCY_ONE;
  return new DCAwareQueryPlan(this, cl, index_++);
}

void DCAwarePolicy::on_add(const SharedRefPtr<Host>& host) {
  const std::string& dc = host->dc();
  if (local_dc_.empty() && !dc.empty()) {
    LOG_INFO("Using '%s' for local data center "
             "(if this is incorrect, please provide the correct data center)",
             host->dc().c_str());
    local_dc_ = dc;
  }

  if (dc == local_dc_) {
    local_dc_live_hosts_->push_back(host);
  } else {
    per_remote_dc_live_hosts_.add_host_to_dc(dc, host);
  }
}

void DCAwarePolicy::on_remove(const SharedRefPtr<Host>& host) {
  const std::string& dc = host->dc();
  if (dc == local_dc_) {
    remove_host(local_dc_live_hosts_, host);
  } else {
    per_remote_dc_live_hosts_.remove_host_from_dc(host->dc(), host);
  }
}

void DCAwarePolicy::on_up(const SharedRefPtr<Host>& host) {
  on_add(host);
}

void DCAwarePolicy::on_down(const SharedRefPtr<Host>& host) {
  on_remove(host);
}

void DCAwarePolicy::PerDCHostMap::add_host_to_dc(const std::string& dc, const SharedRefPtr<Host>& host) {
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

void DCAwarePolicy::PerDCHostMap::remove_host_from_dc(const std::string& dc, const SharedRefPtr<Host>& host) {
  ScopedWriteLock wl(&rwlock_);
  Map::iterator i = map_.find(dc);
  if (i != map_.end()) {
    remove_host(i->second, host);
  }
}

const CopyOnWriteHostVec& DCAwarePolicy::PerDCHostMap::get_hosts(const std::string& dc) const {
  ScopedReadLock rl(&rwlock_);
  Map::const_iterator i = map_.find(dc);
  if (i == map_.end()) return NO_HOSTS;
  return i->second;
}

void DCAwarePolicy::PerDCHostMap::copy_dcs(KeySet* dcs) const {
  ScopedReadLock rl(&rwlock_);
  for (Map::const_iterator i = map_.begin(),
       end = map_.end(); i != end; ++i) {
    dcs->insert(i->first);
  }
}

// Helper method to prevent copy (Notice: "const CopyOnWriteHostVec&")
static const SharedRefPtr<Host>& get_next_host(const CopyOnWriteHostVec& hosts, size_t index) {
  return (*hosts)[index % hosts->size()];
}

// Helper method to prevent copy (Notice: "const CopyOnWriteHostVec&")
static size_t get_hosts_size(const CopyOnWriteHostVec& hosts) {
  return hosts->size();
}

DCAwarePolicy::DCAwareQueryPlan::DCAwareQueryPlan(const DCAwarePolicy* policy,
                                                  CassConsistency cl,
                                                  size_t start_index)
  : policy_(policy)
  , cl_(cl)
  , hosts_(policy_->local_dc_live_hosts_)
  , local_remaining_(get_hosts_size(hosts_))
  , remote_remaining_(0)
  , index_(start_index) {}

SharedRefPtr<Host> DCAwarePolicy::DCAwareQueryPlan::compute_next() {
  while (local_remaining_ > 0) {
    --local_remaining_;
    const SharedRefPtr<Host>& host(get_next_host(hosts_, index_++));
    if (host->is_up()) {
      return host;
    }
  }

  if (policy_->skip_remote_dcs_for_local_cl_ && is_dc_local(cl_)) {
    return SharedRefPtr<Host>();
  }

  if (!remote_dcs_) {
    remote_dcs_.reset(new PerDCHostMap::KeySet());
    policy_->per_remote_dc_live_hosts_.copy_dcs(remote_dcs_.get());
  }

  while (true) {
    while (remote_remaining_ > 0) {
      --remote_remaining_;
      const SharedRefPtr<Host>& host(get_next_host(hosts_, index_++));
      if (host->is_up()) {
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

  return SharedRefPtr<Host>();
}

} // namespace cass
