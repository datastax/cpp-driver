/*
  Copyright 2014 DataStax

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

#ifndef __CASS_ROUND_ROBIN_POLICY_HPP_INCLUDED__
#define __CASS_ROUND_ROBIN_POLICY_HPP_INCLUDED__

#include "cassandra.h"
#include "copy_on_write_ptr.hpp"
#include "load_balancing.hpp"
#include "host.hpp"

#include <algorithm>

namespace cass {

class RoundRobinPolicy : public LoadBalancingPolicy {
public:
  RoundRobinPolicy()
    : live_host_addresses_(new AddressVec)
    , index_(0) {}

  virtual void init(const HostMap& hosts) {
    live_host_addresses_->reserve(hosts.size());
    for (HostMap::const_iterator it = hosts.begin(),
         end = hosts.end(); it != end; ++it) {
      live_host_addresses_->push_back(it->second->address());
    }
  }

  virtual CassHostDistance distance(const SharedRefPtr<Host>& host) {
    return CASS_HOST_DISTANCE_LOCAL;
  }

  virtual QueryPlan* new_query_plan() {
    return new RoundRobinQueryPlan(live_host_addresses_, index_++);
  }

  virtual void on_add(SharedRefPtr<Host> host) {
    on_up(host);
  }

  virtual void on_remove(SharedRefPtr<Host> host) {
    on_down(host);
  }

  virtual void on_up(SharedRefPtr<Host> host) {
    AddressVec::iterator it
        = std::find(live_host_addresses_->begin(), live_host_addresses_->end(), host->address());
    if (it == live_host_addresses_->end()) {
      live_host_addresses_->push_back(host->address());
    }
  }

  virtual void on_down(SharedRefPtr<Host> host) {
    AddressVec::iterator it
        = std::find(live_host_addresses_->begin(), live_host_addresses_->end(), host->address());
    if (it != live_host_addresses_->end()) {
      live_host_addresses_->erase(it);
    }
  }

private:
  class RoundRobinQueryPlan : public QueryPlan {
  public:
    RoundRobinQueryPlan(const CopyOnWritePtr<AddressVec>& hosts, size_t start_index)
      : host_addresses_(hosts)
      , index_(start_index)
      , remaining_(hosts->size()) {}

    bool compute_next(Address* address)  {
      if (remaining_ == 0) {
        return false;
      }

      remaining_--;
      *address = (*host_addresses_)[index_++ % host_addresses_->size()];
      return true;
    }

  private:
    const CopyOnWritePtr<AddressVec> host_addresses_;
    size_t index_;
    size_t remaining_;
  };

  CopyOnWritePtr<AddressVec> live_host_addresses_;
  size_t index_;
};

} // namespace cass

#endif
