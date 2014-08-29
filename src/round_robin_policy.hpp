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
    : hosts_(new HostVec)
    , index_(0) {}

  virtual void init(const HostMap& hosts) {
    hosts_->reserve(hosts.size());
    for (HostMap::const_iterator it = hosts.begin(),
         end = hosts.end(); it != end; ++it) {
      hosts_->push_back(it->second);
    }
  }

  virtual CassHostDistance distance(const SharedRefPtr<Host>& host) {
    return CASS_HOST_DISTANCE_LOCAL;
  }

  virtual QueryPlan* new_query_plan() {
    return new RoundRobinQueryPlan(hosts_, index_++);
  }

  virtual void on_add(SharedRefPtr<Host> host) {
    HostVec::iterator it;
    for (it = hosts_->begin(); it != hosts_->end(); ++it) {
      if ((*it)->address() == host->address()) {
        (*it) = host;
        break;
      }
    }
    if (it == hosts_->end()) {
      hosts_->push_back(host);
    }
  }

  virtual void on_remove(SharedRefPtr<Host> host) {
    for (HostVec::iterator it = hosts_->begin(); it != hosts_->end(); ++it) {
      if ((*it)->address() == host->address()) {
        hosts_->erase(it);
        break;
      }
    }
  }

  // up/down Host state managed through session
  virtual void on_up(SharedRefPtr<Host> host) {}

  virtual void on_down(SharedRefPtr<Host> host) {}

  virtual LoadBalancingPolicy* new_instance() { return new RoundRobinPolicy(); }

private:
  class RoundRobinQueryPlan : public QueryPlan {
  public:
    RoundRobinQueryPlan(const CopyOnWritePtr<HostVec>& hosts, size_t start_index)
      : hosts_(hosts)
      , index_(start_index)
      , remaining_(hosts->size()) {}

    bool compute_next(Address* address)  {
      while (remaining_ > 0) {
        --remaining_;
        const SharedRefPtr<Host>& host((*hosts_)[index_++ % hosts_->size()]);
        if (host->is_up()) {
          *address = host->address();
          return true;
        }
        if (!observed_down_.insert(host->address()).second) {
          ++remaining_;// don't count same host against this plan
        }
        index_ += rand()%remaining_;// hot-spot mitigation for small clusters
      }
      return false;
    }

  private:
    const CopyOnWritePtr<HostVec> hosts_;
    size_t index_;
    size_t remaining_;
    std::set<Address> observed_down_;
  };

  CopyOnWritePtr<HostVec> hosts_;
  size_t index_;

private:
  DISALLOW_COPY_AND_ASSIGN(RoundRobinPolicy);
};

} // namespace cass

#endif
