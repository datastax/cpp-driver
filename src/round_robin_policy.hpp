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
#include "load_balancing.hpp"
#include "host.hpp"

namespace cass {

class RoundRobinPolicy : public LoadBalancingPolicy {
public:
  RoundRobinPolicy()
      : index_(0) {}

  void init(const std::set<Host>& hosts) {
    hosts_.assign(hosts.begin(), hosts.end());
  }

  CassHostDistance distance(const Host& host) {
    return CASS_HOST_DISTANCE_LOCAL;
  }

  QueryPlan* new_query_plan() {
    return new RoundRobinQueryPlan(hosts_, index_++);
  }

private:
  class RoundRobinQueryPlan : public QueryPlan {
  public:
    RoundRobinQueryPlan(const HostVec& hosts, size_t start_index)
      : hosts_(hosts)
      , index_(start_index)
      , remaining_(hosts.size()) {}

    bool compute_next(Host* host)  {
      if (remaining_ == 0) {
        return false;
      }

      remaining_--;
      *host = hosts_[index_++ % hosts_.size()];
      return true;
    }

  private:
    // TODO(mpenick): This can eventually use a ref counted COW container
    HostVec hosts_;
    size_t index_;
    size_t remaining_;
  };

  HostVec hosts_;
  size_t index_;
};

} // namespace cass

#endif
