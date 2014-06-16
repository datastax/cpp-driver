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
#include "load_balancing_policy.hpp"
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

  void new_query_plan(std::list<Host>* output) {
    size_t index = index_++;
    size_t hosts_size = hosts_.size();
    for (size_t i = 0; i < hosts_.size(); ++i) {
      output->push_back(hosts_[index++ % hosts_size]);
    }
  }

private:
  std::vector<Host> hosts_;
  size_t index_;
};

} // namespace cass

#endif
