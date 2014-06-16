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

#ifndef __CASS_LOAD_BALANCING_POLICY_HPP_INCLUDED__
#define __CASS_LOAD_BALANCING_POLICY_HPP_INCLUDED__

#include <set>
#include <string>

#include "common.hpp"
#include "host.hpp"

namespace cass {

class LoadBalancingPolicy {
public:
  virtual ~LoadBalancingPolicy() {}

  virtual void init(const std::set<Host>& hosts) = 0;

  virtual CassHostDistance distance(const Host& host) = 0;

  // TODO(mpenick): Figure out what parameters to pass, keyspace, consistency,
  // etc.
  virtual void new_query_plan(std::list<Host>* output) = 0;
};

} // namespace cass

#endif
