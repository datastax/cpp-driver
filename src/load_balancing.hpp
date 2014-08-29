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

#ifndef __CASS_LOAD_BALANCING_HPP_INCLUDED__
#define __CASS_LOAD_BALANCING_HPP_INCLUDED__

#include <list>
#include <set>
#include <string>

#include "host.hpp"
#include "constants.hpp"
#include "cassandra.h"

extern "C" {

typedef struct CassHost_ {
  CassInet address;
  CassString rack;
  CassString datacenter;
  CassString version;
} CassHost;

typedef enum CassBalancingState_ {
  CASS_BALANCING_INIT,
  CASS_BALANCING_CLEANUP,
  CASS_BALANCING_ON_UP,
  CASS_BALANCING_ON_DOWN,
  CASS_BALANCING_ON_ADD,
  CASS_BALANCING_ON_REMOVE,
  CASS_BALANCING_DISTANCE,
  CASS_BALANCING_NEW_QUERY_PLAN
} CassBalancingState;

typedef enum CassHostDistance_ {
  CASS_HOST_DISTANCE_LOCAL,
  CASS_HOST_DISTANCE_REMOTE,
  CASS_HOST_DISTANCE_IGNORE
} CassHostDistance;

} // extern "C"

namespace cass {

class QueryPlan {
public:
  virtual ~QueryPlan() {}
  virtual bool compute_next(Address* address) = 0;
};

class LoadBalancingPolicy : public Host::StateListener {
public:
  virtual ~LoadBalancingPolicy() {}

  virtual void init(const HostMap& hosts) = 0;

  virtual CassHostDistance distance(const SharedRefPtr<Host>& host) = 0;

  // TODO(mpenick): Figure out what parameters to pass, keyspace, consistency,
  // etc.
  virtual QueryPlan* new_query_plan() = 0;
};

} // namespace cass

#endif

