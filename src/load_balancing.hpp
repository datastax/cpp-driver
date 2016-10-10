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

#ifndef __CASS_LOAD_BALANCING_HPP_INCLUDED__
#define __CASS_LOAD_BALANCING_HPP_INCLUDED__

#include "cassandra.h"
#include "constants.hpp"
#include "host.hpp"
#include "request.hpp"

#include <list>
#include <set>
#include <string>

#include <uv.h>

extern "C" {

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

class Random;
class RequestHandler;
class TokenMap;

inline bool is_dc_local(CassConsistency cl) {
  return cl == CASS_CONSISTENCY_LOCAL_ONE || cl == CASS_CONSISTENCY_LOCAL_QUORUM;
}

class QueryPlan {
public:
  virtual ~QueryPlan() {}
  virtual Host::Ptr compute_next() = 0;

  bool compute_next(Address* address) {
    Host::Ptr host = compute_next();
    if (host) {
      *address = host->address();
      return true;
    }
    return false;
  }
};

class LoadBalancingPolicy : public Host::StateListener, public RefCounted<LoadBalancingPolicy> {
public:
  typedef SharedRefPtr<LoadBalancingPolicy> Ptr;

  LoadBalancingPolicy()
    : RefCounted<LoadBalancingPolicy>() {}

  virtual ~LoadBalancingPolicy() {}

  virtual void init(const Host::Ptr& connected_host, const HostMap& hosts, Random* random) = 0;

  virtual void register_handles(uv_loop_t* loop) {}
  virtual void close_handles() {}

  virtual CassHostDistance distance(const Host::Ptr& host) const = 0;

  virtual QueryPlan* new_query_plan(const std::string& connected_keyspace,
                                    RequestHandler* request_handler,
                                    const TokenMap* token_map) = 0;

  virtual LoadBalancingPolicy* new_instance() = 0;
};


class ChainedLoadBalancingPolicy : public LoadBalancingPolicy {
public:
  ChainedLoadBalancingPolicy(LoadBalancingPolicy* child_policy)
    : child_policy_(child_policy) {}

  virtual ~ChainedLoadBalancingPolicy() {}

  virtual void init(const Host::Ptr& connected_host, const HostMap& hosts, Random* random) {
    return child_policy_->init(connected_host, hosts, random);
  }

  virtual CassHostDistance distance(const Host::Ptr& host) const { return child_policy_->distance(host); }

  virtual void on_add(const Host::Ptr& host) { child_policy_->on_add(host); }

  virtual void on_remove(const Host::Ptr& host) { child_policy_->on_remove(host); }

  virtual void on_up(const Host::Ptr& host) { child_policy_->on_up(host); }

  virtual void on_down(const Host::Ptr& host) { child_policy_->on_down(host); }

protected:
  LoadBalancingPolicy::Ptr child_policy_;
};

} // namespace cass

#endif

