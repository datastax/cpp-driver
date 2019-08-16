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

#ifndef DATASTAX_INTERNAL_LOAD_BALANCING_HPP
#define DATASTAX_INTERNAL_LOAD_BALANCING_HPP

#include "allocated.hpp"
#include "cassandra.h"
#include "constants.hpp"
#include "host.hpp"
#include "request.hpp"
#include "string.hpp"
#include "vector.hpp"

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

namespace datastax { namespace internal {

class Random;

namespace core {

class RequestHandler;
class TokenMap;

inline bool is_dc_local(CassConsistency cl) {
  return cl == CASS_CONSISTENCY_LOCAL_ONE || cl == CASS_CONSISTENCY_LOCAL_QUORUM;
}

class QueryPlan : public Allocated {
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

class LoadBalancingPolicy : public RefCounted<LoadBalancingPolicy> {
public:
  typedef SharedRefPtr<LoadBalancingPolicy> Ptr;
  typedef Vector<Ptr> Vec;

  LoadBalancingPolicy()
      : RefCounted<LoadBalancingPolicy>() {}

  virtual ~LoadBalancingPolicy() {}

  virtual void init(const Host::Ptr& connected_host, const HostMap& hosts, Random* random,
                    const String& local_dc) = 0;

  virtual void register_handles(uv_loop_t* loop) {}
  virtual void close_handles() {}

  virtual CassHostDistance distance(const Host::Ptr& host) const = 0;

  virtual bool is_host_up(const Address& address) const = 0;
  virtual void on_host_added(const Host::Ptr& host) = 0;
  virtual void on_host_removed(const Host::Ptr& host) = 0;
  virtual void on_host_up(const Host::Ptr& host) = 0;
  virtual void on_host_down(const Address& address) = 0;

  virtual QueryPlan* new_query_plan(const String& keyspace, RequestHandler* request_handler,
                                    const TokenMap* token_map) = 0;

  virtual LoadBalancingPolicy* new_instance() = 0;
};

inline bool is_host_ignored(const LoadBalancingPolicy::Vec& policies, const Host::Ptr& host) {
  for (LoadBalancingPolicy::Vec::const_iterator it = policies.begin(), end = policies.end();
       it != end; ++it) {
    if ((*it)->distance(host) != CASS_HOST_DISTANCE_IGNORE) {
      return false;
    }
  }
  return true;
}

class ChainedLoadBalancingPolicy : public LoadBalancingPolicy {
public:
  ChainedLoadBalancingPolicy(LoadBalancingPolicy* child_policy)
      : child_policy_(child_policy) {}

  virtual ~ChainedLoadBalancingPolicy() {}

  virtual void init(const Host::Ptr& connected_host, const HostMap& hosts, Random* random,
                    const String& local_dc) {
    return child_policy_->init(connected_host, hosts, random, local_dc);
  }

  virtual const LoadBalancingPolicy::Ptr& child_policy() const { return child_policy_; }

  virtual CassHostDistance distance(const Host::Ptr& host) const {
    return child_policy_->distance(host);
  }

  virtual bool is_host_up(const Address& address) const {
    return child_policy_->is_host_up(address);
  }

  virtual void on_host_added(const Host::Ptr& host) { child_policy_->on_host_added(host); }
  virtual void on_host_removed(const Host::Ptr& host) { child_policy_->on_host_removed(host); }
  virtual void on_host_up(const Host::Ptr& host) { child_policy_->on_host_up(host); }
  virtual void on_host_down(const Address& address) { child_policy_->on_host_down(address); }

protected:
  LoadBalancingPolicy::Ptr child_policy_;
};

} // namespace core
}} // namespace datastax::internal

#endif
