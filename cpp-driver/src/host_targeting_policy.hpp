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

#ifndef DATASTAX_INTERNAL_HOST_TARGETING_POLICY_HPP
#define DATASTAX_INTERNAL_HOST_TARGETING_POLICY_HPP

#include "address.hpp"
#include "dense_hash_map.hpp"
#include "load_balancing.hpp"
#include "request_handler.hpp"

namespace datastax { namespace internal { namespace core {

class HostTargetingPolicy : public ChainedLoadBalancingPolicy {
public:
  HostTargetingPolicy(LoadBalancingPolicy* child_policy)
      : ChainedLoadBalancingPolicy(child_policy) {}

  virtual void init(const SharedRefPtr<Host>& connected_host, const HostMap& hosts, Random* random,
                    const String& local_dc);

  virtual QueryPlan* new_query_plan(const String& keyspace, RequestHandler* request_handler,
                                    const TokenMap* token_map);

  virtual LoadBalancingPolicy* new_instance() {
    return new HostTargetingPolicy(child_policy_->new_instance());
  }

  virtual void on_host_added(const Host::Ptr& host);
  virtual void on_host_removed(const Host::Ptr& host);

private:
  class HostTargetingQueryPlan : public QueryPlan {
  public:
    HostTargetingQueryPlan(const Host::Ptr& preferred_host, QueryPlan* child_plan)
        : first_(true)
        , preferred_host_(preferred_host)
        , child_plan_(child_plan) {}

    virtual SharedRefPtr<Host> compute_next();

  private:
    bool first_;
    Host::Ptr preferred_host_;
    ScopedPtr<QueryPlan> child_plan_;
  };

private:
  class HostMap : public DenseHashMap<Address, Host::Ptr> {
  public:
    HostMap() {
      set_empty_key(Address::EMPTY_KEY);
      set_deleted_key(Address::DELETED_KEY);
    }
  };
  HostMap hosts_;
};

}}} // namespace datastax::internal::core

#endif
