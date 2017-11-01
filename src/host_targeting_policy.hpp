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

#ifndef __CASS_HOST_TARGETING_POLICY_HPP_INCLUDED__
#define __CASS_HOST_TARGETING_POLICY_HPP_INCLUDED__

#include "address.hpp"
#include "load_balancing.hpp"
#include "request_handler.hpp"

#include <sparsehash/dense_hash_map>

namespace cass {

class HostTargetingPolicy : public ChainedLoadBalancingPolicy {
public:
  HostTargetingPolicy(LoadBalancingPolicy* child_policy)
    : ChainedLoadBalancingPolicy(child_policy) {
    available_hosts_.set_empty_key(Address::EMPTY_KEY);
    available_hosts_.set_deleted_key(Address::DELETED_KEY);
  }

  virtual void init(const SharedRefPtr<Host>& connected_host,
                    const cass::HostMap& hosts, Random* random);

  virtual QueryPlan* new_query_plan(const std::string& keyspace,
                                    RequestHandler* request_handler,
                                    const TokenMap* token_map);

  virtual LoadBalancingPolicy* new_instance() {
    return new HostTargetingPolicy(child_policy_->new_instance());
  }

  virtual void on_add(const SharedRefPtr<Host>& host);
  virtual void on_remove(const SharedRefPtr<Host>& host);
  virtual void on_up(const SharedRefPtr<Host>& host);
  virtual void on_down(const SharedRefPtr<Host>& host);

private:
  class HostTargetingQueryPlan : public QueryPlan {
  public:
    HostTargetingQueryPlan(const Host::Ptr& preferred_host, QueryPlan* child_plan)
      : first_(true)
      , preferred_host_(preferred_host)
      , child_plan_(child_plan) { }

    virtual SharedRefPtr<Host> compute_next();

  private:
    bool first_;
    Host::Ptr preferred_host_;
    ScopedPtr<QueryPlan> child_plan_;
  };

private:
  typedef sparsehash::dense_hash_map<Address, Host::Ptr, AddressHash> HostMap;
  HostMap available_hosts_;
};

} // namespace cass

#endif
