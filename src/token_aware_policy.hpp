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

#ifndef __CASS_TOKEN_AWARE_POLICY_HPP_INCLUDED__
#define __CASS_TOKEN_AWARE_POLICY_HPP_INCLUDED__

#include "token_map.hpp"
#include "load_balancing.hpp"
#include "host.hpp"
#include "scoped_ptr.hpp"

namespace cass {

class TokenAwarePolicy : public ChainedLoadBalancingPolicy {
public:
  TokenAwarePolicy(LoadBalancingPolicy* child_policy)
      : ChainedLoadBalancingPolicy(child_policy)
      , index_(0) {}

  virtual ~TokenAwarePolicy() {}

  virtual void init(const Host::Ptr& connected_host, const HostMap& hosts, Random* random);

  virtual QueryPlan* new_query_plan(const std::string& connected_keyspace,
                                    RequestHandler* request_handler,
                                    const TokenMap* token_map);

  LoadBalancingPolicy* new_instance() { return new TokenAwarePolicy(child_policy_->new_instance()); }

private:
  class TokenAwareQueryPlan : public QueryPlan {
  public:
    TokenAwareQueryPlan(LoadBalancingPolicy* child_policy, QueryPlan* child_plan, const CopyOnWriteHostVec& replicas, size_t start_index)
      : child_policy_(child_policy)
      , child_plan_(child_plan)
      , replicas_(replicas)
      , index_(start_index)
      , remaining_(replicas->size()) {}

    Host::Ptr compute_next();

  private:
    LoadBalancingPolicy* child_policy_;
    ScopedPtr<QueryPlan> child_plan_;
    CopyOnWriteHostVec replicas_;
    size_t index_;
    size_t remaining_;
  };

  size_t index_;

private:
  DISALLOW_COPY_AND_ASSIGN(TokenAwarePolicy);
};
} // namespace cass

#endif
