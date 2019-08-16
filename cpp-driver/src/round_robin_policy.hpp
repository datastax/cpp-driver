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

#ifndef DATASTAX_INTERNAL_ROUND_ROBIN_POLICY_HPP
#define DATASTAX_INTERNAL_ROUND_ROBIN_POLICY_HPP

#include "cassandra.h"
#include "copy_on_write_ptr.hpp"
#include "host.hpp"
#include "load_balancing.hpp"
#include "random.hpp"

namespace datastax { namespace internal { namespace core {

class RoundRobinPolicy : public LoadBalancingPolicy {
public:
  RoundRobinPolicy();
  ~RoundRobinPolicy();

  virtual void init(const Host::Ptr& connected_host, const HostMap& hosts, Random* random,
                    const String& local_dc);

  virtual CassHostDistance distance(const Host::Ptr& host) const;

  virtual QueryPlan* new_query_plan(const String& keyspace, RequestHandler* request_handler,
                                    const TokenMap* token_map);

  virtual bool is_host_up(const Address& address) const;

  virtual void on_host_added(const Host::Ptr& host);
  virtual void on_host_removed(const Host::Ptr& host);
  virtual void on_host_up(const Host::Ptr& host);
  virtual void on_host_down(const Address& address);

  virtual LoadBalancingPolicy* new_instance() { return new RoundRobinPolicy(); }

private:
  class RoundRobinQueryPlan : public QueryPlan {
  public:
    RoundRobinQueryPlan(const RoundRobinPolicy* policy, const CopyOnWriteHostVec& hosts,
                        size_t start_index)
        : policy_(policy)
        , hosts_(hosts)
        , index_(start_index)
        , remaining_(hosts->size()) {}

    virtual Host::Ptr compute_next();

  private:
    const RoundRobinPolicy* policy_;
    const CopyOnWriteHostVec hosts_;
    size_t index_;
    size_t remaining_;
  };

  mutable uv_rwlock_t available_rwlock_;
  AddressSet available_;

  CopyOnWriteHostVec hosts_;
  size_t index_;

private:
  DISALLOW_COPY_AND_ASSIGN(RoundRobinPolicy);
};

}}} // namespace datastax::internal::core

#endif
