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

#ifndef DATASTAX_INTERNAL_LIST_POLICY_HPP
#define DATASTAX_INTERNAL_LIST_POLICY_HPP

#include "host.hpp"
#include "load_balancing.hpp"
#include "scoped_ptr.hpp"

namespace datastax { namespace internal { namespace core {

class ListPolicy : public ChainedLoadBalancingPolicy {
public:
  ListPolicy(LoadBalancingPolicy* child_policy)
      : ChainedLoadBalancingPolicy(child_policy) {}

  virtual ~ListPolicy() {}

  virtual void init(const Host::Ptr& connected_host, const HostMap& hosts, Random* random,
                    const String& local_dc);

  virtual CassHostDistance distance(const Host::Ptr& host) const;

  virtual QueryPlan* new_query_plan(const String& keyspace, RequestHandler* request_handler,
                                    const TokenMap* token_map);

  virtual void on_host_added(const Host::Ptr& host);
  virtual void on_host_up(const Host::Ptr& host);

  virtual ListPolicy* new_instance() = 0;

private:
  virtual bool is_valid_host(const Host::Ptr& host) const = 0;
};

}}} // namespace datastax::internal::core

#endif
