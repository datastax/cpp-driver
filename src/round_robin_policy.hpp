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

#ifndef __CASS_ROUND_ROBIN_POLICY_HPP_INCLUDED__
#define __CASS_ROUND_ROBIN_POLICY_HPP_INCLUDED__

#include "cassandra.h"
#include "copy_on_write_ptr.hpp"
#include "load_balancing.hpp"
#include "host.hpp"
#include "random.hpp"

namespace cass {

class RoundRobinPolicy : public LoadBalancingPolicy {
public:
  RoundRobinPolicy()
    : hosts_(Memory::allocate<HostVec>())
    , index_(0) { }

  virtual void init(const Host::Ptr& connected_host, const HostMap& hosts, Random* random);

  virtual CassHostDistance distance(const Host::Ptr& host) const;

  virtual QueryPlan* new_query_plan(const String& keyspace,
                                    RequestHandler* request_handler,
                                    const TokenMap* token_map);

  virtual void on_add(const Host::Ptr& host);
  virtual void on_remove(const Host::Ptr& host);
  virtual void on_up(const Host::Ptr& host);
  virtual void on_down(const Host::Ptr& host);

  virtual LoadBalancingPolicy* new_instance() { return Memory::allocate<RoundRobinPolicy>(); }

private:
  class RoundRobinQueryPlan : public QueryPlan {
  public:
    RoundRobinQueryPlan(const CopyOnWriteHostVec& hosts, size_t start_index)
      : hosts_(hosts)
      , index_(start_index)
      , remaining_(hosts->size()) { }

    virtual Host::Ptr compute_next();

  private:
    const CopyOnWriteHostVec hosts_;
    size_t index_;
    size_t remaining_;
  };

  CopyOnWriteHostVec hosts_;
  size_t index_;

private:
  DISALLOW_COPY_AND_ASSIGN(RoundRobinPolicy);
};

} // namespace cass

#endif
