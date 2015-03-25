/*
  Copyright (c) 2014-2015 DataStax

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

#include <algorithm>

namespace cass {

class RoundRobinPolicy : public LoadBalancingPolicy {
public:
  RoundRobinPolicy()
    : hosts_(new HostVec)
    , index_(0) {}

  virtual void init(const SharedRefPtr<Host>& connected_host, const HostMap& hosts) {
    copy_hosts(hosts, hosts_);
  }

  virtual CassHostDistance distance(const SharedRefPtr<Host>& host) const {
    return CASS_HOST_DISTANCE_LOCAL;
  }

  virtual QueryPlan* new_query_plan(const std::string& connected_keyspace,
                                    const Request* request,
                                    const TokenMap& token_map) {
    return new RoundRobinQueryPlan(hosts_, index_++);
  }

  virtual void on_add(const SharedRefPtr<Host>& host) {
    add_host(hosts_, host);
  }

  virtual void on_remove(const SharedRefPtr<Host>& host) {
    remove_host(hosts_, host);
  }

  virtual void on_up(const SharedRefPtr<Host>& host) {
    on_add(host);
  }

  virtual void on_down(const SharedRefPtr<Host>& host) {
    on_remove(host);
  }

  virtual LoadBalancingPolicy* new_instance() { return new RoundRobinPolicy(); }

private:
  class RoundRobinQueryPlan : public QueryPlan {
  public:
    RoundRobinQueryPlan(const CopyOnWriteHostVec& hosts, size_t start_index)
      : hosts_(hosts)
      , index_(start_index)
      , remaining_(hosts->size()) {}

    SharedRefPtr<Host> compute_next()  {
      while (remaining_ > 0) {
        --remaining_;
        const SharedRefPtr<Host>& host((*hosts_)[index_++ % hosts_->size()]);
        if (host->is_up()) {
          return host;
        }
      }
      return SharedRefPtr<Host>();
    }

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
