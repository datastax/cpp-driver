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

#ifndef __CASS_DC_AWARE_POLICY_HPP_INCLUDED__
#define __CASS_DC_AWARE_POLICY_HPP_INCLUDED__

#include "load_balancing.hpp"
#include "host.hpp"
#include "scoped_ptr.hpp"

namespace cass {

class DCAwarePolicy : public LoadBalancingPolicy {
public:
  DCAwarePolicy(const std::string& local_dc)
      : local_dc_(local_dc) {}

  void init(const HostMap& hosts) {
    HostMap local_hosts;
    HostMap remote_hosts;
    for (HostMap::const_iterator it = hosts.begin(),
         end = hosts.end(); it != end; ++it) {
      if (it->second->dc() == local_dc_) {
        local_hosts.insert(*it);
      } else {
        remote_hosts.insert(*it);
      }
    }
    local_rr_policy_.init(local_hosts);
    remote_rr_policy_.init(remote_hosts);
  }

  virtual CassHostDistance distance(const SharedRefPtr<Host>& host) {
    if (host->dc() == local_dc_) return CASS_HOST_DISTANCE_LOCAL;
    else return CASS_HOST_DISTANCE_REMOTE;
  }

  QueryPlan* new_query_plan() {
    return new DCAwareQueryPlan(local_rr_policy_.new_query_plan(),
                                remote_rr_policy_.new_query_plan());
  }

  virtual void on_add(const SharedRefPtr<Host>& host) {
    plan_for_host(host).on_add(host);
  }

  virtual void on_remove(const SharedRefPtr<Host>& host) {
    plan_for_host(host).on_remove(host);
  }

  virtual void on_up(const SharedRefPtr<Host>& host) {
    plan_for_host(host).on_up(host);
  }

  virtual void on_down(const SharedRefPtr<Host>& host) {
    plan_for_host(host).on_down(host);
  }

  LoadBalancingPolicy* new_instance() { return new DCAwarePolicy(local_dc_); }

private:
  RoundRobinPolicy& plan_for_host(const SharedRefPtr<Host>& host) {
    if (host->dc() == local_dc_) return local_rr_policy_;
    else return remote_rr_policy_;
  }

private:
  class DCAwareQueryPlan : public QueryPlan {
  public:
    DCAwareQueryPlan(QueryPlan* local_plan, QueryPlan* remote_plan)
      : local_plan_(local_plan)
      , remote_plan_(remote_plan) {}

    bool compute_next(Address* address)  {
      if (local_plan_->compute_next(address)) return true;
      else return remote_plan_->compute_next(address);
    }

  private:
    ScopedPtr<QueryPlan> local_plan_;
    ScopedPtr<QueryPlan> remote_plan_;
  };


  std::string local_dc_;
  RoundRobinPolicy local_rr_policy_;
  RoundRobinPolicy remote_rr_policy_;

private:
  DISALLOW_COPY_AND_ASSIGN(DCAwarePolicy);
};

} // namespace cass

#endif
