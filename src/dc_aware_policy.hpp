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

#ifndef __CASS_DC_AWARE_POLICY_HPP_INCLUDED__
#define __CASS_DC_AWARE_POLICY_HPP_INCLUDED__

#include "load_balancing.hpp"
#include "host.hpp"
#include "round_robin_policy.hpp"
#include "scoped_ptr.hpp"
#include "scoped_lock.hpp"

#include <map>
#include <set>
#include <uv.h>

namespace cass {

class DCAwarePolicy : public LoadBalancingPolicy {
public:
  DCAwarePolicy()
      : used_hosts_per_remote_dc_(0)
      , skip_remote_dcs_for_local_cl_(true)
      , local_dc_live_hosts_(new HostVec)
      , index_(0) {}

  DCAwarePolicy(const std::string& local_dc,
                size_t used_hosts_per_remote_dc,
                bool skip_remote_dcs_for_local_cl)
      : local_dc_(local_dc)
      , used_hosts_per_remote_dc_(used_hosts_per_remote_dc)
      , skip_remote_dcs_for_local_cl_(skip_remote_dcs_for_local_cl)
      , local_dc_live_hosts_(new HostVec)
      , index_(0) {}

  virtual void init(const Host::Ptr& connected_host, const HostMap& hosts, Random* random);

  virtual CassHostDistance distance(const Host::Ptr& host) const;

  virtual QueryPlan* new_query_plan(const std::string& connected_keyspace,
                                    RequestHandler* request_handler,
                                    const TokenMap* token_map);

  virtual void on_add(const Host::Ptr& host);

  virtual void on_remove(const Host::Ptr& host);

  virtual void on_up(const Host::Ptr& host);

  virtual void on_down(const Host::Ptr& host);

  virtual LoadBalancingPolicy* new_instance() {
    return new DCAwarePolicy(local_dc_,
                             used_hosts_per_remote_dc_,
                             skip_remote_dcs_for_local_cl_);
  }

private:
  class PerDCHostMap {
  public:
    typedef std::map<std::string, CopyOnWriteHostVec> Map;
    typedef std::set<std::string> KeySet;

    PerDCHostMap() { uv_rwlock_init(&rwlock_); }
    ~PerDCHostMap() { uv_rwlock_destroy(&rwlock_); }

    void add_host_to_dc(const std::string& dc, const Host::Ptr& host);
    void remove_host_from_dc(const std::string& dc, const Host::Ptr& host);
    const CopyOnWriteHostVec& get_hosts(const std::string& dc) const;
    void copy_dcs(KeySet* dcs) const;

  private:
    Map map_;
    mutable uv_rwlock_t rwlock_;

  private:
    DISALLOW_COPY_AND_ASSIGN(PerDCHostMap);
  };

  const CopyOnWriteHostVec& get_local_dc_hosts() const;
  void get_remote_dcs(PerDCHostMap::KeySet* remote_dcs) const;

  class DCAwareQueryPlan : public QueryPlan {
  public:
    DCAwareQueryPlan(const DCAwarePolicy* policy,
                     CassConsistency cl,
                     size_t start_index);

    virtual Host::Ptr compute_next();

  private:
    const DCAwarePolicy* policy_;
    CassConsistency cl_;
    CopyOnWriteHostVec hosts_;
    ScopedPtr<PerDCHostMap::KeySet> remote_dcs_;
    size_t local_remaining_;
    size_t remote_remaining_;
    size_t index_;
  };

  std::string local_dc_;
  size_t used_hosts_per_remote_dc_;
  bool skip_remote_dcs_for_local_cl_;

  CopyOnWriteHostVec local_dc_live_hosts_;
  PerDCHostMap per_remote_dc_live_hosts_;
  size_t index_;

private:
  DISALLOW_COPY_AND_ASSIGN(DCAwarePolicy);
};

} // namespace cass

#endif
