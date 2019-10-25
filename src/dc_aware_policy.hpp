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

#ifndef DATASTAX_INTERNAL_DC_AWARE_POLICY_HPP
#define DATASTAX_INTERNAL_DC_AWARE_POLICY_HPP

#include "host.hpp"
#include "load_balancing.hpp"
#include "map.hpp"
#include "round_robin_policy.hpp"
#include "scoped_lock.hpp"
#include "scoped_ptr.hpp"
#include "set.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace core {

class DCAwarePolicy : public LoadBalancingPolicy {
public:
  DCAwarePolicy(const String& local_dc = "", size_t used_hosts_per_remote_dc = 0,
                bool skip_remote_dcs_for_local_cl = true);

  ~DCAwarePolicy();

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

  virtual bool skip_remote_dcs_for_local_cl() const;
  virtual size_t used_hosts_per_remote_dc() const;
  virtual const String& local_dc() const;

  virtual LoadBalancingPolicy* new_instance() {
    return new DCAwarePolicy(local_dc_, used_hosts_per_remote_dc_, skip_remote_dcs_for_local_cl_);
  }

private:
  class PerDCHostMap {
  public:
    typedef internal::Map<String, CopyOnWriteHostVec> Map;
    typedef Set<String> KeySet;

    PerDCHostMap()
        : no_hosts_(new HostVec()) {
      uv_rwlock_init(&rwlock_);
    }
    ~PerDCHostMap() { uv_rwlock_destroy(&rwlock_); }

    void add_host_to_dc(const String& dc, const Host::Ptr& host);
    void remove_host_from_dc(const String& dc, const Host::Ptr& host);
    bool remove_host(const Address& address);
    const CopyOnWriteHostVec& get_hosts(const String& dc) const;
    void copy_dcs(KeySet* dcs) const;

  private:
    Map map_;
    mutable uv_rwlock_t rwlock_;
    const CopyOnWriteHostVec no_hosts_;

  private:
    DISALLOW_COPY_AND_ASSIGN(PerDCHostMap);
  };

  const CopyOnWriteHostVec& get_local_dc_hosts() const;
  void get_remote_dcs(PerDCHostMap::KeySet* remote_dcs) const;

public:
  class DCAwareQueryPlan : public QueryPlan {
  public:
    DCAwareQueryPlan(const DCAwarePolicy* policy, CassConsistency cl, size_t start_index);

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

private:
  mutable uv_rwlock_t available_rwlock_;
  AddressSet available_;

  String local_dc_;
  size_t used_hosts_per_remote_dc_;
  bool skip_remote_dcs_for_local_cl_;

  CopyOnWriteHostVec local_dc_live_hosts_;
  PerDCHostMap per_remote_dc_live_hosts_;
  size_t index_;

private:
  DISALLOW_COPY_AND_ASSIGN(DCAwarePolicy);
};

}}} // namespace datastax::internal::core

#endif
