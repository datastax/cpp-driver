/*
  Copyright (c) DataStax, Inc.
  Copyright (c) 2022 Kiwi.com

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

#ifndef DATASTAX_INTERNAL_RACK_AWARE_POLICY_HPP
#define DATASTAX_INTERNAL_RACK_AWARE_POLICY_HPP

#include "host.hpp"
#include "load_balancing.hpp"
#include "map.hpp"
#include "round_robin_policy.hpp"
#include "scoped_lock.hpp"
#include "scoped_ptr.hpp"
#include "set.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace core {

class RackAwarePolicy : public LoadBalancingPolicy {
public:
  RackAwarePolicy(const String& local_dc = "", const String &local_rack = "");

  ~RackAwarePolicy();

  virtual void init(const Host::Ptr& connected_host, const HostMap& hosts, Random* random,
                    const String& local_dc, const String& local_rack);

  virtual CassHostDistance distance(const Host::Ptr& host) const;

  virtual QueryPlan* new_query_plan(const String& keyspace, RequestHandler* request_handler,
                                    const TokenMap* token_map);

  virtual bool is_host_up(const Address& address) const;

  virtual void on_host_added(const Host::Ptr& host);
  virtual void on_host_removed(const Host::Ptr& host);
  virtual void on_host_up(const Host::Ptr& host);
  virtual void on_host_down(const Address& address);

  virtual const String& local_dc() const;
  virtual const String& local_rack() const;

  virtual LoadBalancingPolicy* new_instance() {
    return new RackAwarePolicy(local_dc_, local_rack_);
  }

private:
  class PerKeyHostMap {
  public:
    typedef internal::Map<String, CopyOnWriteHostVec> Map;
    typedef Set<String> KeySet;

    PerKeyHostMap()
        : no_hosts_(new HostVec()) {
      uv_rwlock_init(&rwlock_);
    }
    ~PerKeyHostMap() { uv_rwlock_destroy(&rwlock_); }

    void add_host_to_key(const String& key, const Host::Ptr& host);
    void remove_host_from_key(const String& key, const Host::Ptr& host);
    bool remove_host(const Address& address);
    const CopyOnWriteHostVec& get_hosts(const String& key) const;
    void copy_keys(KeySet* keys) const;

  private:
    Map map_;
    mutable uv_rwlock_t rwlock_;
    const CopyOnWriteHostVec no_hosts_;

  private:
    DISALLOW_COPY_AND_ASSIGN(PerKeyHostMap);
  };

  const CopyOnWriteHostVec& get_local_dc_hosts() const;
  void get_remote_dcs(PerKeyHostMap::KeySet* remote_dcs) const;

public:
  class RackAwareQueryPlan : public QueryPlan {
  public:
    RackAwareQueryPlan(const RackAwarePolicy* policy, CassConsistency cl, size_t start_index);

    virtual Host::Ptr compute_next();

  private:
    const RackAwarePolicy* policy_;
    CassConsistency cl_;
    CopyOnWriteHostVec hosts_;
    ScopedPtr<PerKeyHostMap::KeySet> remote_racks_;
    ScopedPtr<PerKeyHostMap::KeySet> remote_dcs_;
    size_t local_remaining_;
    size_t remote_remaining_;
    size_t index_;
  };

private:
  mutable uv_rwlock_t available_rwlock_;
  AddressSet available_;

  String local_dc_;
  String local_rack_;

  CopyOnWriteHostVec local_rack_live_hosts_;
  // remote rack, local dc
  PerKeyHostMap per_remote_rack_live_hosts_;
  PerKeyHostMap per_remote_dc_live_hosts_;
  size_t index_;

private:
  DISALLOW_COPY_AND_ASSIGN(RackAwarePolicy);
};

}}} // namespace datastax::internal::core

#endif
