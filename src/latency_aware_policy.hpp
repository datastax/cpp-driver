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

#ifndef __CASS_LATENCY_AWARE_POLICY_HPP_INCLUDED__
#define __CASS_LATENCY_AWARE_POLICY_HPP_INCLUDED__

#include "atomic.hpp"
#include "load_balancing.hpp"
#include "macros.hpp"
#include "periodic_task.hpp"
#include "scoped_ptr.hpp"

namespace cass {

class LatencyAwarePolicy : public ChainedLoadBalancingPolicy {
public:
  struct Settings {
    Settings()
      : exclusion_threshold(2.0)
      , scale_ns(100LL * 1000LL * 1000LL)
      , retry_period_ns(10LL * 1000LL * 1000LL * 1000LL)
      , update_rate_ms(100LL)
      , min_measured(50LL) {}

    double exclusion_threshold;
    uint64_t scale_ns;
    uint64_t retry_period_ns;
    uint64_t update_rate_ms;
    uint64_t min_measured;
  };

  LatencyAwarePolicy(LoadBalancingPolicy* child_policy, const Settings& settings)
    : ChainedLoadBalancingPolicy(child_policy)
    , min_average_(-1)
    , calculate_min_average_task_(NULL)
    , settings_(settings)
    , hosts_(new HostVec) {}

  virtual ~LatencyAwarePolicy() {}

  virtual void init(const SharedRefPtr<Host>& connected_host, const HostMap& hosts);

  virtual void register_handles(uv_loop_t* loop);
  virtual void close_handles();

  virtual QueryPlan* new_query_plan(const std::string& connected_keyspace,
                                    const Request* request,
                                    const TokenMap& token_map);

  virtual LoadBalancingPolicy* new_instance() {
    return new LatencyAwarePolicy(child_policy_->new_instance(), settings_);
  }

  virtual void on_add(const SharedRefPtr<Host>& host);
  virtual void on_remove(const SharedRefPtr<Host>& host);
  virtual void on_up(const SharedRefPtr<Host>& host);
  virtual void on_down(const SharedRefPtr<Host>& host);

public:
  // Testing only
  int64_t min_average() const {
    return min_average_.load();
  }

private:
  class LatencyAwareQueryPlan : public QueryPlan {
  public:
    LatencyAwareQueryPlan(LatencyAwarePolicy* policy, QueryPlan* child_plan)
      : policy_(policy)
      , child_plan_(child_plan)
      , skipped_index_(0) {}

    SharedRefPtr<Host> compute_next();

  private:
    LatencyAwarePolicy* policy_;
    ScopedPtr<QueryPlan> child_plan_;

    HostVec skipped_;
    size_t skipped_index_;
  };

  static void on_work(PeriodicTask* task);
  static void on_after_work(PeriodicTask* task);

  Atomic<int64_t> min_average_;
  PeriodicTask* calculate_min_average_task_;
  Settings settings_;
  CopyOnWriteHostVec hosts_;

private:
  DISALLOW_COPY_AND_ASSIGN(LatencyAwarePolicy);
};

} // namespace cass

#endif
