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

#ifndef DATASTAX_INTERNAL_LATENCY_AWARE_POLICY_HPP
#define DATASTAX_INTERNAL_LATENCY_AWARE_POLICY_HPP

#include "atomic.hpp"
#include "load_balancing.hpp"
#include "macros.hpp"
#include "scoped_ptr.hpp"
#include "timer.hpp"

namespace datastax { namespace internal { namespace core {

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
      , settings_(settings)
      , hosts_(new HostVec()) {}

  virtual ~LatencyAwarePolicy() {}

  virtual void init(const Host::Ptr& connected_host, const HostMap& hosts, Random* random,
                    const String& local_dc);

  virtual void register_handles(uv_loop_t* loop);
  virtual void close_handles();

  virtual QueryPlan* new_query_plan(const String& keyspace, RequestHandler* request_handler,
                                    const TokenMap* token_map);

  virtual LoadBalancingPolicy* new_instance() {
    return new LatencyAwarePolicy(child_policy_->new_instance(), settings_);
  }

  virtual void on_host_added(const Host::Ptr& host);
  virtual void on_host_removed(const Host::Ptr& host);

public:
  // Testing only
  int64_t min_average() const { return min_average_.load(); }

private:
  void start_timer(uv_loop_t* loop);

private:
  class LatencyAwareQueryPlan : public QueryPlan {
  public:
    LatencyAwareQueryPlan(LatencyAwarePolicy* policy, QueryPlan* child_plan)
        : policy_(policy)
        , child_plan_(child_plan)
        , skipped_index_(0) {}

    Host::Ptr compute_next();

  private:
    LatencyAwarePolicy* policy_;
    ScopedPtr<QueryPlan> child_plan_;

    HostVec skipped_;
    size_t skipped_index_;
  };

  void on_timer(Timer* timer);

  Atomic<int64_t> min_average_;
  Timer timer_;
  Settings settings_;
  CopyOnWriteHostVec hosts_;

private:
  DISALLOW_COPY_AND_ASSIGN(LatencyAwarePolicy);
};

}}} // namespace datastax::internal::core

#endif
