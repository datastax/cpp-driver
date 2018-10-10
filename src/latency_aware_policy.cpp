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

#include "latency_aware_policy.hpp"

#include "get_time.hpp"
#include "logger.hpp"

#include <algorithm>
#include <iterator>

namespace cass {

void LatencyAwarePolicy::init(const Host::Ptr& connected_host,
                              const HostMap& hosts,
                              Random* random) {
  hosts_->reserve(hosts.size());
  std::transform(hosts.begin(), hosts.end(), std::back_inserter(*hosts_), GetHost());
  for (HostMap::const_iterator i = hosts.begin(),
       end = hosts.end(); i != end; ++i) {
    i->second->enable_latency_tracking(settings_.scale_ns, settings_.min_measured);
  }
  ChainedLoadBalancingPolicy::init(connected_host, hosts, random);
}

void LatencyAwarePolicy::register_handles(uv_loop_t* loop) {
  start_timer(loop);
}

void LatencyAwarePolicy::close_handles() {
  timer_.stop();
}

QueryPlan* LatencyAwarePolicy::new_query_plan(const String& keyspace,
                                              RequestHandler* request_handler,
                                              const TokenMap* token_map) {
  return Memory::allocate<LatencyAwareQueryPlan>(this,
                                                 child_policy_->new_query_plan(keyspace,
                                                                               request_handler,
                                                                               token_map));
}

void LatencyAwarePolicy::on_add(const Host::Ptr& host) {
  host->enable_latency_tracking(settings_.scale_ns, settings_.min_measured);
  add_host(hosts_, host);
  ChainedLoadBalancingPolicy::on_add(host);
}

void LatencyAwarePolicy::on_remove(const Host::Ptr& host) {
  remove_host(hosts_, host);
  ChainedLoadBalancingPolicy::on_remove(host);
}

void LatencyAwarePolicy::on_up(const Host::Ptr& host) {
  add_host(hosts_, host);
  ChainedLoadBalancingPolicy::on_up(host);
}

void LatencyAwarePolicy::on_down(const Host::Ptr& host) {
  remove_host(hosts_, host);
  ChainedLoadBalancingPolicy::on_down(host);
}

void LatencyAwarePolicy::start_timer(uv_loop_t* loop) {
  timer_.start(loop, settings_.update_rate_ms,
               bind_callback(&LatencyAwarePolicy::on_timer, this));
}

void LatencyAwarePolicy::on_timer(Timer* timer) {
  const CopyOnWriteHostVec& hosts(hosts_);

  int64_t new_min_average = CASS_INT64_MAX;
  int64_t now = uv_hrtime();

  for (HostVec::const_iterator i = hosts->begin(),
       end = hosts->end(); i != end; ++i) {
    TimestampedAverage latency = (*i)->get_current_average();
    if (latency.average >= 0
        && latency.num_measured >= settings_.min_measured
        && (now - latency.timestamp) <= settings_.retry_period_ns) {
      new_min_average = std::min(new_min_average, latency.average);
    }
  }

  if (new_min_average != CASS_INT64_MAX) {
    LOG_TRACE("Calculated new minimum: %f", static_cast<double>(new_min_average) / 1e6);
    min_average_.store(new_min_average);
  }

  start_timer(timer_.loop());
}

Host::Ptr LatencyAwarePolicy::LatencyAwareQueryPlan::compute_next() {
  int64_t min = policy_->min_average_.load();
  const Settings& settings = policy_->settings_;
  uint64_t now = uv_hrtime();

  Host::Ptr host;
  while ((host = child_plan_->compute_next())) {
    TimestampedAverage latency = host->get_current_average();

    if (min < 0 ||
        latency.average < 0 ||
        latency.num_measured < settings.min_measured ||
        (now - latency.timestamp) > settings.retry_period_ns) {
      return host;
    }

    if (latency.average <= static_cast<int64_t>(settings.exclusion_threshold * min)) {
      return host;
    }

    skipped_.push_back(host);
  }

  if (skipped_index_ < skipped_.size()) {
    return skipped_[skipped_index_++];
  }

  return Host::Ptr();
}

} // namespace cass
