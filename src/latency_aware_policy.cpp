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

#include "latency_aware_policy.hpp"

#include "get_time.hpp"
#include "logger.hpp"

namespace cass {

void LatencyAwarePolicy::init(const SharedRefPtr<Host>& connected_host, const HostMap& hosts) {
  copy_hosts(hosts, hosts_);
  for (HostMap::const_iterator i = hosts.begin(),
       end = hosts.end(); i != end; ++i) {
    i->second->enable_latency_tracking(settings_.scale_ns, settings_.min_measured);
  }
  ChainedLoadBalancingPolicy::init(connected_host, hosts);
}

void LatencyAwarePolicy::register_handles(uv_loop_t* loop) {
  calculate_min_average_task_ = PeriodicTask::start(loop,
                                                    settings_.update_rate_ms,
                                                    this,
                                                    LatencyAwarePolicy::on_work,
                                                    LatencyAwarePolicy::on_after_work);
}

void LatencyAwarePolicy::close_handles() {
  if (calculate_min_average_task_ != NULL) {
    PeriodicTask::stop(calculate_min_average_task_);
  }
}

QueryPlan* LatencyAwarePolicy::new_query_plan(const std::string& connected_keyspace,
                                              const Request* request,
                                              const TokenMap& token_map) {
  return new LatencyAwareQueryPlan(this,
                                   child_policy_->new_query_plan(connected_keyspace, request, token_map));
}

void LatencyAwarePolicy::on_add(const SharedRefPtr<Host>& host) {
  host->enable_latency_tracking(settings_.scale_ns, settings_.min_measured);
  add_host(hosts_, host);
  ChainedLoadBalancingPolicy::on_add(host);
}

void LatencyAwarePolicy::on_remove(const SharedRefPtr<Host>& host) {
  remove_host(hosts_, host);
  ChainedLoadBalancingPolicy::on_remove(host);
}

void LatencyAwarePolicy::on_up(const SharedRefPtr<Host>& host) {
  add_host(hosts_, host);
  ChainedLoadBalancingPolicy::on_up(host);
}

void LatencyAwarePolicy::on_down(const SharedRefPtr<Host>& host) {
  remove_host(hosts_, host);
  ChainedLoadBalancingPolicy::on_down(host);
}

SharedRefPtr<Host> LatencyAwarePolicy::LatencyAwareQueryPlan::compute_next() {
  int64_t min = policy_->min_average_.load();
  const Settings& settings = policy_->settings_;
  uint64_t now = uv_hrtime();

  SharedRefPtr<Host> host;
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

  return SharedRefPtr<Host>();
}

void LatencyAwarePolicy::on_work(PeriodicTask* task) {
  LatencyAwarePolicy* policy = static_cast<LatencyAwarePolicy*>(task->data());

  const Settings& settings = policy->settings_;
  const CopyOnWriteHostVec& hosts = policy->hosts_;

  int64_t new_min_average = std::numeric_limits<int64_t>::max();
  int64_t now = uv_hrtime();

  for (HostVec::const_iterator i = hosts->begin(),
       end = hosts->end(); i != end; ++i) {
    TimestampedAverage latency = (*i)->get_current_average();
    if (latency.average >= 0
        && latency.num_measured >= settings.min_measured
        && (now - latency.timestamp) <= settings.retry_period_ns) {
      new_min_average = std::min(new_min_average, latency.average);
    }
  }

  if (new_min_average != std::numeric_limits<int64_t>::max()) {
    LOG_TRACE("Calculated new minimum: %f", static_cast<double>(new_min_average) / 1e6);
    policy->min_average_.store(new_min_average);
  }
}

void LatencyAwarePolicy::on_after_work(PeriodicTask* task) {
  // no-op
}

} // namespace cass
