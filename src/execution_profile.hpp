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

#ifndef DATASTAX_INTERNAL_EXECUTION_PROFILE_HPP
#define DATASTAX_INTERNAL_EXECUTION_PROFILE_HPP

#include "allocated.hpp"
#include "blacklist_dc_policy.hpp"
#include "blacklist_policy.hpp"
#include "cassandra.h"
#include "constants.hpp"
#include "dc_aware_policy.hpp"
#include "dense_hash_map.hpp"
#include "latency_aware_policy.hpp"
#include "speculative_execution.hpp"
#include "string.hpp"
#include "token_aware_policy.hpp"
#include "utils.hpp"
#include "whitelist_dc_policy.hpp"
#include "whitelist_policy.hpp"

namespace datastax { namespace internal { namespace core {

class ExecutionProfile : public Allocated {
public:
  typedef DenseHashMap<String, ExecutionProfile> Map;

  ExecutionProfile()
      : request_timeout_ms_(CASS_UINT64_MAX)
      , consistency_(CASS_CONSISTENCY_UNKNOWN)
      , serial_consistency_(CASS_CONSISTENCY_UNKNOWN)
      , latency_aware_routing_(false)
      , token_aware_routing_(true)
      , token_aware_routing_shuffle_replicas_(true) {}

  uint64_t request_timeout_ms() const { return request_timeout_ms_; }

  void set_request_timeout(uint64_t timeout_ms) { request_timeout_ms_ = timeout_ms; }

  CassConsistency consistency() const { return consistency_; }

  void set_consistency(CassConsistency consistency) { consistency_ = consistency; }

  CassConsistency serial_consistency() const { return serial_consistency_; }

  void set_serial_consistency(CassConsistency serial_consistency) {
    serial_consistency_ = serial_consistency;
  }

  ContactPointList& blacklist() { return blacklist_; }
  const ContactPointList& blacklist() const { return blacklist_; }

  DcList& blacklist_dc() { return blacklist_dc_; }
  const DcList& blacklist_dc() const { return blacklist_dc_; }

  bool latency_aware() const { return latency_aware_routing_; }

  void set_latency_aware_routing(bool is_latency_aware) {
    latency_aware_routing_ = is_latency_aware;
  }

  void set_latency_aware_routing_settings(const LatencyAwarePolicy::Settings& settings) {
    latency_aware_routing_settings_ = settings;
  }

  LatencyAwarePolicy::Settings latency_aware_routing_settings() const {
    return latency_aware_routing_settings_;
  }

  bool token_aware_routing() const { return token_aware_routing_; }

  void set_token_aware_routing(bool is_token_aware) { token_aware_routing_ = is_token_aware; }

  void set_token_aware_routing_shuffle_replicas(bool shuffle_replicas) {
    token_aware_routing_shuffle_replicas_ = shuffle_replicas;
  }

  bool token_aware_routing_shuffle_replicas() const {
    return token_aware_routing_shuffle_replicas_;
  }

  ContactPointList& whitelist() { return whitelist_; }
  const ContactPointList& whitelist() const { return whitelist_; }

  DcList& whitelist_dc() { return whitelist_dc_; }
  const DcList& whitelist_dc() const { return whitelist_dc_; }

  const LoadBalancingPolicy::Ptr& load_balancing_policy() const { return load_balancing_policy_; }

  void set_load_balancing_policy(LoadBalancingPolicy* lbp) {
    base_load_balancing_policy_.reset(lbp);
  }

  /**
   * Use another profile's load balancing policy. This is used to override profiles that don't have
   * policies of their own with the default profile's load balancing policy.
   *
   * @param lbp The other profile's load balancing policy chain.
   */
  void use_load_balancing_policy(const LoadBalancingPolicy::Ptr& lbp) {
    assert(!base_load_balancing_policy_ &&
           "The profile should have a no base load balancing policy");
    load_balancing_policy_ = lbp;
  }

  void build_load_balancing_policy() {
    // The base LBP can be augmented by special wrappers (whitelist,
    // token aware, latency aware)
    if (base_load_balancing_policy_) {
      LoadBalancingPolicy* chain = base_load_balancing_policy_->new_instance();

      if (!blacklist_.empty()) {
        chain = new BlacklistPolicy(chain, blacklist_);
      }
      if (!whitelist_.empty()) {
        chain = new WhitelistPolicy(chain, whitelist_);
      }
      if (!blacklist_dc_.empty()) {
        chain = new BlacklistDCPolicy(chain, blacklist_dc_);
      }
      if (!whitelist_dc_.empty()) {
        chain = new WhitelistDCPolicy(chain, whitelist_dc_);
      }
      if (token_aware_routing()) {
        chain = new TokenAwarePolicy(chain, token_aware_routing_shuffle_replicas_);
      }
      if (latency_aware()) {
        chain = new LatencyAwarePolicy(chain, latency_aware_routing_settings_);
      }

      load_balancing_policy_.reset(chain);
    }
  }

  const RetryPolicy::Ptr& retry_policy() const { return retry_policy_; }

  void set_retry_policy(RetryPolicy* retry_policy) { retry_policy_.reset(retry_policy); }

  const SpeculativeExecutionPolicy::Ptr& speculative_execution_policy() const {
    return speculative_execution_policy_;
  }

  void set_speculative_execution_policy(SpeculativeExecutionPolicy* sep) {
    if (sep == NULL) return;
    speculative_execution_policy_.reset(sep);
  }

private:
  cass_uint64_t request_timeout_ms_;
  CassConsistency consistency_;
  CassConsistency serial_consistency_;
  ContactPointList blacklist_;
  DcList blacklist_dc_;
  bool latency_aware_routing_;
  LatencyAwarePolicy::Settings latency_aware_routing_settings_;
  bool token_aware_routing_;
  bool token_aware_routing_shuffle_replicas_;
  ContactPointList whitelist_;
  DcList whitelist_dc_;
  LoadBalancingPolicy::Ptr load_balancing_policy_;
  LoadBalancingPolicy::Ptr base_load_balancing_policy_;
  RetryPolicy::Ptr retry_policy_;
  SpeculativeExecutionPolicy::Ptr speculative_execution_policy_;
};

}}} // namespace datastax::internal::core

EXTERNAL_TYPE(datastax::internal::core::ExecutionProfile, CassExecProfile)

#endif // DATASTAX_INTERNAL_EXECUTION_PROFILE_HPP
