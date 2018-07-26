/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __CASS_EXECUTION_PROFILE_HPP_INCLUDED__
#define __CASS_EXECUTION_PROFILE_HPP_INCLUDED__

#include "blacklist_policy.hpp"
#include "blacklist_dc_policy.hpp"
#include "cassandra.h"
#include "constants.hpp"
#include "dc_aware_policy.hpp"
#include "host_targeting_policy.hpp"
#include "latency_aware_policy.hpp"
#include "dense_hash_map.hpp"
#include "string.hpp"
#include "token_aware_policy.hpp"
#include "utils.hpp"
#include "whitelist_policy.hpp"
#include "whitelist_dc_policy.hpp"

namespace cass {

class ExecutionProfile {
public:
  typedef DenseHashMap<String, ExecutionProfile> Map;

  ExecutionProfile()
    : request_timeout_ms_(CASS_UINT64_MAX)
    , consistency_(CASS_CONSISTENCY_UNKNOWN)
    , serial_consistency_(CASS_CONSISTENCY_UNKNOWN)
    , host_targeting_(false)
    , latency_aware_routing_(false)
    , token_aware_routing_(true)
    , token_aware_routing_shuffle_replicas_(true)
    , load_balancing_policy_(NULL)
    , retry_policy_(NULL) { }

  uint64_t request_timeout_ms() const { return request_timeout_ms_;  }

  void set_request_timeout(uint64_t timeout_ms) {
    request_timeout_ms_ = timeout_ms;
  }

  CassConsistency consistency() const { return consistency_; }

  void set_consistency(CassConsistency consistency) {
    consistency_ = consistency;
  }

  CassConsistency serial_consistency() const { return serial_consistency_; }

  void set_serial_consistency(CassConsistency serial_consistency) {
    serial_consistency_ = serial_consistency;
  }

  ContactPointList& blacklist() {
    return blacklist_;
  }

  DcList& blacklist_dc() {
    return blacklist_dc_;
  }

  bool host_targeting() const { return host_targeting_; }

  void set_host_targeting(bool is_host_targeting) {
    host_targeting_ = is_host_targeting;
  }

  bool latency_aware() const { return latency_aware_routing_; }

  void set_latency_aware_routing(bool is_latency_aware) {
    latency_aware_routing_ = is_latency_aware;
  }

  void set_latency_aware_routing_settings(const LatencyAwarePolicy::Settings& settings) {
    latency_aware_routing_settings_ = settings;
  }

  bool token_aware_routing() const { return token_aware_routing_; }

  void set_token_aware_routing(bool is_token_aware) {
    token_aware_routing_ = is_token_aware;
  }

  void set_token_aware_routing_shuffle_replicas(bool shuffle_replicas) {
    token_aware_routing_shuffle_replicas_ = shuffle_replicas;
  }

  ContactPointList& whitelist() {
    return whitelist_;
  }

  DcList& whitelist_dc() {
    return whitelist_dc_;
  }

  const LoadBalancingPolicy::Ptr& load_balancing_policy() const {
    return load_balancing_policy_;
  }

  void set_load_balancing_policy(LoadBalancingPolicy* lbp) {
    load_balancing_policy_.reset(lbp);
  }

  void build_load_balancing_policy() {
    // The base LBP can be augmented by special wrappers (whitelist,
    // token aware, latency aware)
    if (load_balancing_policy_) {
      LoadBalancingPolicy* chain = load_balancing_policy_->new_instance();

      if (!blacklist_.empty()) {
        chain = Memory::allocate<BlacklistPolicy>(chain, blacklist_);
      }
      if (!whitelist_.empty()) {
        chain = Memory::allocate<WhitelistPolicy>(chain, whitelist_);
      }
      if (!blacklist_dc_.empty()) {
        chain = Memory::allocate<BlacklistDCPolicy>(chain, blacklist_dc_);
      }
      if (!whitelist_dc_.empty()) {
        chain = Memory::allocate<WhitelistDCPolicy>(chain, whitelist_dc_);
      }
      if (token_aware_routing()) {
        chain = Memory::allocate<TokenAwarePolicy>(chain,
                                                   token_aware_routing_shuffle_replicas_);
      }
      if (latency_aware()) {
        chain = Memory::allocate<LatencyAwarePolicy>(chain,
                                                     latency_aware_routing_settings_);
      }
      if (host_targeting()) {
        chain = Memory::allocate<HostTargetingPolicy>(chain);
      }

      load_balancing_policy_.reset(chain);
    }
  }

  const RetryPolicy::Ptr& retry_policy() const {
    return retry_policy_;
  }

  void set_retry_policy(RetryPolicy* retry_policy) {
    retry_policy_.reset(retry_policy);
  }

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
  bool host_targeting_;
  bool latency_aware_routing_;
  LatencyAwarePolicy::Settings latency_aware_routing_settings_;
  bool token_aware_routing_;
  bool token_aware_routing_shuffle_replicas_;
  ContactPointList whitelist_;
  DcList whitelist_dc_;
  LoadBalancingPolicy::Ptr load_balancing_policy_;
  RetryPolicy::Ptr retry_policy_;
  SpeculativeExecutionPolicy::Ptr speculative_execution_policy_;
};

} // namespace cass

EXTERNAL_TYPE(cass::ExecutionProfile, CassExecProfile)

#endif // __CASS_EXECUTION_PROFILE_HPP_INCLUDED__
