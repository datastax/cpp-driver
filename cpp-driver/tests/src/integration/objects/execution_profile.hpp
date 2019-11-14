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

#ifndef __TEST_EXECUTION_PROFILE_HPP__
#define __TEST_EXECUTION_PROFILE_HPP__
#include "cassandra.h"

#include "objects/object_base.hpp"
#include "objects/retry_policy.hpp"

#include <string>

#include <gtest/gtest.h>

namespace test { namespace driver {

/**
 * Wrapped execution profile object (builder)
 */
class ExecutionProfile : public Object<CassExecProfile, cass_execution_profile_free> {
public:
  typedef std::map<std::string, ExecutionProfile> Map;

  /**
   * Create the execution profile for the builder object
   */
  ExecutionProfile()
      : Object<CassExecProfile, cass_execution_profile_free>(cass_execution_profile_new()) {}

  /**
   * Create the execution profile for the builder object
   *
   * @param profile Already defined execution profile object to utilize
   */
  ExecutionProfile(CassExecProfile* profile)
      : Object<CassExecProfile, cass_execution_profile_free>(profile) {}

  /**
   * Create the execution profile object from a shared reference
   *
   * @param profile Shared reference
   */
  ExecutionProfile(Ptr profile)
      : Object<CassExecProfile, cass_execution_profile_free>(profile) {}

  /**
   * Build/Create the execution profile
   *
   * @return Execution profile object
   */
  static ExecutionProfile build() { return ExecutionProfile(); }

  /**
   * Append/Assign/Set the blacklist hosts for statement/batch execution
   *
   * @param hosts A comma delimited list of hosts addresses
   * @return Execution profile object
   */
  ExecutionProfile& with_blacklist_filtering(const std::string& hosts) {
    EXPECT_EQ(CASS_OK, cass_execution_profile_set_blacklist_filtering(get(), hosts.c_str()));
    return *this;
  }

  /**
   * Append/Assign/Set the blacklist data centers for statement/batch execution
   *
   * @param dcs A comma delimited list of data center names
   * @return Execution profile object
   */
  ExecutionProfile& with_blacklist_dc_filtering(const std::string& dcs) {
    EXPECT_EQ(CASS_OK, cass_execution_profile_set_blacklist_dc_filtering(get(), dcs.c_str()));
    return *this;
  }

  /**
   * Assign/Set the profile consistency level for statement/batch execution
   *
   * @param consistency Consistency to use for the profile
   * @return Execution profile object
   */
  ExecutionProfile& with_consistency(CassConsistency consistency) {
    EXPECT_EQ(CASS_OK, cass_execution_profile_set_consistency(get(), consistency));
    return *this;
  }

  /**
   * Enable/Disable latency aware routing for statement/batch execution
   *
   * @param enable True if latency aware routing should be enabled; false
   *               otherwise (default: true)
   * @return Execution profile object
   */
  ExecutionProfile& with_latency_aware_routing(bool enable = true) {
    EXPECT_EQ(CASS_OK, cass_execution_profile_set_latency_aware_routing(
                           get(), (enable == true ? cass_true : cass_false)));
    return *this;
  }

  /**
   * Latency aware routing settings to utilize for statement/batch execution
   *
   * @param exclusion_threshold Controls how much worse the latency must be
   *                            compared to the average latency of the best
   *                            performing node before it penalized
   * @param scale_ms Controls the weight given to older latencies when
   *                 calculating the average latency of a node. A bigger scale
   *                 will give more weight to older latency measurements
   * @param retry_period_ms The amount of time a node is penalized by the policy
   *                        before being given a second chance when the current
   *                        average latency exceeds the calculated threshold
   * @param update_rate_ms Rate at  which the best average latency is
   *                       recomputed (in milliseconds)
   * @param min_measured The minimum number of measurements per-host required
   *                     to be considered by the policy
   * @return Execution profile object
   */
  ExecutionProfile& with_latency_aware_routing_settings(double exclusion_threshold,
                                                        uint64_t scale_ms, uint64_t retry_period_ms,
                                                        uint64_t update_rate_ms,
                                                        uint64_t min_measured) {
    EXPECT_EQ(CASS_OK, cass_execution_profile_set_latency_aware_routing_settings(
                           get(), exclusion_threshold, scale_ms, retry_period_ms, update_rate_ms,
                           min_measured));
    return *this;
  }

  /**
   * Enable data center aware load balance policy for statement/batch execution
   *
   * @param local_dc The primary data center to try first
   * @param used_hosts_per_remote_dc The number of hosts used in each remote
   *                                 data center if no hosts are available in
   *                                 the local data center
   * @param allow_remote_dcs_for_local_cl True if remote hosts are to be used as
   *                                      local data centers when no local data
   *                                      center is available and consistency
   *                                      levels are LOCAL_ONE or LOCAL_QUORUM;
   *                                      otherwise false
   * @return Execution profile object
   */
  ExecutionProfile& with_load_balance_dc_aware(const std::string& local_dc,
                                               size_t used_hosts_per_remote_dc,
                                               bool allow_remote_dcs_for_local_cl) {
    EXPECT_EQ(CASS_OK, cass_execution_profile_set_load_balance_dc_aware(
                           get(), local_dc.c_str(), used_hosts_per_remote_dc,
                           (allow_remote_dcs_for_local_cl == true ? cass_true : cass_false)));
    return *this;
  }

  /**
   * Enable round robin load balance policy for statement/batch execution
   *
   * @return Execution profile object
   */
  ExecutionProfile& with_load_balance_round_robin() {
    EXPECT_EQ(CASS_OK, cass_execution_profile_set_load_balance_round_robin(get()));
    return *this;
  }

  /**
   * Assign/Set the profile no speculative executions
   *
   * @return Execution profile object
   */
  ExecutionProfile& with_no_speculative_execution_policy() {
    EXPECT_EQ(CASS_OK, cass_execution_profile_set_no_speculative_execution_policy(get()));
    return *this;
  }

  /**
   * Assign/Set the profile timeout for statement/batch execution
   *
   * @param timeout_ms Timeout in milliseconds
   * @return Execution profile object
   */
  ExecutionProfile& with_request_timeout(uint64_t timeout_ms) {
    EXPECT_EQ(CASS_OK, cass_execution_profile_set_request_timeout(get(), timeout_ms));
    return *this;
  }

  /**
   * Assign/Set the profile retry policy for statement/batch execution
   *
   * @param retry_policy Retry policy
   * @return Execution profile object
   */
  ExecutionProfile& with_retry_policy(RetryPolicy retry_policy) {
    EXPECT_EQ(CASS_OK, cass_execution_profile_set_retry_policy(get(), retry_policy.get()));
    return *this;
  }

  /**
   * Assign/Set the profile serial consistency level for statement/batch
   * execution
   *
   * @param serial_consistency Serial consistency to use for the profile
   * @return Execution profile object
   */
  ExecutionProfile& with_serial_consistency(CassConsistency serial_consistency) {
    EXPECT_EQ(CASS_OK, cass_execution_profile_set_serial_consistency(get(), serial_consistency));
    return *this;
  }

  /**
   * Assign/Set the profile constant speculative executions
   *
   * @param constant_delay_ms Constant delay before speculatively executing
   *                          idempotent request
   * @param max_speculative_executions Maximum number of times to speculatively
   *                                   execute a idempotent request
   * @return Execution profile object
   */
  ExecutionProfile& with_constant_speculative_execution_policy(cass_int64_t constant_delay_ms,
                                                               int max_speculative_executions) {
    EXPECT_EQ(CASS_OK, cass_execution_profile_set_constant_speculative_execution_policy(
                           get(), constant_delay_ms, max_speculative_executions));
    return *this;
  }

  /**
   * Enable/Disable token aware routing for statement/batch execution
   *
   * @param enable True if token aware routing should be enabled; false
   *               otherwise (default: true)
   * @return Execution profile object
   */
  ExecutionProfile& with_token_aware_routing(bool enable = true) {
    EXPECT_EQ(CASS_OK, cass_execution_profile_set_token_aware_routing(
                           get(), (enable == true ? cass_true : cass_false)));
    return *this;
  }

  /**
   * Enable/Disable replica shuffling when using token aware routing for
   * statement/batch execution
   *
   * @param enable True if token aware routing replica shuffling should be
   *               enabled; false otherwise (default: true)
   * @return Execution profile object
   */
  ExecutionProfile& with_token_aware_routing_shuffle_replicas(bool enable = true) {
    EXPECT_EQ(CASS_OK, cass_execution_profile_set_token_aware_routing_shuffle_replicas(
                           get(), (enable == true ? cass_true : cass_false)));
    return *this;
  }

  /**
   * Append/Assign/Set the whitelist hosts for statement/batch execution
   *
   * @param hosts A comma delimited list of hosts addresses
   * @return Execution profile object
   */
  ExecutionProfile& with_whitelist_filtering(const std::string& hosts) {
    EXPECT_EQ(CASS_OK, cass_execution_profile_set_whitelist_filtering(get(), hosts.c_str()));
    return *this;
  }

  /**
   * Append/Assign/Set the whitelist data centers for statement/batch execution
   *
   * @param dcs A comma delimited list of data center names
   * @return Execution profile object
   */
  ExecutionProfile& with_whitelist_dc_filtering(const std::string& dcs) {
    EXPECT_EQ(CASS_OK, cass_execution_profile_set_whitelist_dc_filtering(get(), dcs.c_str()));
    return *this;
  }
};

}} // namespace test::driver

#endif // __TEST_EXECUTION_PROFILE_HPP__
