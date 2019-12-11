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
#include <gtest/gtest.h>

#include "config.hpp"

using namespace datastax;
using namespace datastax::internal::core;

bool execution_profile(const Config& config, const String& name, ExecutionProfile& profile) {
  // Handle profile lookup
  const ExecutionProfile::Map& profiles = config.profiles();
  ExecutionProfile::Map::const_iterator it = profiles.find(name);
  if (it != profiles.end()) {
    profile = it->second;
    return true;
  }
  return false;
}

TEST(ExecutionProfileUnitTest, Consistency) {
  ExecutionProfile profile;
  ASSERT_EQ(CASS_CONSISTENCY_UNKNOWN, profile.consistency());

  Config config;
  config.set_execution_profile("profile", &profile);

  Config copy_config = config.new_instance();
  ExecutionProfile profile_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile", profile_lookup));
  ASSERT_EQ(CASS_CONSISTENCY_UNKNOWN, profile_lookup.consistency());
  ASSERT_EQ(CASS_CONSISTENCY_UNKNOWN, copy_config.default_profile().consistency());
}

TEST(ExecutionProfileUnitTest, SerialConsistency) {
  ExecutionProfile profile;
  ASSERT_EQ(CASS_CONSISTENCY_UNKNOWN, profile.serial_consistency());

  Config config;
  config.set_execution_profile("profile", &profile);

  Config copy_config = config.new_instance();
  ExecutionProfile profile_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile", profile_lookup));
  ASSERT_EQ(CASS_DEFAULT_SERIAL_CONSISTENCY, profile_lookup.serial_consistency());
  ASSERT_EQ(CASS_DEFAULT_SERIAL_CONSISTENCY, copy_config.default_profile().serial_consistency());
}

TEST(ExecutionProfileUnitTest, RequestTimeout) {
  ExecutionProfile profile;
  ASSERT_EQ(CASS_UINT64_MAX, profile.request_timeout_ms());

  Config config;
  config.set_execution_profile("profile", &profile);

  Config copy_config = config.new_instance();
  ExecutionProfile profile_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile", profile_lookup));
  ASSERT_EQ(CASS_DEFAULT_REQUEST_TIMEOUT_MS, profile_lookup.request_timeout_ms());
  ASSERT_EQ(CASS_DEFAULT_REQUEST_TIMEOUT_MS, copy_config.default_profile().request_timeout_ms());
}

TEST(ExecutionProfileUnitTest, NullLoadBalancingPolicy) {
  ExecutionProfile profile;
  profile.build_load_balancing_policy();

  ASSERT_TRUE(!profile.load_balancing_policy());
}

// TODO(CPP_404): Remove disabled tests case as the profiles LBPs are initialized
//               by the request processor init method now. The last assertion in
//               the test is invalid making the whole test invalid.
TEST(ExecutionProfileUnitTest, DISABLED_ClusterLoadBalancingPolicy) {
  ExecutionProfile profile;

  Config config;
  config.set_execution_profile("profile", &profile);

  Config copy_config = config.new_instance();
  ExecutionProfile profile_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile", profile_lookup));
  ASSERT_EQ(copy_config.default_profile().load_balancing_policy().get(),
            profile_lookup.load_balancing_policy().get());
}

// TODO(CPP_404): Remove disabled tests case as the profiles LBPs are initialized
//               by the request processor init method now The last assertion in
//               the test is invalid making the whole test invalid.
TEST(ExecutionProfileUnitTest, DISABLED_ClusterLoadBalancingPolicies) {
  ExecutionProfile profile_1;
  profile_1.set_load_balancing_policy(new DCAwarePolicy());
  ExecutionProfile profile_2;
  ExecutionProfile profile_3;
  profile_3.set_load_balancing_policy(new RoundRobinPolicy());

  Config config;
  config.set_execution_profile("profile_1", &profile_1);
  config.set_execution_profile("profile_2", &profile_2);
  config.set_execution_profile("profile_3", &profile_3);

  Config copy_config = config.new_instance();
  ExecutionProfile profile_1_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile_1", profile_1_lookup));
  ExecutionProfile profile_2_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile_2", profile_2_lookup));
  ExecutionProfile profile_3_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile_3", profile_3_lookup));
  const LoadBalancingPolicy* profile_1_lbp = profile_1_lookup.load_balancing_policy().get();
  const LoadBalancingPolicy* profile_2_lbp = profile_2_lookup.load_balancing_policy().get();
  const LoadBalancingPolicy* profile_3_lbp = profile_3_lookup.load_balancing_policy().get();
  EXPECT_NE(profile_1_lbp, profile_2_lbp);
  EXPECT_NE(profile_2_lbp, profile_3_lbp);
  EXPECT_NE(profile_3_lbp, profile_1_lbp);
  ASSERT_EQ(copy_config.default_profile().load_balancing_policy().get(), profile_2_lbp);
}

TEST(ExecutionProfileUnitTest, Blacklist) {
  ExecutionProfile profile;
  explode(String("0.0.0.0, 0.0.0.2, 0.0.0.4"), profile.blacklist());
  ASSERT_EQ(3u, profile.blacklist().size());
  ASSERT_STREQ("0.0.0.0", profile.blacklist().at(0).c_str());
  ASSERT_STREQ("0.0.0.2", profile.blacklist().at(1).c_str());
  ASSERT_STREQ("0.0.0.4", profile.blacklist().at(2).c_str());

  Config config;
  config.set_execution_profile("profile", &profile);

  Config copy_config = config.new_instance();
  ExecutionProfile profile_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile", profile_lookup));
  ASSERT_EQ(profile.blacklist().size(), profile_lookup.blacklist().size());
  ASSERT_STREQ(profile.blacklist().at(0).c_str(), profile_lookup.blacklist().at(0).c_str());
  ASSERT_STREQ(profile.blacklist().at(1).c_str(), profile_lookup.blacklist().at(1).c_str());
  ASSERT_STREQ(profile.blacklist().at(2).c_str(), profile_lookup.blacklist().at(2).c_str());
  ASSERT_EQ(0u, copy_config.default_profile().blacklist().size());
}

TEST(ExecutionProfileUnitTest, BlacklistDC) {
  ExecutionProfile profile;
  explode(String("dc1, dc3, dc5"), profile.blacklist_dc());
  ASSERT_EQ(3u, profile.blacklist_dc().size());
  ASSERT_STREQ("dc1", profile.blacklist_dc().at(0).c_str());
  ASSERT_STREQ("dc3", profile.blacklist_dc().at(1).c_str());
  ASSERT_STREQ("dc5", profile.blacklist_dc().at(2).c_str());

  Config config;
  config.set_execution_profile("profile", &profile);

  Config copy_config = config.new_instance();
  ExecutionProfile profile_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile", profile_lookup));
  ASSERT_EQ(profile.blacklist_dc().size(), profile_lookup.blacklist_dc().size());
  ASSERT_STREQ(profile.blacklist_dc().at(0).c_str(), profile_lookup.blacklist_dc().at(0).c_str());
  ASSERT_STREQ(profile.blacklist_dc().at(1).c_str(), profile_lookup.blacklist_dc().at(1).c_str());
  ASSERT_STREQ(profile.blacklist_dc().at(2).c_str(), profile_lookup.blacklist_dc().at(2).c_str());
  ASSERT_EQ(0u, copy_config.default_profile().blacklist_dc().size());
}

TEST(ExecutionProfileUnitTest, Whitelist) {
  ExecutionProfile profile;
  explode(String("0.0.0.0, 0.0.0.2, 0.0.0.4"), profile.whitelist());
  ASSERT_EQ(3u, profile.whitelist().size());
  ASSERT_STREQ("0.0.0.0", profile.whitelist().at(0).c_str());
  ASSERT_STREQ("0.0.0.2", profile.whitelist().at(1).c_str());
  ASSERT_STREQ("0.0.0.4", profile.whitelist().at(2).c_str());

  Config config;
  config.set_execution_profile("profile", &profile);

  Config copy_config = config.new_instance();
  ExecutionProfile profile_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile", profile_lookup));
  ASSERT_EQ(profile.whitelist().size(), profile_lookup.whitelist().size());
  ASSERT_STREQ(profile.whitelist().at(0).c_str(), profile_lookup.whitelist().at(0).c_str());
  ASSERT_STREQ(profile.whitelist().at(1).c_str(), profile_lookup.whitelist().at(1).c_str());
  ASSERT_STREQ(profile.whitelist().at(2).c_str(), profile_lookup.whitelist().at(2).c_str());
  ASSERT_EQ(0u, copy_config.default_profile().whitelist().size());
}

TEST(ExecutionProfileUnitTest, WhitelistDC) {
  ExecutionProfile profile;
  explode(String("dc1, dc3, dc5"), profile.whitelist_dc());
  ASSERT_EQ(3u, profile.whitelist_dc().size());
  ASSERT_STREQ("dc1", profile.whitelist_dc().at(0).c_str());
  ASSERT_STREQ("dc3", profile.whitelist_dc().at(1).c_str());
  ASSERT_STREQ("dc5", profile.whitelist_dc().at(2).c_str());

  Config config;
  config.set_execution_profile("profile", &profile);

  Config copy_config = config.new_instance();
  ExecutionProfile profile_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile", profile_lookup));
  ASSERT_EQ(profile.whitelist_dc().size(), profile_lookup.whitelist_dc().size());
  ASSERT_STREQ(profile.whitelist_dc().at(0).c_str(), profile_lookup.whitelist_dc().at(0).c_str());
  ASSERT_STREQ(profile.whitelist_dc().at(1).c_str(), profile_lookup.whitelist_dc().at(1).c_str());
  ASSERT_STREQ(profile.whitelist_dc().at(2).c_str(), profile_lookup.whitelist_dc().at(2).c_str());
  ASSERT_EQ(0u, copy_config.default_profile().whitelist_dc().size());
}

TEST(ExecutionProfileUnitTest, LatencyAware) {
  ExecutionProfile profile;
  profile.set_latency_aware_routing(true);

  Config config;
  config.set_execution_profile("profile", &profile);

  Config copy_config = config.new_instance();
  ExecutionProfile profile_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile", profile_lookup));
  ASSERT_FALSE(copy_config.default_profile().latency_aware());
  ASSERT_TRUE(profile_lookup.latency_aware());
}

TEST(ExecutionProfileUnitTest, TokenAware) {
  ExecutionProfile profile;
  profile.set_token_aware_routing(false);

  Config config;
  config.set_execution_profile("profile", &profile);

  Config copy_config = config.new_instance();
  ExecutionProfile profile_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile", profile_lookup));
  ASSERT_TRUE(copy_config.default_profile().token_aware_routing());
  ASSERT_FALSE(profile_lookup.token_aware_routing());
}

TEST(ExecutionProfileUnitTest, NullRetryPolicy) {
  ExecutionProfile profile;
  ASSERT_TRUE(!profile.retry_policy());
}

TEST(ExecutionProfileUnitTest, ClusterRetryPolicy) {
  ExecutionProfile profile;

  Config config;
  config.set_execution_profile("profile", &profile);

  Config copy_config = config.new_instance();
  ExecutionProfile profile_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile", profile_lookup));
  ASSERT_EQ(copy_config.default_profile().retry_policy().get(),
            profile_lookup.retry_policy().get());
}

TEST(ExecutionProfileUnitTest, ClusterRetryPolicies) {
  ExecutionProfile profile_1;
  profile_1.set_retry_policy(new FallthroughRetryPolicy());
  ExecutionProfile profile_2;
  ExecutionProfile profile_3;
  profile_3.set_retry_policy(new DefaultRetryPolicy());

  Config config;
  config.set_execution_profile("profile_1", &profile_1);
  config.set_execution_profile("profile_2", &profile_2);
  config.set_execution_profile("profile_3", &profile_3);

  Config copy_config = config.new_instance();
  ExecutionProfile profile_1_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile_1", profile_1_lookup));
  ExecutionProfile profile_2_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile_2", profile_2_lookup));
  ExecutionProfile profile_3_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile_3", profile_3_lookup));
  const RetryPolicy* profile_1_retry_policy = profile_1_lookup.retry_policy().get();
  const RetryPolicy* profile_2_retry_policy = profile_2_lookup.retry_policy().get();
  const RetryPolicy* profile_3_retry_policy = profile_3_lookup.retry_policy().get();
  EXPECT_NE(profile_1_retry_policy, profile_2_retry_policy);
  EXPECT_NE(profile_2_retry_policy, profile_3_retry_policy);
  EXPECT_NE(profile_3_retry_policy, profile_1_retry_policy);
  ASSERT_EQ(copy_config.default_profile().retry_policy().get(), profile_2_retry_policy);
}

TEST(ExecutionProfileUnitTest, NullSpeculativeExecutionPolicy) {
  ExecutionProfile profile;
  ASSERT_TRUE(!profile.speculative_execution_policy());
}

TEST(ExecutionProfileUnitTest, ClusterSpeculativeExecutionPolicy) {
  ExecutionProfile profile;

  Config config;
  config.set_speculative_execution_policy(new NoSpeculativeExecutionPolicy());
  config.set_execution_profile("profile", &profile);

  Config copy_config = config.new_instance();
  ExecutionProfile profile_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile", profile_lookup));
  ASSERT_NE(copy_config.default_profile().speculative_execution_policy().get(),
            profile_lookup.speculative_execution_policy().get());
  ASSERT_EQ(typeid(copy_config.default_profile().speculative_execution_policy().get()),
            typeid(profile_lookup.speculative_execution_policy().get()));
}

TEST(ExecutionProfileUnitTest, ClusterSpeculativeExecutionPolicies) {
  ExecutionProfile profile_1;
  profile_1.set_speculative_execution_policy(new ConstantSpeculativeExecutionPolicy(1, 2));
  ExecutionProfile profile_2;
  ExecutionProfile profile_3;
  profile_3.set_speculative_execution_policy(new NoSpeculativeExecutionPolicy());

  Config config;
  config.set_speculative_execution_policy(new ConstantSpeculativeExecutionPolicy(3, 4));
  config.set_execution_profile("profile_1", &profile_1);
  config.set_execution_profile("profile_2", &profile_2);
  config.set_execution_profile("profile_3", &profile_3);

  Config copy_config = config.new_instance();
  ExecutionProfile profile_1_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile_1", profile_1_lookup));
  ExecutionProfile profile_2_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile_2", profile_2_lookup));
  ExecutionProfile profile_3_lookup;
  ASSERT_TRUE(execution_profile(copy_config, "profile_3", profile_3_lookup));
  const SpeculativeExecutionPolicy* profile_1_speculative_execution_policy =
      profile_1_lookup.speculative_execution_policy().get();
  const SpeculativeExecutionPolicy* profile_2_speculative_execution_policy =
      profile_2_lookup.speculative_execution_policy().get();
  const SpeculativeExecutionPolicy* profile_3_speculative_execution_policy =
      profile_3_lookup.speculative_execution_policy().get();
  const SpeculativeExecutionPolicy* config_speculative_execution_policy =
      copy_config.default_profile().speculative_execution_policy().get();
  EXPECT_NE(profile_1_speculative_execution_policy, profile_2_speculative_execution_policy);
  EXPECT_NE(profile_2_speculative_execution_policy, profile_3_speculative_execution_policy);
  EXPECT_NE(profile_3_speculative_execution_policy, profile_1_speculative_execution_policy);
  ASSERT_NE(config_speculative_execution_policy, profile_2_speculative_execution_policy);
  ASSERT_EQ(typeid(config_speculative_execution_policy),
            typeid(profile_2_speculative_execution_policy));
  ASSERT_EQ(typeid(config_speculative_execution_policy),
            typeid(profile_1_speculative_execution_policy));
  ASSERT_EQ(
      dynamic_cast<const ConstantSpeculativeExecutionPolicy*>(config_speculative_execution_policy)
          ->constant_delay_ms_,
      dynamic_cast<const ConstantSpeculativeExecutionPolicy*>(
          profile_2_speculative_execution_policy)
          ->constant_delay_ms_);
  ASSERT_EQ(
      dynamic_cast<const ConstantSpeculativeExecutionPolicy*>(config_speculative_execution_policy)
          ->max_speculative_executions_,
      dynamic_cast<const ConstantSpeculativeExecutionPolicy*>(
          profile_2_speculative_execution_policy)
          ->max_speculative_executions_);
  ASSERT_NE(
      dynamic_cast<const ConstantSpeculativeExecutionPolicy*>(config_speculative_execution_policy)
          ->constant_delay_ms_,
      dynamic_cast<const ConstantSpeculativeExecutionPolicy*>(
          profile_1_speculative_execution_policy)
          ->constant_delay_ms_);
  ASSERT_NE(
      dynamic_cast<const ConstantSpeculativeExecutionPolicy*>(config_speculative_execution_policy)
          ->max_speculative_executions_,
      dynamic_cast<const ConstantSpeculativeExecutionPolicy*>(
          profile_1_speculative_execution_policy)
          ->max_speculative_executions_);
}
