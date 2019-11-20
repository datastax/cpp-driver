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

#include "constants.hpp"
#include "reconnection_policy.hpp"
#include "scoped_ptr.hpp"

using datastax::internal::ScopedPtr;
using datastax::internal::core::ConstantReconnectionPolicy;
using datastax::internal::core::ConstantReconnectionSchedule;
using datastax::internal::core::ExponentialReconnectionPolicy;
using datastax::internal::core::ExponentialReconnectionSchedule;
using datastax::internal::core::ReconnectionPolicy;
using datastax::internal::core::ReconnectionSchedule;

#define FIFTEEN_PERCENT(value) static_cast<double>(value - ((value * 85) / 100))

TEST(ReconnectionPolicyUnitTest, Constant) {
  { // Ensure default value is assigned
    ConstantReconnectionPolicy policy;
    EXPECT_EQ(ReconnectionPolicy::CONSTANT, policy.type());
    EXPECT_EQ(CASS_DEFAULT_CONSTANT_RECONNECT_WAIT_TIME_MS, policy.delay_ms());
    EXPECT_STREQ("constant", policy.name());
  }

  { // Ensure assigned value
    ConstantReconnectionPolicy policy(65535u);
    EXPECT_EQ(ReconnectionPolicy::CONSTANT, policy.type());
    EXPECT_EQ(65535u, policy.delay_ms());
    EXPECT_STREQ("constant", policy.name());
  }
}

TEST(ReconnectionPolicyUnitTest, ConstantSchedule) {
  { // Ensure default value is scheduled
    ConstantReconnectionPolicy policy;
    ScopedPtr<ReconnectionSchedule> schedule(policy.new_reconnection_schedule());
    EXPECT_EQ(CASS_DEFAULT_CONSTANT_RECONNECT_WAIT_TIME_MS, schedule->next_delay_ms());

    for (int i = 0; i < 1000; ++i) {
      EXPECT_EQ(CASS_DEFAULT_CONSTANT_RECONNECT_WAIT_TIME_MS, schedule->next_delay_ms());
    }
  }

  { // Ensure assigned value is scheduled
    ConstantReconnectionPolicy policy(65535u);
    ScopedPtr<ReconnectionSchedule> schedule(policy.new_reconnection_schedule());
    EXPECT_EQ(65535u, schedule->next_delay_ms());

    for (int i = 0; i < 1000; ++i) {
      EXPECT_EQ(65535u, schedule->next_delay_ms());
    }
  }
}

TEST(ReconnectionPolicyUnitTest, Exponential) {
  { // Ensure default value is assigned
    ExponentialReconnectionPolicy policy;
    EXPECT_EQ(CASS_DEFAULT_EXPONENTIAL_RECONNECT_BASE_DELAY_MS, policy.base_delay_ms());
    EXPECT_EQ(CASS_DEFAULT_EXPONENTIAL_RECONNECT_MAX_DELAY_MS, policy.max_delay_ms());
    EXPECT_STREQ("exponential", policy.name());
  }

  { // Ensure assigned value
    ExponentialReconnectionPolicy policy(1u, 2u);
    EXPECT_EQ(1u, policy.base_delay_ms());
    EXPECT_EQ(2u, policy.max_delay_ms());
    EXPECT_STREQ("exponential", policy.name());
  }
}

TEST(ReconnectionPolicyUnitTest, ExponentialSchedule) {
  ExponentialReconnectionPolicy policy(2u, 16u);
  ScopedPtr<ReconnectionSchedule> schedule(policy.new_reconnection_schedule());
  EXPECT_NEAR(2.0, static_cast<double>(schedule->next_delay_ms()), FIFTEEN_PERCENT(2));
  EXPECT_NEAR(4.0, static_cast<double>(schedule->next_delay_ms()), FIFTEEN_PERCENT(4));
  EXPECT_NEAR(8.0, static_cast<double>(schedule->next_delay_ms()), FIFTEEN_PERCENT(8));
  EXPECT_NEAR(16.0, static_cast<double>(schedule->next_delay_ms()), FIFTEEN_PERCENT(16));
  EXPECT_NEAR(16.0, static_cast<double>(schedule->next_delay_ms()), FIFTEEN_PERCENT(16));
}

TEST(ReconnectionPolicyUnitTest, ExponentialScheduleOverflow) {
  for (uint64_t base_delay_ms = 2; base_delay_ms < 4294967295u; base_delay_ms += base_delay_ms) {
    uint64_t max_delay_ms = base_delay_ms * base_delay_ms;
    ExponentialReconnectionPolicy policy(base_delay_ms, max_delay_ms);
    ScopedPtr<ReconnectionSchedule> schedule(policy.new_reconnection_schedule());
    uint64_t delay_ms = schedule->next_delay_ms();
    while (delay_ms != max_delay_ms) {
      delay_ms = schedule->next_delay_ms();
    }

    for (int j = 0; j < 63; ++j) {
      EXPECT_NEAR(static_cast<double>(max_delay_ms), static_cast<double>(schedule->next_delay_ms()),
                  FIFTEEN_PERCENT(max_delay_ms));
    }
  }
}

TEST(ReconnectionPolicyUnitTest, ExponentialScheduleIndependent) {
  ExponentialReconnectionPolicy policy(2u, 16u);

  ScopedPtr<ReconnectionSchedule> schedule_1(policy.new_reconnection_schedule());
  EXPECT_NEAR(2.0, static_cast<double>(schedule_1->next_delay_ms()), FIFTEEN_PERCENT(2));
  EXPECT_NEAR(4.0, static_cast<double>(schedule_1->next_delay_ms()), FIFTEEN_PERCENT(4));

  ScopedPtr<ReconnectionSchedule> schedule_2(policy.new_reconnection_schedule());
  EXPECT_NEAR(2.0, static_cast<double>(schedule_2->next_delay_ms()), FIFTEEN_PERCENT(2));
  EXPECT_NEAR(4.0, static_cast<double>(schedule_2->next_delay_ms()), FIFTEEN_PERCENT(4));
}
