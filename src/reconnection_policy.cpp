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

#include "reconnection_policy.hpp"
#include "utils.hpp"

#include <algorithm>
#include <limits>

using namespace datastax::internal::core;

namespace datastax { namespace internal { namespace core {

unsigned calculate_max_attempts(uint64_t base_delay_ms) {
  assert(base_delay_ms > 0 && "Base delay cannot be zero");
  return 63u - num_leading_zeros(std::numeric_limits<uint64_t>::max() / base_delay_ms);
}

ReconnectionSchedule::~ReconnectionSchedule() {}

ReconnectionPolicy::ReconnectionPolicy(Type type)
    : type_(type) {}

ReconnectionPolicy::~ReconnectionPolicy() {}

ReconnectionPolicy::Type ReconnectionPolicy::type() const { return type_; }

ConstantReconnectionPolicy::ConstantReconnectionPolicy(uint64_t delay_ms)
    : ReconnectionPolicy(CONSTANT)
    , delay_ms_(delay_ms) {}

uint64_t ConstantReconnectionPolicy::delay_ms() const { return delay_ms_; }

const char* ConstantReconnectionPolicy::name() const { return "constant"; }

ReconnectionSchedule* ConstantReconnectionPolicy::new_reconnection_schedule() {
  return new ConstantReconnectionSchedule(delay_ms_);
}

ConstantReconnectionSchedule::ConstantReconnectionSchedule(uint64_t delay_ms)
    : delay_ms_(delay_ms) {}

uint64_t ConstantReconnectionSchedule::next_delay_ms() { return delay_ms_; }

ExponentialReconnectionPolicy::ExponentialReconnectionPolicy(uint64_t base_delay_ms,
                                                             uint64_t max_delay_ms)
    : ReconnectionPolicy(EXPONENTIAL)
    , base_delay_ms_(base_delay_ms)
    , max_delay_ms_(max_delay_ms)
    , max_attempts_(calculate_max_attempts(base_delay_ms)) {}

uint64_t ExponentialReconnectionPolicy::base_delay_ms() const { return base_delay_ms_; }

uint64_t ExponentialReconnectionPolicy::max_delay_ms() const { return max_delay_ms_; }

const char* ExponentialReconnectionPolicy::name() const { return "exponential"; }

ReconnectionSchedule* ExponentialReconnectionPolicy::new_reconnection_schedule() {
  return new ExponentialReconnectionSchedule(base_delay_ms_, max_delay_ms_, max_attempts_,
                                             &random_);
}

ExponentialReconnectionSchedule::ExponentialReconnectionSchedule(uint64_t base_delay_ms,
                                                                 uint64_t max_delay_ms,
                                                                 unsigned max_attempts,
                                                                 Random* random)
    : base_delay_ms_(base_delay_ms)
    , max_delay_ms_(max_delay_ms)
    , max_attempts_(max_attempts)
    , attempt_(0)
    , random_(random) {}

uint64_t ExponentialReconnectionSchedule::next_delay_ms() {
  uint64_t delay_ms = max_delay_ms_;
  if (attempt_ <= max_attempts_) {
    // Calculate the delay and add +/- 15% jitter
    delay_ms = std::min(base_delay_ms_ * (static_cast<uint64_t>(1) << attempt_++), max_delay_ms_);
    uint64_t jitter = random_->next(30) + 85; // 85 - 115 range
    delay_ms = (jitter * delay_ms) / 100;

    // Ensure the delay is not less than the base and not greater than the max delay
    delay_ms = std::min(std::max(base_delay_ms_, delay_ms), max_delay_ms_);
    assert(delay_ms > 0);
  }
  return delay_ms;
}

}}} // namespace datastax::internal::core
