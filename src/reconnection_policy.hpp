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

#ifndef DATASTAX_INTERNAL_RECONNECTION_POLICY_HPP
#define DATASTAX_INTERNAL_RECONNECTION_POLICY_HPP

#include "constants.hpp"
#include "random.hpp"
#include "ref_counted.hpp"

namespace datastax { namespace internal { namespace core {

class ReconnectionSchedule : public RefCounted<ReconnectionSchedule> {
public:
  virtual ~ReconnectionSchedule();

  virtual uint64_t next_delay_ms() = 0;
};

class ReconnectionPolicy : public RefCounted<ReconnectionPolicy> {
public:
  typedef SharedRefPtr<ReconnectionPolicy> Ptr;
  enum Type { CONSTANT, EXPONENTIAL };

  ReconnectionPolicy(Type type);
  virtual ~ReconnectionPolicy();

  Type type() const;

  virtual const char* name() const = 0;
  virtual ReconnectionSchedule* new_reconnection_schedule() = 0;

private:
  Type type_;
};

class ConstantReconnectionSchedule : public ReconnectionSchedule {
public:
  ConstantReconnectionSchedule(uint64_t delay_ms);

  virtual uint64_t next_delay_ms();

private:
  uint64_t delay_ms_;
};

class ConstantReconnectionPolicy : public ReconnectionPolicy {
public:
  typedef SharedRefPtr<ConstantReconnectionPolicy> Ptr;

  ConstantReconnectionPolicy(uint64_t delay_ms = CASS_DEFAULT_CONSTANT_RECONNECT_WAIT_TIME_MS);

  uint64_t delay_ms() const;

  virtual const char* name() const;
  virtual ReconnectionSchedule* new_reconnection_schedule();

private:
  uint64_t delay_ms_;
};

class ExponentialReconnectionSchedule : public ReconnectionSchedule {
public:
  ExponentialReconnectionSchedule(uint64_t base_delay_ms, uint64_t max_delay_ms,
                                  unsigned max_attempts, Random* random);

  virtual uint64_t next_delay_ms();

private:
  uint64_t base_delay_ms_;
  uint64_t max_delay_ms_;
  unsigned max_attempts_;
  unsigned attempt_;
  Random* random_;
};

class ExponentialReconnectionPolicy : public ReconnectionPolicy {
public:
  typedef SharedRefPtr<ExponentialReconnectionPolicy> Ptr;

  ExponentialReconnectionPolicy(
      uint64_t base_delay_ms = CASS_DEFAULT_EXPONENTIAL_RECONNECT_BASE_DELAY_MS,
      uint64_t max_delay_ms = CASS_DEFAULT_EXPONENTIAL_RECONNECT_MAX_DELAY_MS);

  uint64_t base_delay_ms() const;
  uint64_t max_delay_ms() const;

  virtual const char* name() const;
  virtual ReconnectionSchedule* new_reconnection_schedule();

private:
  uint64_t base_delay_ms_;
  uint64_t max_delay_ms_;
  unsigned max_attempts_;
  Random random_;
};

}}} // namespace datastax::internal::core

#endif
