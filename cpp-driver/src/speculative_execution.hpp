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

#ifndef DATASTAX_INTERNAL_SPECULATIVE_EXECUTION_HPP
#define DATASTAX_INTERNAL_SPECULATIVE_EXECUTION_HPP

#include "allocated.hpp"
#include "host.hpp"
#include "ref_counted.hpp"
#include "string.hpp"

#include <stdint.h>

namespace datastax { namespace internal { namespace core {

class Request;

class SpeculativeExecutionPlan : public Allocated {
public:
  virtual ~SpeculativeExecutionPlan() {}

  virtual int64_t next_execution(const Host::Ptr& current_host) = 0;
};

class SpeculativeExecutionPolicy : public RefCounted<SpeculativeExecutionPolicy> {
public:
  typedef SharedRefPtr<SpeculativeExecutionPolicy> Ptr;

  virtual ~SpeculativeExecutionPolicy() {}

  virtual SpeculativeExecutionPlan* new_plan(const String& keyspace, const Request* request) = 0;

  virtual SpeculativeExecutionPolicy* new_instance() = 0;
};

class NoSpeculativeExecutionPlan : public SpeculativeExecutionPlan {
public:
  virtual int64_t next_execution(const Host::Ptr& current_host) { return -1; }
};

class NoSpeculativeExecutionPolicy : public SpeculativeExecutionPolicy {
public:
  virtual SpeculativeExecutionPlan* new_plan(const String& keyspace, const Request* request) {
    return new NoSpeculativeExecutionPlan();
  }

  virtual SpeculativeExecutionPolicy* new_instance() { return new NoSpeculativeExecutionPolicy(); }
};

class ConstantSpeculativeExecutionPlan : public SpeculativeExecutionPlan {
public:
  ConstantSpeculativeExecutionPlan(int64_t constant_delay_ms, int count)
      : constant_delay_ms_(constant_delay_ms)
      , count_(count) {}

  virtual int64_t next_execution(const Host::Ptr& current_host) {
    return --count_ >= 0 ? constant_delay_ms_ : -1;
  }

private:
  const int64_t constant_delay_ms_;
  int count_;
};

class ConstantSpeculativeExecutionPolicy : public SpeculativeExecutionPolicy {
public:
  ConstantSpeculativeExecutionPolicy(int64_t constant_delay_ms, int max_speculative_executions)
      : constant_delay_ms_(constant_delay_ms)
      , max_speculative_executions_(max_speculative_executions) {}

  virtual SpeculativeExecutionPlan* new_plan(const String& keyspace, const Request* request) {
    return new ConstantSpeculativeExecutionPlan(constant_delay_ms_, max_speculative_executions_);
  }

  virtual SpeculativeExecutionPolicy* new_instance() {
    return new ConstantSpeculativeExecutionPolicy(constant_delay_ms_, max_speculative_executions_);
  }

  const int64_t constant_delay_ms_;
  const int max_speculative_executions_;
};

}}} // namespace datastax::internal::core

#endif
