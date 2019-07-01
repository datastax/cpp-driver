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

#ifndef DATASTAX_INTERNAL_TIMESTAMP_GENERATOR_HPP
#define DATASTAX_INTERNAL_TIMESTAMP_GENERATOR_HPP

#include "atomic.hpp"
#include "constants.hpp"
#include "external.hpp"
#include "macros.hpp"
#include "ref_counted.hpp"
#include "request.hpp"

#include <stdint.h>

namespace datastax { namespace internal { namespace core {

class TimestampGenerator : public RefCounted<TimestampGenerator> {
public:
  typedef SharedRefPtr<TimestampGenerator> Ptr;

  enum Type { SERVER_SIDE, MONOTONIC };

  TimestampGenerator(Type type)
      : type_(type) {}

  virtual ~TimestampGenerator() {}

  Type type() const { return type_; }

  virtual int64_t next() = 0;

private:
  Type type_;

private:
  DISALLOW_COPY_AND_ASSIGN(TimestampGenerator);
};

class ServerSideTimestampGenerator : public TimestampGenerator {
public:
  ServerSideTimestampGenerator()
      : TimestampGenerator(SERVER_SIDE) {}

  virtual int64_t next() { return CASS_INT64_MIN; }
};

class MonotonicTimestampGenerator : public TimestampGenerator {
public:
  MonotonicTimestampGenerator(int64_t warning_threshold_us = 1000000,
                              int64_t warning_interval_ms = 1000)
      : TimestampGenerator(MONOTONIC)
      , last_(0)
      , last_warning_(0)
      , warning_threshold_us_(warning_threshold_us)
      , warning_interval_ms_(warning_interval_ms < 0 ? 0 : warning_interval_ms) {}

  virtual int64_t next();

private:
  int64_t compute_next(int64_t last);

  Atomic<int64_t> last_;
  Atomic<int64_t> last_warning_;

  const int64_t warning_threshold_us_;
  const int64_t warning_interval_ms_;
};

}}} // namespace datastax::internal::core

EXTERNAL_TYPE(datastax::internal::core::TimestampGenerator, CassTimestampGen)

#endif
