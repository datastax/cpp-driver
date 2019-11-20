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

#ifndef TEST_TIMESTAMP_GENERATOR_HPP
#define TEST_TIMESTAMP_GENERATOR_HPP
#include "cassandra.h"

#include "objects/object_base.hpp"

#include <gtest/gtest.h>

namespace test { namespace driver {

/**
 * Wrapped timestamp generator object
 */
class TimestampGenerator : public Object<CassTimestampGen, cass_timestamp_gen_free> {
public:
  /**
   * Create the timestamp generator object from the native driver object
   *
   * @param timestamp_generator Native driver object
   */
  TimestampGenerator(CassTimestampGen* timestamp_generator)
      : Object<CassTimestampGen, cass_timestamp_gen_free>(timestamp_generator) {}

  /**
   * Create the timestamp generator object from a shared reference
   *
   * @param timestamp_generator Shared reference
   */
  TimestampGenerator(Ptr timestamp_generator)
      : Object<CassTimestampGen, cass_timestamp_gen_free>(timestamp_generator) {}
};

/**
 * Wrapped server side timestamp generator object
 */
class ServerSideTimestampGenerator : public TimestampGenerator {
public:
  /**
   * Create the default server side timestamp generator object
   */
  ServerSideTimestampGenerator()
      : TimestampGenerator(cass_timestamp_gen_server_side_new()) {}
};

/**
 * Wrapped monotonic timestamp generator object
 */
class MonotonicTimestampGenerator : public TimestampGenerator {
public:
  /**
   * Create the default monotonic timestamp generator object
   */
  MonotonicTimestampGenerator()
      : TimestampGenerator(cass_timestamp_gen_monotonic_new()) {}

  /**
   * Create the monotonic timestamp generator object with settings
   *
   * @param warning_threshold_us Amount of clock skew to illicit warning (0 to disable)
   * @param warning_interval_ms Internal amount of time before next clock skew warning (<= 1
   * triggers every millisecond)
   */
  MonotonicTimestampGenerator(int64_t warning_threshold_us, int64_t warning_interval_ms)
      : TimestampGenerator(cass_timestamp_gen_monotonic_new_with_settings(warning_threshold_us,
                                                                          warning_interval_ms)) {}
};

}} // namespace test::driver

#endif