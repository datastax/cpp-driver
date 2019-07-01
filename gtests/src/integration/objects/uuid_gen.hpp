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

#ifndef __TEST_UUID_GEN_HPP__
#define __TEST_UUID_GEN_HPP__
#include "cassandra.h"

#include "objects/object_base.hpp"

#include "values.hpp"

namespace test { namespace driver {

/**
 * Wrapped UUID generator object
 */
class UuidGen : public Object<CassUuidGen, cass_uuid_gen_free> {
public:
  /**
   * Create the UUID generator object
   */
  UuidGen()
      : Object<CassUuidGen, cass_uuid_gen_free>(cass_uuid_gen_new()) {}

  /**
   * Create the UUID generator object with custom node information
   *
   * @param node Node to use for custom node information
   */
  UuidGen(cass_uint64_t node)
      : Object<CassUuidGen, cass_uuid_gen_free>(cass_uuid_gen_new_with_node(node)) {}

  /**
   * Create the UUID generator object from the native driver object
   *
   * @param uuid_gen Native driver object
   */
  UuidGen(CassUuidGen* uuid_gen)
      : Object<CassUuidGen, cass_uuid_gen_free>(uuid_gen) {}

  /**
   * Create the UUID generator object from a shared reference
   *
   * @param uuid_gen Shared reference
   */
  UuidGen(Ptr uuid_gen)
      : Object<CassUuidGen, cass_uuid_gen_free>(uuid_gen) {}

  virtual ~UuidGen() {}

  /**
   * Generate a v1 UUID (time based)
   *
   * @return v1 UUID for the current time
   */
  TimeUuid generate_timeuuid() {
    CassUuid uuid;
    cass_uuid_gen_time(get(), &uuid);
    return TimeUuid(values::TimeUuid(uuid));
  }

  /**
   * Generate a v1 UUID (time based) from the given timestamp
   *
   * @param timestamp Timestamp to generate v1 UUID from
   * @return v1 UUID for the given timestamp
   */
  TimeUuid generate_timeuuid(cass_uint64_t timestamp) {
    CassUuid uuid;
    cass_uuid_gen_from_time(get(), timestamp, &uuid);
    return TimeUuid(values::TimeUuid(uuid));
  }

  /**
   * Generate a v4 random UUID
   *
   * @return Randomly generated v4 UUID
   */
  Uuid generate_random_uuid() {
    CassUuid uuid;
    cass_uuid_gen_random(get(), &uuid);
    return Uuid(values::Uuid(uuid));
  }
};

}} // namespace test::driver

#endif // __TEST_UUID_GEN_HPP__
