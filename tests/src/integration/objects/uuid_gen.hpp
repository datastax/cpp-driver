/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_UUID_GEN_HPP__
#define __TEST_UUID_GEN_HPP__
#include "cassandra.h"

#include "objects/object_base.hpp"

#include "values/uuid.hpp"

namespace test {
namespace driver {

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

  /**
   * Generate a v1 UUID (time based)
   *
   * @return v1 UUID for the current time
   */
  TimeUuid generate_timeuuid() {
    CassUuid uuid;
    cass_uuid_gen_time(get(), &uuid);
    return TimeUuid(uuid);
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
    return TimeUuid(uuid);
  }

  /**
   * Generate a v4 random UUID
   *
   * @return Randomly generated v4 UUID
   */
  Uuid generate_random_uuid() {
    CassUuid uuid;
    cass_uuid_gen_random(get(), &uuid);
    return Uuid(uuid);
  }
};

} // namespace driver
} // namespace test

#endif // __TEST_UUID_GEN_HPP__
