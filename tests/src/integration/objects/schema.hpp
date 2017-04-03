/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_SCHEMA_HPP__
#define __TEST_SCHEMA_HPP__

#include "cassandra.h"

#include "objects/object_base.hpp"

namespace test {
namespace driver {

// Forward declarations
class Keyspace;
class UserTypeType;

/**
 * Wrapped schema object
 */
class Schema : public Object<const CassSchemaMeta, cass_schema_meta_free> {
public:
  class Exception : public test::Exception {
  public:
    Exception(const std::string& message)
      : test::Exception(message) {}
  };

  /**
   * Create a schema object
   *
   * @param schema_meta Native driver object
   */
  Schema(const CassSchemaMeta* schema_meta)
    : Object<const CassSchemaMeta, cass_schema_meta_free>(schema_meta) {}

  /**
   * Get the keyspace metadata for a given keyspace
   *
   * @param name Keyspace name to lookup
   * @return Keyspace metadata for given keyspace name
   * @throws Exception if keyspace is not available
   */
  Keyspace keyspace(const std::string& name);
};

/**
 * Keyspace object
 */
class Keyspace {
public:
  class Exception : public test::Exception {
  public:
    Exception(const std::string& message)
      : test::Exception(message) {}
  };

  /**
   * Create the keyspace object
   *
   * @param keyspace_meta Native driver object
   * @param parent The schema object for the keyspace metadata
   */
  Keyspace(const CassKeyspaceMeta* keyspace_meta, const Schema& parent)
    : keyspace_meta_(keyspace_meta)
    , parent_(parent) {}

  /**
   * Get the UserType type object for a given user type
   *
   * @param name User type name to lookup
   * @throws Exception if user type is not available
   */
  UserTypeType user_type(const std::string& name);

private:
  /**
   * The keyspace metadata held by this keyspace object
   */
  const CassKeyspaceMeta* keyspace_meta_;
  /**
   * Parent schema object
   */
  Schema parent_;
};

/**
 * UserType type object
 */
class UserTypeType {
public:
  /**
   * Create the UserType type object
   *
   * @param data_type Native driver object
   * @param parent The keyspace object for the UserType type
   */
  UserTypeType(const CassDataType* data_type, const Keyspace& parent)
    : data_type_(data_type)
    , parent_(parent) {}

  /**
   * Get the data type
   *
   * @return Native driver data type
   */
  const CassDataType* data_type() const {
    return data_type_;
  }

private:
  /**
   * The data type held by this user type object
   */
  const CassDataType* data_type_;
  /**
   * Parent keyspace object
   */
  Keyspace parent_;
};


inline Keyspace Schema::keyspace(const std::string& name) {
  const CassKeyspaceMeta* keyspace_meta
      = cass_schema_meta_keyspace_by_name(get(), name.c_str());
  if (keyspace_meta == NULL) {
    throw Exception("Unable to get metadata for keyspace: " + name);
  }
  return Keyspace(keyspace_meta, *this);
}

inline UserTypeType Keyspace::user_type(const std::string& name) {
  const CassDataType* data_type
      = cass_keyspace_meta_user_type_by_name(keyspace_meta_, name.c_str());
  if (data_type == NULL) {
    throw Exception("Unable to get metadata for user type: " + name);
  }
  return UserTypeType(data_type, *this);
}

} // namespace driver
} // namespace test

#endif // __TEST_SCHEMA_HPP__
