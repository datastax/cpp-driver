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

#ifndef __TEST_SCHEMA_HPP__
#define __TEST_SCHEMA_HPP__

#include "cassandra.h"

#include "objects/object_base.hpp"

namespace test { namespace driver {

// Forward declarations
class Keyspace;
class UserTypeType;
class Table;

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
   * Default constructor
   */
  Schema()
      : Object<const CassSchemaMeta, cass_schema_meta_free>(NULL) {}

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

  const CassVersion version() const { return cass_schema_meta_version(get()); }
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
   * Default constructor
   */
  Keyspace()
      : keyspace_meta_(NULL) {}

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
   * Determine if the keyspace is virtual
   *
   * @return true if virtual, otherwise false
   */
  bool is_virtual() const { return cass_keyspace_meta_is_virtual(keyspace_meta_) == cass_true; }

  /**
   * Get the UserType type object for a given user type
   *
   * @param name User type name to lookup
   * @throws Exception if user type is not available
   */
  UserTypeType user_type(const std::string& name);

  /**
   * Get the Table object for a given table
   *
   * @param name Table name to lookup
   * @throws Exception if name is not available
   */
  Table table(const std::string& name);

  const CassKeyspaceMeta* get() const { return keyspace_meta_; }

  operator bool() const { return keyspace_meta_ != NULL; }

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
 * Table object
 */
class Table {
public:
  /*
   * Default constructor
   */
  Table()
      : table_meta_(NULL) {}

  /**
   * Create the table object
   *
   * @param table_meta Native driver object
   * @param parent The keyspace object that owns the table
   */
  Table(const CassTableMeta* table_meta, const Keyspace& parent)
      : table_meta_(table_meta)
      , parent_(parent) {}

  /**
   * Determine if the table is virtual
   *
   * @return true if virtual, otherwise false
   */
  bool is_virtual() const { return cass_table_meta_is_virtual(table_meta_) == cass_true; }

  const CassTableMeta* get() const { return table_meta_; }

  operator bool() const { return table_meta_ != NULL; }

private:
  /**
   * The table metadata held by this table object
   */
  const CassTableMeta* table_meta_;
  /**
   * Parent keyspace object
   */
  Keyspace parent_;
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
  const CassDataType* data_type() const { return data_type_; }

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
  const CassKeyspaceMeta* keyspace_meta = cass_schema_meta_keyspace_by_name(get(), name.c_str());
  if (keyspace_meta == NULL) {
    throw Exception("Unable to get metadata for keyspace: " + name);
  }
  return Keyspace(keyspace_meta, *this);
}

inline UserTypeType Keyspace::user_type(const std::string& name) {
  const CassDataType* data_type =
      cass_keyspace_meta_user_type_by_name(keyspace_meta_, name.c_str());
  if (data_type == NULL) {
    throw Exception("Unable to get metadata for user type: " + name);
  }
  return UserTypeType(data_type, *this);
}

inline Table Keyspace::table(const std::string& name) {
  const CassTableMeta* table = cass_keyspace_meta_table_by_name(keyspace_meta_, name.c_str());
  if (table == NULL) {
    throw Exception("Unable to get metadata for table: " + name);
  }
  return Table(table, *this);
}

}} // namespace test::driver

#endif // __TEST_SCHEMA_HPP__
