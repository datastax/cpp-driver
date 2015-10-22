/*
  Copyright (c) 2014-2015 DataStax

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

#include "cassandra.h"
#include "testing.hpp"
#include "test_utils.hpp"
#include "metadata.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/format.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>


#include <algorithm>
#include <set>
#include <stdlib.h>
#include <string>

#define SIMPLE_STRATEGY_KEYSPACE_NAME "simple"
#define NETWORK_TOPOLOGY_KEYSPACE_NAME "network"
#define SIMPLE_STRATEGY_CLASS_NAME  "org.apache.cassandra.locator.SimpleStrategy"
#define NETWORK_TOPOLOGY_STRATEGY_CLASS_NAME "org.apache.cassandra.locator.NetworkTopologyStrategy"
#define LOCAL_STRATEGY_CLASS_NAME "org.apache.cassandra.locator.LocalStrategy"
#define COMMENT "A TESTABLE COMMENT HERE"
#define ALL_DATA_TYPES_TABLE_NAME "all"
#define USER_DATA_TYPE_NAME "user_data_type"
#define USER_DEFINED_FUNCTION_NAME "user_defined_function"

/**
 * Schema Metadata Test Class
 *
 * The purpose of this class is to setup a single session integration test
 * while initializing a single node cluster through CCM in order to perform
 * schema metadata validation against.
 */
struct TestSchemaMetadata : public test_utils::SingleSessionTest {
  const CassSchemaMeta* schema_meta_;

  TestSchemaMetadata()
    : SingleSessionTest(1, 0)
    , schema_meta_(NULL) {}

	~TestSchemaMetadata() {
    if (schema_meta_) {
      cass_schema_meta_free(schema_meta_);
		}
	}

  void verify_keyspace_created(const std::string& ks) {
    test_utils::CassResultPtr result;

    for (int i = 0; i < 10; ++i) {
      test_utils::execute_query(session,
                                str(boost::format(
                                      "SELECT * FROM system.schema_keyspaces WHERE keyspace_name = '%s'") % ks), &result);
      if (cass_result_row_count(result.get()) > 0) {
        return;
      }
      boost::this_thread::sleep_for(boost::chrono::milliseconds(10));
    }
    BOOST_REQUIRE(false);
  }

  void refresh_schema_meta() {
    if (schema_meta_) {
      const CassSchemaMeta* old = schema_meta_;
      schema_meta_ = cass_session_get_schema_meta(session);

      for (size_t i = 0;
           i < 10 && cass_schema_meta_snapshot_version(schema_meta_) == cass_schema_meta_snapshot_version(old);
           ++i) {
        boost::this_thread::sleep_for(boost::chrono::milliseconds(10));
        cass_schema_meta_free(schema_meta_);
        schema_meta_ = cass_session_get_schema_meta(session);
      }
      if (cass_schema_meta_snapshot_version(schema_meta_) == cass_schema_meta_snapshot_version(old)) {
        std::cout << "Schema metadata was not refreshed or was not changed" << std::endl;
      }
      cass_schema_meta_free(old);
    } else {
      schema_meta_ = cass_session_get_schema_meta(session);
    }
	}

  void create_simple_strategy_keyspace(unsigned int replication_factor = 1, bool durable_writes = true) {
		test_utils::execute_query_with_error(session, str(boost::format(test_utils::DROP_KEYSPACE_FORMAT) % SIMPLE_STRATEGY_KEYSPACE_NAME));
    test_utils::execute_query(session, str(boost::format("CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %s } AND durable_writes = %s")
                                           % SIMPLE_STRATEGY_KEYSPACE_NAME % replication_factor % (durable_writes ? "true" : "false")));
    refresh_schema_meta();
	}

  void create_network_topology_strategy_keyspace(unsigned int replicationFactorDataCenterOne = 3, unsigned int replicationFactorDataCenterTwo = 2, bool isDurableWrites = true) {
		test_utils::execute_query_with_error(session, str(boost::format(test_utils::DROP_KEYSPACE_FORMAT) % NETWORK_TOPOLOGY_KEYSPACE_NAME));
    test_utils::execute_query(session, str(boost::format("CREATE KEYSPACE %s WITH replication = { 'class' : 'NetworkTopologyStrategy',  'dc1' : %d, 'dc2' : %d } AND durable_writes = %s")
                                           % NETWORK_TOPOLOGY_KEYSPACE_NAME % replicationFactorDataCenterOne % replicationFactorDataCenterTwo
                                           % (isDurableWrites ? "true" : "false")));

    refresh_schema_meta();
	}

  const CassKeyspaceMeta* schema_get_keyspace(const std::string& ks_name) {
    const CassKeyspaceMeta* ks_meta = cass_schema_meta_keyspace_by_name(schema_meta_, ks_name.c_str());
    BOOST_REQUIRE(ks_meta);
    return ks_meta;
  }

  const CassTableMeta* schema_get_table(const std::string& ks_name,
                                     const std::string& table_name) {
    const CassTableMeta* table_meta = cass_keyspace_meta_table_by_name(schema_get_keyspace(ks_name), table_name.c_str());
    BOOST_REQUIRE(table_meta);
    return table_meta;
  }

  const CassColumnMeta* schema_get_column(const std::string& ks_name,
                                      const std::string& table_name,
                                      const std::string& col_name) {
    const CassColumnMeta* col_meta = cass_table_meta_column_by_name(schema_get_table(ks_name, table_name), col_name.c_str());
    BOOST_REQUIRE(col_meta);
    return col_meta;
  }

  const CassFunctionMeta* schema_get_function(const std::string& ks_name,
                              const std::string& func_name,
                              const std::vector<std::string>& func_types) {

    const CassFunctionMeta* func_meta = cass_keyspace_meta_function_by_name(schema_get_keyspace(ks_name),
      func_name.c_str(),
      test_utils::implode(func_types, ',').c_str());
    BOOST_REQUIRE(func_meta);
    return func_meta;
  }

  void verify_fields(const test_utils::CassIteratorPtr& itr, const std::set<std::string>& expected_fields) {
    // all fields present, nothing extra
    std::set<std::string> observed;
    while (cass_iterator_next(itr.get())) {
      CassString name;
      cass_iterator_get_meta_field_name(itr.get(), &name.data, &name.length);
      observed.insert(std::string(name.data, name.length));
    }

    BOOST_REQUIRE_EQUAL_COLLECTIONS(observed.begin(), observed.end(),
                                    expected_fields.begin(), expected_fields.end());
  }


  void verify_value(const CassValue* value, const std::string& expected) {
    BOOST_REQUIRE(value);
    CassValueType type = cass_value_type(value);
    BOOST_REQUIRE(type == CASS_VALUE_TYPE_ASCII || type == CASS_VALUE_TYPE_TEXT || type == CASS_VALUE_TYPE_VARCHAR);
    CassString v;
    cass_value_get_string(value, &v.data, &v.length);
    BOOST_CHECK_EQUAL(v.length, expected.size());
    BOOST_CHECK_EQUAL(expected.compare(0, std::string::npos, v.data, v.length), 0);
  }

  void verify_value(const CassValue* value, cass_bool_t expected) {
    BOOST_REQUIRE(value);
    BOOST_REQUIRE_EQUAL(cass_value_type(value), CASS_VALUE_TYPE_BOOLEAN);
    cass_bool_t v;
    cass_value_get_bool(value, &v);
    BOOST_CHECK_EQUAL(v, expected);
  }

  template<typename value_type>
  void verify_value(const CassValue* value, std::map<std::string, value_type> expected) {
    BOOST_REQUIRE(value);
    BOOST_REQUIRE_EQUAL(cass_value_type(value), CASS_VALUE_TYPE_MAP);
    BOOST_CHECK_EQUAL(cass_value_item_count(value), expected.size());
    test_utils::CassIteratorPtr itr(cass_iterator_from_map(value));
    while (cass_iterator_next(itr.get())) {
      CassString key;
      cass_value_get_string(cass_iterator_get_map_key(itr.get()), &key.data, &key.length);
      typename std::map<std::string, value_type>::const_iterator i = expected.find(std::string(key.data, key.length));
      BOOST_REQUIRE(i != expected.end());
      verify_value(cass_iterator_get_map_value(itr.get()), i->second);
    }
  }

  const std::set<std::string>& column_fields() {
    static std::set<std::string> fields;
    if (fields.empty()) {
      fields.insert("keyspace_name");
      fields.insert("columnfamily_name");
      fields.insert("column_name");
      fields.insert("component_index");
      fields.insert("index_name");
      fields.insert("index_options");
      fields.insert("index_type");
      fields.insert("validator");

      if (version.major == 2) {
        fields.insert("type");
      }
    }
    return fields;
  }

  void verify_fields_by_name(const CassKeyspaceMeta* keyspace_meta, const std::set<std::string>& fields) {
    for (std::set<std::string>::iterator i = fields.begin(); i != fields.end(); ++i) {
      BOOST_CHECK(cass_keyspace_meta_field_by_name(keyspace_meta, i->c_str()) != NULL);
    }
  }

  void verify_fields_by_name(const CassTableMeta* table_meta, const std::set<std::string>& fields) {
    for (std::set<std::string>::iterator i = fields.begin(); i != fields.end(); ++i) {
      BOOST_CHECK(cass_table_meta_field_by_name(table_meta, i->c_str()) != NULL);
    }
  }

  void verify_fields_by_name(const CassColumnMeta* column_meta, const std::set<std::string>& fields) {
    for (std::set<std::string>::iterator i = fields.begin(); i != fields.end(); ++i) {
      BOOST_CHECK(cass_column_meta_field_by_name(column_meta, i->c_str()) != NULL);
    }
  }

  void verify_columns(const CassTableMeta* table_meta) {
    test_utils::CassIteratorPtr itr(cass_iterator_columns_from_table_meta(table_meta));
    while (cass_iterator_next(itr.get())) {
      const CassColumnMeta* col_meta = cass_iterator_get_column_meta(itr.get());
      verify_fields(test_utils::CassIteratorPtr(cass_iterator_fields_from_column_meta(col_meta)), column_fields());
      verify_fields_by_name(col_meta, column_fields());
      // no entries at this level
      BOOST_CHECK(!cass_column_meta_field_by_name(col_meta, "some bogus entry"));
    }
  }

  const std::set<std::string>& table_fields() {
    static std::set<std::string> fields;
    if (fields.empty()) {
      fields.insert("keyspace_name");
      fields.insert("columnfamily_name");
      fields.insert("bloom_filter_fp_chance");
      fields.insert("caching");
      fields.insert("column_aliases");
      fields.insert("comment");
      fields.insert("compaction_strategy_class");
      fields.insert("compaction_strategy_options");
      fields.insert("comparator");
      fields.insert("compression_parameters");
      fields.insert("default_validator");
      fields.insert("gc_grace_seconds");
      fields.insert("id");
      fields.insert("key_alias");
      fields.insert("key_aliases");
      fields.insert("key_validator");
      fields.insert("local_read_repair_chance");
      fields.insert("max_compaction_threshold");
      fields.insert("min_compaction_threshold");
      fields.insert("populate_io_cache_on_flush");
      fields.insert("read_repair_chance");
      fields.insert("replicate_on_write");
      fields.insert("subcomparator");
      fields.insert("type");
      fields.insert("value_alias");

      if (version.major == 2) {
        fields.insert("default_time_to_live");
        fields.insert("dropped_columns");
        fields.erase("id");
        fields.insert("index_interval");
        fields.insert("is_dense");
        fields.erase("key_alias");
        fields.insert("memtable_flush_period_in_ms");
        fields.insert("speculative_retry");

        if (version.minor >= 1) {
          fields.insert("cf_id");
          fields.insert("max_index_interval");
          fields.insert("min_index_interval");
          fields.erase("populate_io_cache_on_flush");
          fields.erase("replicate_on_write");
        }

        if (version.minor >= 2) {
          fields.erase("column_aliases");
          fields.erase("key_aliases");
          fields.erase("value_alias");
          fields.erase("index_interval");
        }
      }
    }
    return fields;
  }

  void verify_table(const std::string& ks_name, const std::string& table_name,
                    const std::string& comment, const std::string& non_key_column) {
    const CassTableMeta* table_meta = schema_get_table(SIMPLE_STRATEGY_KEYSPACE_NAME, ALL_DATA_TYPES_TABLE_NAME);

    verify_fields(test_utils::CassIteratorPtr(cass_iterator_fields_from_table_meta(table_meta)), table_fields());
    verify_fields_by_name(table_meta, table_fields());
    verify_value(cass_table_meta_field_by_name(table_meta, "keyspace_name"), SIMPLE_STRATEGY_KEYSPACE_NAME);
    verify_value(cass_table_meta_field_by_name(table_meta, "columnfamily_name"), ALL_DATA_TYPES_TABLE_NAME);

    // not going for every field, just making sure one of each type (fixed, list, map) is correctly added
    verify_value(cass_table_meta_field_by_name(table_meta, "comment"), COMMENT);

    const CassValue* value = cass_table_meta_field_by_name(table_meta, "compression_parameters");
    BOOST_REQUIRE_EQUAL(cass_value_type(value), CASS_VALUE_TYPE_MAP);
    BOOST_REQUIRE_GE(cass_value_item_count(value), 1ul);
    test_utils::CassIteratorPtr itr(cass_iterator_from_map(value));
    const std::string parameter = "sstable_compression";
    bool param_found = false;
    while (cass_iterator_next(itr.get())) {
      value = cass_iterator_get_map_key(itr.get());
      CassString name;
      cass_value_get_string(value, &name.data, &name.length);
      if (name.length == parameter.size() &&
          parameter.compare(0, std::string::npos, name.data, name.length) == 0) {
        param_found = true;
        break;
      }
    }
    BOOST_CHECK(param_found);

    if ((version.major >= 2 && version.minor >= 2) || version.major >= 3) {
      value = cass_table_meta_field_by_name(table_meta, "cf_id");
      BOOST_REQUIRE_EQUAL(cass_value_type(value), CASS_VALUE_TYPE_UUID);
    } else {
      value = cass_table_meta_field_by_name(table_meta, "key_aliases");
      BOOST_REQUIRE_EQUAL(cass_value_type(value), CASS_VALUE_TYPE_LIST);
      BOOST_CHECK_GE(cass_value_item_count(value), 1ul);
    }

    BOOST_CHECK(!cass_table_meta_column_by_name(table_meta, "some bogus entry"));

    verify_columns(table_meta);

    // known column
    BOOST_REQUIRE(cass_table_meta_column_by_name(table_meta, non_key_column.c_str()));

    // goes away
    if (version.major >= 2) {// dropping a column not supported in 1.2
      test_utils::execute_query(session, "ALTER TABLE "+table_name+" DROP "+non_key_column);
      refresh_schema_meta();
      table_meta = schema_get_table(SIMPLE_STRATEGY_KEYSPACE_NAME, ALL_DATA_TYPES_TABLE_NAME);
      BOOST_CHECK(!cass_table_meta_column_by_name(table_meta, non_key_column.c_str()));
    }

    // new column
    test_utils::execute_query(session, "ALTER TABLE "+table_name+" ADD jkldsfafdjsklafajklsljkfds text");
    refresh_schema_meta();
    table_meta = schema_get_table(SIMPLE_STRATEGY_KEYSPACE_NAME, ALL_DATA_TYPES_TABLE_NAME);
    BOOST_CHECK(cass_table_meta_column_by_name(table_meta, "jkldsfafdjsklafajklsljkfds"));

    // drop table
    test_utils::execute_query(session, "DROP TABLE "+table_name);
    refresh_schema_meta();
    const CassKeyspaceMeta* ks_meta = cass_schema_meta_keyspace_by_name(schema_meta_, SIMPLE_STRATEGY_KEYSPACE_NAME);
    table_meta = cass_keyspace_meta_table_by_name(ks_meta, ALL_DATA_TYPES_TABLE_NAME);
    BOOST_CHECK(!table_meta);
  }

  const std::set<std::string>& keyspace_fields() {
    static std::set<std::string> fields;
    if (fields.empty()) {
      fields.insert("keyspace_name");
      fields.insert("durable_writes");
      fields.insert("strategy_class");
      fields.insert("strategy_options");
    }
    return fields;
  }

  void verify_keyspace(const std::string& name,
                       bool durable_writes,
                       const std::string& strategy_class,
                       const std::map<std::string, std::string>& strategy_options) {
    const CassKeyspaceMeta* ks_meta = schema_get_keyspace(name);
    verify_fields(test_utils::CassIteratorPtr(cass_iterator_fields_from_keyspace_meta(ks_meta)), keyspace_fields());
    verify_fields_by_name(ks_meta, keyspace_fields());
    verify_value(cass_keyspace_meta_field_by_name(ks_meta, "keyspace_name"), name);
    verify_value(cass_keyspace_meta_field_by_name(ks_meta, "durable_writes"), (cass_bool_t)durable_writes);
    verify_value(cass_keyspace_meta_field_by_name(ks_meta, "strategy_class"), strategy_class);
    verify_value(cass_keyspace_meta_field_by_name(ks_meta, "strategy_options"), strategy_options);
    BOOST_CHECK(!cass_keyspace_meta_table_by_name(ks_meta, "some bogus entry"));
  }


  void verify_system_tables() {
    // make sure system tables present, and nothing extra
    refresh_schema_meta();
    std::map<std::string, std::string> strategy_options;

    verify_keyspace("system", true, LOCAL_STRATEGY_CLASS_NAME, strategy_options);

    strategy_options.insert(std::make_pair("replication_factor", "2"));
    verify_keyspace("system_traces", true, SIMPLE_STRATEGY_CLASS_NAME, strategy_options);

    test_utils::CassIteratorPtr itr(cass_iterator_keyspaces_from_schema_meta(schema_meta_));
    size_t keyspace_count = 0;
    while (cass_iterator_next(itr.get())) ++keyspace_count;
    size_t number_of_default_keyspaces = 2;
    if (version.major == 2 && version.minor >= 2) {
      number_of_default_keyspaces = 4;
    }
    BOOST_CHECK_EQUAL(keyspace_count, number_of_default_keyspaces);
  }

  void verify_user_keyspace() {
    // new keyspace
    create_simple_strategy_keyspace(1, true);

    std::map<std::string, std::string> strategy_options;
    strategy_options.insert(std::make_pair("replication_factor", "1"));
    verify_keyspace(SIMPLE_STRATEGY_KEYSPACE_NAME, true, SIMPLE_STRATEGY_CLASS_NAME, strategy_options);

    // alter keyspace
    test_utils::execute_query(session,
                              "ALTER KEYSPACE "
                              SIMPLE_STRATEGY_KEYSPACE_NAME
                              " WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '2' } AND durable_writes = false");
    refresh_schema_meta();

    strategy_options["replication_factor"] = "2";
    verify_keyspace(SIMPLE_STRATEGY_KEYSPACE_NAME, false, SIMPLE_STRATEGY_CLASS_NAME, strategy_options);

    // keyspace goes away
    test_utils::execute_query(session, "DROP KEYSPACE " SIMPLE_STRATEGY_KEYSPACE_NAME);
    refresh_schema_meta();
    BOOST_CHECK(!cass_schema_meta_keyspace_by_name(schema_meta_, SIMPLE_STRATEGY_KEYSPACE_NAME));

    // nts
    create_network_topology_strategy_keyspace(3, 2, true);
    strategy_options.clear();
    strategy_options.insert(std::make_pair("dc1", "3"));
    strategy_options.insert(std::make_pair("dc2", "2"));
    verify_keyspace(NETWORK_TOPOLOGY_KEYSPACE_NAME, true, NETWORK_TOPOLOGY_STRATEGY_CLASS_NAME, strategy_options);
    test_utils::execute_query(session, "DROP KEYSPACE " NETWORK_TOPOLOGY_KEYSPACE_NAME);
  }

  void verify_user_table() {
    create_simple_strategy_keyspace();

    test_utils::execute_query(session, "USE " SIMPLE_STRATEGY_KEYSPACE_NAME);
    test_utils::execute_query(session, str(boost::format(test_utils::CREATE_TABLE_ALL_TYPES) % ALL_DATA_TYPES_TABLE_NAME));
    test_utils::execute_query(session, "ALTER TABLE " ALL_DATA_TYPES_TABLE_NAME " WITH comment='" COMMENT "'");
    refresh_schema_meta();

    verify_table(SIMPLE_STRATEGY_KEYSPACE_NAME, ALL_DATA_TYPES_TABLE_NAME, COMMENT, "boolean_sample");
  }

  void verify_user_type(const std::string& ks_name,
    const std::string& udt_name,
    const std::vector<std::string>& udt_datatypes) {
    std::vector<std::string> udt_field_names = cass::get_user_data_type_field_names(schema_meta_, ks_name, udt_name);
    BOOST_REQUIRE_EQUAL_COLLECTIONS(udt_datatypes.begin(), udt_datatypes.end(), udt_field_names.begin(), udt_field_names.end());
  }

  void verify_user_function(const std::string& ks_name,
    const std::string& udf_name,
    const std::vector<std::string>& udf_argument,
    const std::vector<std::string> udf_value_types,
    const std::string& udf_body,
    const std::string udf_language,
    const cass_bool_t is_called_on_null,
    const CassValueType return_value_type) {
    BOOST_REQUIRE_EQUAL(udf_argument.size(), udf_value_types.size());
    const CassFunctionMeta* func_meta = schema_get_function(ks_name, udf_name, udf_value_types);
    CassString func_meta_string_value;

    // Function name
    cass_function_meta_name(func_meta, &func_meta_string_value.data, &func_meta_string_value.length);
    BOOST_CHECK(test_utils::Value<CassString>::equal(CassString(udf_name.c_str()), func_meta_string_value));

    // Full Function name (includes datatypes of arguments)
    std::stringstream udf_full_name;
    udf_full_name << udf_name << "(" << test_utils::implode(udf_value_types, ',') << ")";
    cass_function_meta_full_name(func_meta, &func_meta_string_value.data, &func_meta_string_value.length);
    BOOST_CHECK(test_utils::Value<CassString>::equal(CassString(udf_full_name.str().c_str()), func_meta_string_value));

    // Function body
    cass_function_meta_body(func_meta, &func_meta_string_value.data, &func_meta_string_value.length);
    BOOST_CHECK(test_utils::Value<CassString>::equal(CassString(udf_body.c_str()), func_meta_string_value));

    // Function language
    cass_function_meta_language(func_meta, &func_meta_string_value.data, &func_meta_string_value.length);
    BOOST_CHECK(test_utils::Value<CassString>::equal(CassString(udf_language.c_str()), func_meta_string_value));

    // Is function called on null input
    BOOST_CHECK_EQUAL(is_called_on_null, cass_function_meta_called_on_null_input(func_meta));

    // Function argument count
    BOOST_CHECK_EQUAL(udf_value_types.size(), cass_function_meta_argument_count(func_meta));

    // Function arguments (by index)
    for (int i = 0; i < udf_argument.size(); ++i) {
      const CassDataType* datatype;
      cass_function_meta_argument(func_meta, i, &func_meta_string_value.data, &func_meta_string_value.length, &datatype);
      BOOST_CHECK_EQUAL(udf_value_types[i].compare(std::string(test_utils::get_value_type(cass_data_type_type(datatype)))), 0);
    }

    // Function arguments (by name)
    std::vector<std::string>::const_iterator value_iterator = udf_value_types.begin();
    for (std::vector<std::string>::const_iterator iterator = udf_argument.begin(); iterator != udf_argument.end(); ++iterator) {
      const CassDataType* datatype = cass_function_meta_argument_type_by_name(func_meta, (*iterator).c_str());
      BOOST_CHECK_EQUAL((*value_iterator).compare(std::string(test_utils::get_value_type(cass_data_type_type(datatype)))), 0);
      ++value_iterator;
    }

    // Function return type
    const CassDataType* return_datatype = cass_function_meta_return_type(func_meta);
    BOOST_CHECK_EQUAL(return_value_type, cass_data_type_type(return_datatype));
  }

  void verify_user_data_type() {
    create_simple_strategy_keyspace();
    test_utils::execute_query(session, "USE " SIMPLE_STRATEGY_KEYSPACE_NAME);
    std::vector<std::string> udt_datatypes;

    // New UDT
    test_utils::execute_query(session, str(boost::format("CREATE TYPE %s(integer_value int)") % USER_DATA_TYPE_NAME));
    udt_datatypes.push_back("integer_value");
    refresh_schema_meta();
    verify_user_type(SIMPLE_STRATEGY_KEYSPACE_NAME, USER_DATA_TYPE_NAME, udt_datatypes);

    // Altered UDT
    test_utils::execute_query(session, str(boost::format("ALTER TYPE %s ADD text_value text") % USER_DATA_TYPE_NAME));
    udt_datatypes.push_back("text_value");
    refresh_schema_meta();
    verify_user_type(SIMPLE_STRATEGY_KEYSPACE_NAME, USER_DATA_TYPE_NAME, udt_datatypes);

    // Dropped UDT
    test_utils::execute_query(session, str(boost::format("DROP TYPE %s") % USER_DATA_TYPE_NAME));
    udt_datatypes.clear();
    refresh_schema_meta();
    verify_user_type(SIMPLE_STRATEGY_KEYSPACE_NAME, USER_DATA_TYPE_NAME, udt_datatypes);
  }

  void create_simple_strategy_function() {
    test_utils::execute_query(session, str(boost::format("DROP FUNCTION IF EXISTS %s.%s")
      % SIMPLE_STRATEGY_KEYSPACE_NAME
      % USER_DEFINED_FUNCTION_NAME));
    test_utils::execute_query(session,
      str(boost::format("CREATE FUNCTION %s.%s(key int, val int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE javascript AS 'key + val';")
      % SIMPLE_STRATEGY_KEYSPACE_NAME
      % USER_DEFINED_FUNCTION_NAME));
    refresh_schema_meta();
  }

  void verify_user_defined_function() {
    create_simple_strategy_keyspace();

    // New UDF
    create_simple_strategy_function();
    std::vector<std::string> udf_arguments;
    std::vector<std::string> udf_value_types;
    udf_arguments.push_back("key");
    udf_arguments.push_back("val");
    udf_value_types.push_back(test_utils::get_value_type(CASS_VALUE_TYPE_INT));
    udf_value_types.push_back(test_utils::get_value_type(CASS_VALUE_TYPE_INT));
    refresh_schema_meta();
    verify_user_function(SIMPLE_STRATEGY_KEYSPACE_NAME,
      USER_DEFINED_FUNCTION_NAME, udf_arguments, udf_value_types,
      "key + val",
      "javascript", cass_false, CASS_VALUE_TYPE_INT);

    // Drop UDF
    test_utils::execute_query(session, str(boost::format("DROP FUNCTION %s.%s")
      % SIMPLE_STRATEGY_KEYSPACE_NAME
      % USER_DEFINED_FUNCTION_NAME));
    refresh_schema_meta();
    const CassFunctionMeta* func_meta = schema_get_function(SIMPLE_STRATEGY_KEYSPACE_NAME, USER_DEFINED_FUNCTION_NAME, udf_value_types);
    BOOST_REQUIRE(func_meta != NULL);
  }
};

BOOST_FIXTURE_TEST_SUITE(schema_metadata, TestSchemaMetadata)

// not modular, but speeds execution by using the same cluster
// It might be good to get away from boost suite fixtures and do a static global.
// (not going down that route ahead of test runner refactor)
BOOST_AUTO_TEST_CASE(simple) {
  verify_system_tables();// must be run first -- looking for "no other tables"
  verify_user_keyspace();
  verify_user_table();
  if (version >= CCM::CassVersion("2.1.0")) {
    verify_user_data_type();
  }
  if (version >= CCM::CassVersion("2.2.0")) {
    verify_user_defined_function();
    //verify_user_defined_aggregate();
  }
}

/**
 * Test the disabling schema metadata
 *
 * Verifies that initial schema and schema change events don't occur when
 * schema metadata is disabled.
 *
 * @since 2.1.0
 * @jira_ticket CPP-249
 * @test_category schema
 * @cassandra_version 1.2.x
 */
BOOST_AUTO_TEST_CASE(disable) {
  // Verify known keyspace
  {
    test_utils::CassSchemaMetaPtr schema_meta(cass_session_get_schema_meta(session));
    BOOST_CHECK(cass_schema_meta_keyspace_by_name(schema_meta.get(), "system") != NULL);
  }

  // Verify schema change event
  {
    test_utils::execute_query(session, "CREATE KEYSPACE ks1 WITH replication = "
                                       "{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
    verify_keyspace_created("ks1");
    test_utils::CassSchemaMetaPtr schema_meta(cass_session_get_schema_meta(session));
    BOOST_CHECK(cass_schema_meta_keyspace_by_name(schema_meta.get(), "ks1") != NULL);
  }

  close_session();


  // Disable schema and reconnect
  cass_cluster_set_use_schema(cluster, cass_false);
  create_session();

  // Verify known keyspace doesn't exist in metadata
  {
    test_utils::CassSchemaMetaPtr schema_meta(cass_session_get_schema_meta(session));
    BOOST_CHECK(cass_schema_meta_keyspace_by_name(schema_meta.get(), "system") == NULL);
  }

  // Verify schema change event didn't happen
  {
    test_utils::execute_query(session, "CREATE KEYSPACE ks2 WITH replication = "
                                       "{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
    verify_keyspace_created("ks2");
    test_utils::CassSchemaMetaPtr schema_meta(cass_session_get_schema_meta(session));
    BOOST_CHECK(cass_schema_meta_keyspace_by_name(schema_meta.get(), "ks2") == NULL);
  }

  // Drop the keyspace (ignore any and all errors)
  test_utils::execute_query_with_error(session, str(boost::format(test_utils::DROP_KEYSPACE_FORMAT) % "ks1"));
  test_utils::execute_query_with_error(session, str(boost::format(test_utils::DROP_KEYSPACE_FORMAT) % "ks2"));
}

BOOST_AUTO_TEST_SUITE_END()

