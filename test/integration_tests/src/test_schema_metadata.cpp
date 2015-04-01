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

#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "cassandra.h"
#include "testing.hpp"
#include "test_utils.hpp"

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

/**
 * Schema Metadata Test Class
 *
 * The purpose of this class is to setup a single session integration test
 * while initializing a single node cluster through CCM in order to perform
 * schema metadata validation against.
 */
struct TestSchemaMetadata : public test_utils::SingleSessionTest {
  const CassSchema* schema_;

  TestSchemaMetadata()
    : SingleSessionTest(1, 0)
    , schema_(NULL) {}

	~TestSchemaMetadata() {
    if (schema_) {
      cass_schema_free(schema_);
		}
	}

  void refresh_schema_meta() {
    if (schema_) {
      const CassSchema* old(schema_);
      schema_ = cass_session_get_schema(session);

      for (size_t i = 0;
           i < 10 && cass::get_schema_meta_from_keyspace(schema_, "system") == cass::get_schema_meta_from_keyspace(old, "system");
           ++i) {
        boost::this_thread::sleep_for(boost::chrono::milliseconds(1));
        cass_schema_free(schema_);
        schema_ = cass_session_get_schema(session);
      }
      cass_schema_free(old);
    } else {
      schema_ = cass_session_get_schema(session);
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

  const CassSchemaMeta* schema_get_keyspace(const std::string& ks_name) {
    const CassSchemaMeta* ks_meta = cass_schema_get_keyspace(schema_, ks_name.c_str());
    BOOST_REQUIRE(ks_meta);
    return ks_meta;
  }

  const CassSchemaMeta* schema_get_table(const std::string& ks_name,
                                     const std::string& table_name) {
    const CassSchemaMeta* table_meta = cass_schema_meta_get_entry(schema_get_keyspace(ks_name), table_name.c_str());
    BOOST_REQUIRE(table_meta);
    return table_meta;
  }

  const CassSchemaMeta* schema_get_column(const std::string& ks_name,
                                      const std::string& table_name,
                                      const std::string& col_name) {
    const CassSchemaMeta* col_meta = cass_schema_meta_get_entry(schema_get_table(ks_name, table_name), col_name.c_str());
    BOOST_REQUIRE(col_meta);
    return col_meta;
  }

  void verify_fields(const CassSchemaMeta* meta, const std::set<std::string>& expected_fields) {
    // all fields present, nothing extra
    std::set<std::string> observed;
    test_utils::CassIteratorPtr itr(cass_iterator_fields_from_schema_meta(meta));
    while (cass_iterator_next(itr.get())) {
      const CassSchemaMetaField* field = cass_iterator_get_schema_meta_field(itr.get());
      CassString name;
      cass_schema_meta_field_name(field, &name.data, &name.length);
      observed.insert(std::string(name.data, name.length));
      // can get same field by name as by iterator
      BOOST_CHECK_EQUAL(cass_schema_meta_get_field(meta, std::string(name.data, name.length).c_str()), field);
    }

    BOOST_REQUIRE_EQUAL_COLLECTIONS(observed.begin(), observed.end(),
                                    expected_fields.begin(), expected_fields.end());

    BOOST_CHECK(!cass_schema_meta_get_field(meta, "some bogus field"));
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

  void verify_columns(const CassSchemaMeta* table_meta) {
    test_utils::CassIteratorPtr itr(cass_iterator_from_schema_meta(table_meta));
    while (cass_iterator_next(itr.get())) {
      const CassSchemaMeta* col_meta = cass_iterator_get_schema_meta(itr.get());
      BOOST_REQUIRE_EQUAL(cass_schema_meta_type(col_meta), CASS_SCHEMA_META_TYPE_COLUMN);
      verify_fields(col_meta, column_fields());
      // no entries at this level
      BOOST_CHECK(!cass_iterator_from_schema_meta(col_meta));
      BOOST_CHECK(!cass_schema_meta_get_entry(col_meta, "some bogus entry"));
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

        if (version.minor == 1) {
          fields.insert("cf_id");
          fields.insert("max_index_interval");
          fields.insert("min_index_interval");
          fields.erase("populate_io_cache_on_flush");
          fields.erase("replicate_on_write");
        }
      }
    }
    return fields;
  }

  void verify_table(const std::string& ks_name, const std::string& table_name,
                    const std::string& comment, const std::string& non_key_column) {
    const CassSchemaMeta* table_meta = schema_get_table(SIMPLE_STRATEGY_KEYSPACE_NAME, ALL_DATA_TYPES_TABLE_NAME);

    verify_fields(table_meta, table_fields());
    verify_value(cass_schema_meta_field_value(cass_schema_meta_get_field(table_meta, "keyspace_name")), SIMPLE_STRATEGY_KEYSPACE_NAME);
    verify_value(cass_schema_meta_field_value(cass_schema_meta_get_field(table_meta, "columnfamily_name")), ALL_DATA_TYPES_TABLE_NAME);

    // not going for every field, just making sure one of each type (fixed, list, map) is correctly added
    verify_value(cass_schema_meta_field_value(cass_schema_meta_get_field(table_meta, "comment")), COMMENT);

    const CassValue* value = cass_schema_meta_field_value(cass_schema_meta_get_field(table_meta, "compression_parameters"));
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

    value = cass_schema_meta_field_value(cass_schema_meta_get_field(table_meta, "key_aliases"));
    BOOST_REQUIRE_EQUAL(cass_value_type(value), CASS_VALUE_TYPE_LIST);
    BOOST_CHECK_GE(cass_value_item_count(value), 1ul);

    BOOST_CHECK(!cass_schema_meta_get_entry(table_meta, "some bogus entry"));

    verify_columns(table_meta);

    // known column
    BOOST_REQUIRE(cass_schema_meta_get_entry(table_meta, non_key_column.c_str()));

    // goes away
    if (version.major > 1) {// dropping a column not supported in 1.2
      test_utils::execute_query(session, "ALTER TABLE "+table_name+" DROP "+non_key_column);
      refresh_schema_meta();
      table_meta = schema_get_table(SIMPLE_STRATEGY_KEYSPACE_NAME, ALL_DATA_TYPES_TABLE_NAME);
      BOOST_CHECK(!cass_schema_meta_get_entry(table_meta, non_key_column.c_str()));
    }

    // new column
    test_utils::execute_query(session, "ALTER TABLE "+table_name+" ADD jkldsfafdjsklafajklsljkfds text");
    refresh_schema_meta();
    table_meta = schema_get_table(SIMPLE_STRATEGY_KEYSPACE_NAME, ALL_DATA_TYPES_TABLE_NAME);
    BOOST_CHECK(cass_schema_meta_get_entry(table_meta, "jkldsfafdjsklafajklsljkfds"));

    // drop table
    test_utils::execute_query(session, "DROP TABLE "+table_name);
    refresh_schema_meta();
    const CassSchemaMeta* ks_meta = cass_schema_get_keyspace(schema_, SIMPLE_STRATEGY_KEYSPACE_NAME);
    table_meta = cass_schema_meta_get_entry(ks_meta, ALL_DATA_TYPES_TABLE_NAME);
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
    const CassSchemaMeta* ks_meta = schema_get_keyspace(name);
    BOOST_REQUIRE_EQUAL(cass_schema_meta_type(ks_meta), CASS_SCHEMA_META_TYPE_KEYSPACE);
    verify_fields(ks_meta, keyspace_fields());
    verify_value(cass_schema_meta_field_value(cass_schema_meta_get_field(ks_meta, "keyspace_name")), name);
    verify_value(cass_schema_meta_field_value(cass_schema_meta_get_field(ks_meta, "durable_writes")), (cass_bool_t)durable_writes);
    verify_value(cass_schema_meta_field_value(cass_schema_meta_get_field(ks_meta, "strategy_class")), strategy_class);
    verify_value(cass_schema_meta_field_value(cass_schema_meta_get_field(ks_meta, "strategy_options")), strategy_options);
    BOOST_CHECK(!cass_schema_meta_get_entry(ks_meta, "some bogus entry"));
  }


  void verify_system_tables() {
    // make sure system tables present, and nothing extra
    refresh_schema_meta();
    std::map<std::string, std::string> strategy_options;

    verify_keyspace("system", true, LOCAL_STRATEGY_CLASS_NAME, strategy_options);

    strategy_options.insert(std::make_pair("replication_factor", "2"));
    verify_keyspace("system_traces", true, SIMPLE_STRATEGY_CLASS_NAME, strategy_options);

    test_utils::CassIteratorPtr itr(cass_iterator_from_schema(schema_));
    size_t keyspace_count = 0;
    while (cass_iterator_next(itr.get())) ++keyspace_count;
    BOOST_CHECK_EQUAL(keyspace_count, 2ul);
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
    test_utils::execute_query(session, "DROP KEYSPACE "SIMPLE_STRATEGY_KEYSPACE_NAME);
    refresh_schema_meta();
    BOOST_CHECK(!cass_schema_get_keyspace(schema_, SIMPLE_STRATEGY_KEYSPACE_NAME));

    // nts
    create_network_topology_strategy_keyspace(3, 2, true);
    strategy_options.clear();
    strategy_options.insert(std::make_pair("dc1", "3"));
    strategy_options.insert(std::make_pair("dc2", "2"));
    verify_keyspace(NETWORK_TOPOLOGY_KEYSPACE_NAME, true, NETWORK_TOPOLOGY_STRATEGY_CLASS_NAME, strategy_options);
    test_utils::execute_query(session, "DROP KEYSPACE "NETWORK_TOPOLOGY_KEYSPACE_NAME);
  }

  void verify_user_table() {
    create_simple_strategy_keyspace();

    test_utils::execute_query(session, "USE "SIMPLE_STRATEGY_KEYSPACE_NAME);
    test_utils::execute_query(session, str(boost::format(test_utils::CREATE_TABLE_ALL_TYPES) % ALL_DATA_TYPES_TABLE_NAME));
    test_utils::execute_query(session, "ALTER TABLE "ALL_DATA_TYPES_TABLE_NAME" WITH comment='"COMMENT"'");
    refresh_schema_meta();

    verify_table(SIMPLE_STRATEGY_KEYSPACE_NAME, ALL_DATA_TYPES_TABLE_NAME, COMMENT, "boolean_sample");
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
}

BOOST_AUTO_TEST_SUITE_END()

