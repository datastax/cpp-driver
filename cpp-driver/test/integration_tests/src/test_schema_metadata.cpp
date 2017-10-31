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
#define USER_DEFINED_AGGREGATE_NAME "user_defined_aggregate"
#define USER_DEFINED_AGGREGATE_FINAL_FUNCTION_NAME "uda_udf_final"

#define RETRIEVE_METADATA(meta, func, type) do { \
  int attempts = 0; \
  while (!meta && attempts < 10) { \
    meta = func; \
    if (!meta) { \
      std::cout << type << " metadata is not valid; initiating schema refresh" << std::endl; \
      boost::this_thread::sleep_for(boost::chrono::milliseconds(1000)); \
      refresh_schema_meta(); \
    } \
    ++attempts; \
  } \
} while(0)

namespace std {

std::ostream& operator<<(std::ostream& os, const CassString& str) {
  return os << std::string(str.data, str.length);
}

std::ostream& operator<<(std::ostream& os, const std::pair<std::string, std::string>& p)
{
  return os << "(\"" << p.first << "\", \"" << p.second << "\")";
}

} // namespace std

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
    : SingleSessionTest(1, 0, false)
    , schema_meta_(NULL) {
    cass_cluster_set_token_aware_routing(cluster, cass_false);
    create_session();
  }

  ~TestSchemaMetadata() {
    if (schema_meta_) {
      cass_schema_meta_free(schema_meta_);
    }
  }

  void verify_keyspace_created(const std::string& ks) {
    test_utils::CassResultPtr result;

    for (int i = 0; i < 10; ++i) {
      const char* system_schema_ks = version >= "3.0.0" ? "system_schema.keyspaces" : "system.schema_keyspaces";
      test_utils::execute_query(session,
                                str(boost::format(
                                      "SELECT * FROM  %s WHERE keyspace_name = '%s'") % system_schema_ks % ks), &result);
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
        boost::this_thread::sleep_for(boost::chrono::milliseconds(1000));
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
    const CassKeyspaceMeta* ks_meta = NULL;
    RETRIEVE_METADATA(ks_meta,
                      cass_schema_meta_keyspace_by_name(schema_meta_, ks_name.c_str()),
                      "Keyspace");
    BOOST_REQUIRE(ks_meta);
    return ks_meta;
  }

  const CassTableMeta* schema_get_table(const std::string& ks_name,
                                     const std::string& table_name) {
    const CassTableMeta* table_meta = NULL;
    RETRIEVE_METADATA(table_meta,
                      cass_keyspace_meta_table_by_name(schema_get_keyspace(ks_name),
                                                       table_name.c_str()),
                      "Table");
    BOOST_REQUIRE(table_meta);
    return table_meta;
  }

  const CassMaterializedViewMeta* schema_get_view(const std::string& ks_name,
                                                  const std::string& view_name) {
    const CassMaterializedViewMeta* view_meta = NULL;
    RETRIEVE_METADATA(view_meta,
                      cass_keyspace_meta_materialized_view_by_name(schema_get_keyspace(ks_name),
                                                                   view_name.c_str()),
                      "View");
    BOOST_REQUIRE(view_meta);
    return view_meta;
  }

  const CassColumnMeta* schema_get_column(const std::string& ks_name,
                                      const std::string& table_name,
                                      const std::string& col_name) {
    const CassColumnMeta* col_meta = NULL;
    RETRIEVE_METADATA(col_meta,
                      cass_table_meta_column_by_name(schema_get_table(ks_name, table_name),
                                                     col_name.c_str()),
                      "Column");
    BOOST_REQUIRE(col_meta);
    return col_meta;
  }

  const CassFunctionMeta* schema_get_function(const std::string& ks_name,
                              const std::string& func_name,
                              const std::vector<std::string>& func_types) {

    const CassFunctionMeta* func_meta = NULL;
    RETRIEVE_METADATA(func_meta,
                      cass_keyspace_meta_function_by_name(schema_get_keyspace(ks_name),
                                                          func_name.c_str(),
                                                          test_utils::implode(func_types, ',').c_str()),
                      "Function");
    BOOST_REQUIRE(func_meta);
    return func_meta;
  }

  const CassAggregateMeta* schema_get_aggregate(const std::string& ks_name,
                                const std::string& agg_name,
                                const std::vector<std::string>& agg_types) {
    const CassAggregateMeta* agg_meta = NULL;
    RETRIEVE_METADATA(agg_meta,
                      cass_keyspace_meta_aggregate_by_name(schema_get_keyspace(ks_name),
                                                           agg_name.c_str(),
                                                           test_utils::implode(agg_types, ',').c_str()),
                      "Aggregate");
    BOOST_REQUIRE(agg_meta);
    return agg_meta;
  }

  void verify_function_dropped(const std::string& ks_name,
                               const std::string& func_name,
                               const std::vector<std::string>& func_types) {
    const CassFunctionMeta* func_meta = cass_keyspace_meta_function_by_name(schema_get_keyspace(ks_name),
                                                                            func_name.c_str(),
                                                                            test_utils::implode(func_types, ',').c_str());
    BOOST_CHECK(func_meta == NULL);
  }

  void verify_aggregate_dropped(const std::string& ks_name,
                                const std::string& agg_name,
                                const std::vector<std::string>& agg_types) {
    const CassAggregateMeta* agg_meta = cass_keyspace_meta_aggregate_by_name(schema_get_keyspace(ks_name),
                                                                             agg_name.c_str(),
                                                                             test_utils::implode(agg_types, ',').c_str());
    BOOST_CHECK(agg_meta == NULL);
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

  void verify_index(const CassIndexMeta* index_meta,
                    const std::string& index_name, CassIndexType index_type,
                    const std::string& index_target, const std::map<std::string, std::string>& index_options) {
    BOOST_REQUIRE(index_meta != NULL);

    CassString name;
    cass_index_meta_name(index_meta, &name.data, &name.length);
    BOOST_CHECK(name == CassString(index_name.c_str()));

    CassString target;
    cass_index_meta_target(index_meta, &target.data, &target.length);
    BOOST_CHECK(target == CassString(index_target.c_str()));

    CassIndexType type = cass_index_meta_type(index_meta);
    BOOST_CHECK(type == index_type);

    const CassValue* options = cass_index_meta_options(index_meta);

    if (cass_value_is_null(options)) {
      BOOST_CHECK(index_options.empty());
      return;
    }

    test_utils::CassIteratorPtr iterator(cass_iterator_from_map(options));

    std::map<std::string, std::string> actual_index_options;
    while (cass_iterator_next(iterator.get())) {
      CassString k, v;
      const CassValue* key = cass_iterator_get_map_key(iterator.get());
      cass_value_get_string(key, &k.data, &k.length);
      const CassValue* value = cass_iterator_get_map_value(iterator.get());
      cass_value_get_string(value, &v.data, &v.length);
      actual_index_options[std::string(k.data, k.length)] = std::string(v.data, v.length);
    }

    BOOST_CHECK_EQUAL_COLLECTIONS(actual_index_options.begin(), actual_index_options.end(),
                                  index_options.begin(), index_options.end());
  }


  const std::set<std::string>& column_fields() {
    static std::set<std::string> fields;
    if (fields.empty()) {
      if (version >= "3.0.0") {
        fields.insert("keyspace_name");
        fields.insert("table_name");
        fields.insert("column_name");
        fields.insert("clustering_order");
        fields.insert("column_name_bytes");
        fields.insert("kind");
        fields.insert("position");
        fields.insert("type");
      } else {
        fields.insert("keyspace_name");
        fields.insert("columnfamily_name");
        fields.insert("column_name");
        fields.insert("component_index");
        fields.insert("index_name");
        fields.insert("index_options");
        fields.insert("index_type");
        fields.insert("validator");

        if (version >= "2.0.0") {
          fields.insert("type");
        }
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
      CassColumnType col_type = cass_column_meta_type(col_meta);
      // C* 1.2.0 columns won't have fields for partition and clustering key columns
      if (version >= "2.0.0" ||
          (col_type != CASS_COLUMN_TYPE_CLUSTERING_KEY &&
           col_type != CASS_COLUMN_TYPE_PARTITION_KEY)) {
        verify_fields(test_utils::CassIteratorPtr(cass_iterator_fields_from_column_meta(col_meta)), column_fields());
        verify_fields_by_name(col_meta, column_fields());
      }
      // no entries at this level
      BOOST_CHECK(!cass_column_meta_field_by_name(col_meta, "some bogus entry"));
    }
  }

  const std::set<std::string>& table_fields() {
    static std::set<std::string> fields;
    if (fields.empty()) {
      if (version >= "3.0.0") {
        fields.insert("keyspace_name");
        fields.insert("table_name");
        fields.insert("bloom_filter_fp_chance");
        fields.insert("caching");
        fields.insert("comment");
        fields.insert("compaction");
        fields.insert("compression");
        fields.insert("crc_check_chance");
        fields.insert("dclocal_read_repair_chance");
        fields.insert("default_time_to_live");
        fields.insert("extensions");
        fields.insert("flags");
        fields.insert("gc_grace_seconds");
        fields.insert("id");
        fields.insert("max_index_interval");
        fields.insert("memtable_flush_period_in_ms");
        fields.insert("min_index_interval");
        fields.insert("read_repair_chance");
        fields.insert("speculative_retry");
      } else {
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

        if (version >= "2.0.0") {
          fields.insert("default_time_to_live");
          fields.insert("dropped_columns");
          fields.erase("id");
          fields.insert("index_interval");
          fields.insert("is_dense");
          fields.erase("key_alias");
          fields.insert("memtable_flush_period_in_ms");
          fields.insert("speculative_retry");

          if (version >= "2.1.0") {
            fields.insert("cf_id");
            fields.insert("max_index_interval");
            fields.insert("min_index_interval");
            fields.erase("populate_io_cache_on_flush");
            fields.erase("replicate_on_write");
          }

          if (version >= "2.2.0") {
            fields.erase("column_aliases");
            fields.erase("key_aliases");
            fields.erase("value_alias");
            fields.erase("index_interval");
          }
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
    verify_value(cass_table_meta_field_by_name(table_meta, version >= "3.0.0" ? "table_name" : "columnfamily_name"), ALL_DATA_TYPES_TABLE_NAME);

    // not going for every field, just making sure one of each type (fixed, list, map) is correctly added
    verify_value(cass_table_meta_field_by_name(table_meta, "comment"), COMMENT);


    const CassValue* value = cass_table_meta_field_by_name(table_meta, version >= "3.0.0" ? "compression" : "compression_parameters");
    BOOST_REQUIRE(value);
    BOOST_REQUIRE_EQUAL(cass_value_type(value), CASS_VALUE_TYPE_MAP);
    BOOST_REQUIRE_GE(cass_value_item_count(value), 1ul);
    test_utils::CassIteratorPtr itr(cass_iterator_from_map(value));
    const std::string parameter = version >= "3.0.0." ? "class" : "sstable_compression";
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

    if (version >= "3.0.0") {
      value = cass_table_meta_field_by_name(table_meta, "id");
      BOOST_REQUIRE_EQUAL(cass_value_type(value), CASS_VALUE_TYPE_UUID);
    } else if (version >= "2.1.0") {
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
    if (version >= "2.0.0") {// dropping a column not supported in 1.2
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

  void verify_materialized_view_count(const std::string& keyspace_name,
                                      size_t count) {
    size_t actual_count = 0;
    size_t attempts = 0;
    // Allow for extra attempts in the event the schema needs to be refreshed
    while (actual_count != count && attempts < 10) {
      const CassKeyspaceMeta* keyspace_meta = schema_get_keyspace("materialized_views");
      test_utils::CassIteratorPtr iterator(cass_iterator_materialized_views_from_keyspace_meta(keyspace_meta));
      while (cass_iterator_next(iterator.get())) {
        actual_count++;
      }
      if (actual_count != count) {
        std::cout << "View count is not valid; initiating schema refresh" << std::endl;
        boost::this_thread::sleep_for(boost::chrono::milliseconds(1000));
        refresh_schema_meta();
      }
      ++attempts;
    }
    BOOST_CHECK_EQUAL(actual_count, count);
  }

  void verify_materialized_view(const CassMaterializedViewMeta* view,
                                const std::string& view_name,
                                const std::string& view_base_table_name,
                                const std::string& view_columns,
                                const std::string& view_partition_key,
                                const std::string& view_clustering_key) {
    BOOST_REQUIRE(view != NULL);

    CassString name;
    cass_materialized_view_meta_name(view, &name.data, &name.length);
    BOOST_CHECK(name == CassString(view_name.c_str()));

    const CassTableMeta* base_table = cass_materialized_view_meta_base_table(view);
    CassString base_table_name;
    cass_table_meta_name(base_table, &base_table_name.data, &base_table_name.length);
    BOOST_CHECK(base_table_name == CassString(view_base_table_name.c_str()));

    std::vector<std::string> columns;
    test_utils::explode(view_columns, columns);
    BOOST_REQUIRE_EQUAL(cass_materialized_view_meta_column_count(view),  columns.size());

    test_utils::CassIteratorPtr iterator(cass_iterator_columns_from_materialized_view_meta(view));
    for (std::vector<std::string>::const_iterator i = columns.begin(),
         end = columns.end(); i != end; ++i) {
      BOOST_REQUIRE(cass_iterator_next(iterator.get()));
      const CassColumnMeta* column = cass_iterator_get_column_meta(iterator.get());

      CassString column_name;
      cass_column_meta_name(column, &column_name.data, &column_name.length);

      BOOST_CHECK(column_name == CassString(i->c_str()));
    }
    BOOST_CHECK(cass_iterator_next(iterator.get()) == cass_false);

    for (size_t i = 0; i < columns.size(); ++i) {
      const CassColumnMeta* column = cass_materialized_view_meta_column(view, i);

      CassString column_name;
      cass_column_meta_name(column, &column_name.data, &column_name.length);

      BOOST_CHECK_EQUAL(column_name, CassString(columns[i].c_str()));
    }

    std::vector<std::string> partition_key;
    test_utils::explode(view_partition_key, partition_key);
    BOOST_REQUIRE_EQUAL(cass_materialized_view_meta_partition_key_count(view), partition_key.size());
    for (size_t i = 0; i < partition_key.size(); ++i) {
      const CassColumnMeta* column = cass_materialized_view_meta_partition_key(view, i);

      CassString column_name;
      cass_column_meta_name(column, &column_name.data, &column_name.length);

      BOOST_CHECK_EQUAL(column_name, CassString(partition_key[i].c_str()));
    }

    std::vector<std::string> clustering_key;
    test_utils::explode(view_clustering_key, clustering_key);
    BOOST_REQUIRE_EQUAL(cass_materialized_view_meta_clustering_key_count(view), clustering_key.size());
    for (size_t i = 0; i < clustering_key.size(); ++i) {
      const CassColumnMeta* column = cass_materialized_view_meta_clustering_key(view, i);

      CassString column_name;
      cass_column_meta_name(column, &column_name.data, &column_name.length);

      BOOST_CHECK_EQUAL(column_name, CassString(clustering_key[i].c_str()));
    }
  }

  const std::set<std::string>& keyspace_fields() {
    static std::set<std::string> fields;
    if (fields.empty()) {
      fields.insert("keyspace_name");
      fields.insert("durable_writes");
      if (version >= "3.0.0") {
        fields.insert("replication");
      } else {
        fields.insert("strategy_class");
        fields.insert("strategy_options");
      }
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
    //verify_value(cass_keyspace_meta_field_by_name(ks_meta, "durable_writes"), (cass_bool_t)durable_writes);
    if (version >= "3.0.0") {
      std::map<std::string, std::string> replication(strategy_options);
      replication["class"] = strategy_class;
      verify_value(cass_keyspace_meta_field_by_name(ks_meta, "replication"), replication);
    } else {
      verify_value(cass_keyspace_meta_field_by_name(ks_meta, "strategy_class"), strategy_class);
      verify_value(cass_keyspace_meta_field_by_name(ks_meta, "strategy_options"), strategy_options);
    }
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
    if (ccm->is_dse() && ccm->get_dse_version() >= "5.0.0") {
      number_of_default_keyspaces = 9;
    } else if (version >= "3.0.0") {
      number_of_default_keyspaces = 5;
    } else if (version >= "2.2.0") {
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
    refresh_schema_meta();
    test_utils::execute_query(session, "ALTER TABLE " ALL_DATA_TYPES_TABLE_NAME " WITH comment='" COMMENT "'");
    refresh_schema_meta();

    verify_table(SIMPLE_STRATEGY_KEYSPACE_NAME, ALL_DATA_TYPES_TABLE_NAME, COMMENT, "boolean_sample");
  }

  std::vector<std::string> get_user_data_type_field_names(const std::string& ks_name, const std::string& udt_name) {
    std::vector<std::string> result;

    const CassKeyspaceMeta* ks_meta = cass_schema_meta_keyspace_by_name(schema_meta_, ks_name.c_str());
    if (ks_meta) {
      const CassDataType* data_type = cass_keyspace_meta_user_type_by_name(ks_meta, udt_name.c_str());
      if (data_type) {
        size_t count = cass_data_type_sub_type_count(data_type);
        for (size_t i = 0; i < count; ++i) {
          const char *name;
          size_t name_length;
          if (cass_data_type_sub_type_name(data_type, i, &name, &name_length) == CASS_OK) {
            result.push_back(std::string(name, name_length));
          }
        }
      }
    }

    return result;
  }

  void verify_user_type(const std::string& ks_name,
    const std::string& udt_name,
    const std::vector<std::string>& udt_datatypes) {
    std::vector<std::string> udt_field_names = get_user_data_type_field_names(ks_name.c_str(), udt_name.c_str());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(udt_datatypes.begin(), udt_datatypes.end(), udt_field_names.begin(), udt_field_names.end());
  }

  void verify_partition_key(const CassTableMeta* table_meta,
                            size_t index,
                            const std::string& column_name) {
    CassString actual_name;
    const CassColumnMeta* column_meta = cass_table_meta_partition_key(table_meta, index);
    BOOST_REQUIRE(column_meta);
    cass_column_meta_name(column_meta, &actual_name.data, &actual_name.length);
    BOOST_CHECK_EQUAL(std::string(actual_name.data, actual_name.length), column_name);
    BOOST_CHECK_EQUAL(cass_column_meta_type(column_meta), CASS_COLUMN_TYPE_PARTITION_KEY);
  }

  void verify_clustering_key(const CassTableMeta* table_meta,
                             size_t index,
                             const std::string& column_name) {
    CassString actual_name;
    const CassColumnMeta* column_meta = cass_table_meta_clustering_key(table_meta, index);
    BOOST_REQUIRE(column_meta);
    cass_column_meta_name(column_meta, &actual_name.data, &actual_name.length);
    BOOST_CHECK_EQUAL(std::string(actual_name.data, actual_name.length), column_name);
    BOOST_CHECK_EQUAL(cass_column_meta_type(column_meta), CASS_COLUMN_TYPE_CLUSTERING_KEY);
  }

  void verify_column_order(const CassTableMeta* table_meta,
                           size_t partition_key_size,
                           size_t clustering_key_size,
                           size_t column_count) {
    size_t actual_column_count = cass_table_meta_column_count(table_meta);
    BOOST_REQUIRE(partition_key_size + clustering_key_size <= actual_column_count);

    size_t index = 0;
    for (; index < partition_key_size; ++index) {
      const CassColumnMeta* column_meta = cass_table_meta_column(table_meta, index);
      BOOST_REQUIRE(column_meta);
      BOOST_CHECK_EQUAL(cass_column_meta_type(column_meta), CASS_COLUMN_TYPE_PARTITION_KEY);
    }
    for (; index < clustering_key_size; ++index) {
      const CassColumnMeta* column_meta = NULL;
      RETRIEVE_METADATA(column_meta, cass_table_meta_column(table_meta, index),
                        "Column");
      BOOST_REQUIRE(column_meta);
      BOOST_CHECK_EQUAL(cass_column_meta_type(column_meta), CASS_COLUMN_TYPE_CLUSTERING_KEY);
    }

    BOOST_CHECK_EQUAL(actual_column_count, column_count);
  }

  void verify_user_function(const std::string& ks_name,
    const std::string& udf_name,
    const std::vector<std::string>& udf_argument,
    const std::vector<std::string>& udf_value_types,
    const std::string& udf_body,
    const std::string& udf_language,
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
    for (size_t i = 0; i < udf_argument.size(); ++i) {
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

  template <class T>
  void verify_user_aggregate(const std::string& ks_name,
    const std::string& udf_name, std::string udf_final_name,
    const std::string& uda_name, const std::vector<std::string>& uda_value_types,
    const CassValueType return_value_type,
    const CassValueType state_value_type,
    T init_cond_value) {
    const CassAggregateMeta* agg_meta = schema_get_aggregate(ks_name, uda_name, uda_value_types);
    CassString agg_meta_string_value;

    // Aggregate name
    cass_aggregate_meta_name(agg_meta, &agg_meta_string_value.data, &agg_meta_string_value.length);
    BOOST_CHECK(test_utils::Value<CassString>::equal(CassString(uda_name.c_str()), agg_meta_string_value));

    // Full Aggregate name (includes datatypes of arguments)
    std::stringstream uda_full_name;
    uda_full_name << uda_name << "(" << test_utils::implode(uda_value_types, ',') << ")";
    cass_aggregate_meta_full_name(agg_meta, &agg_meta_string_value.data, &agg_meta_string_value.length);
    BOOST_CHECK(test_utils::Value<CassString>::equal(CassString(uda_full_name.str().c_str()), agg_meta_string_value));

    // Aggregate argument count
    BOOST_CHECK_EQUAL(uda_value_types.size(), cass_aggregate_meta_argument_count(agg_meta));

    // Aggregate argument (indexes only)
    for (size_t i = 0; i < uda_value_types.size(); ++i) {
      const CassDataType* datatype = cass_aggregate_meta_argument_type(agg_meta, i);
      BOOST_CHECK_EQUAL(uda_value_types[i].compare(std::string(test_utils::get_value_type(cass_data_type_type(datatype)))), 0);
    }

    // Aggregate return type
    const CassDataType* return_datatype = cass_aggregate_meta_return_type(agg_meta);
    BOOST_CHECK_EQUAL(return_value_type, cass_data_type_type(return_datatype));

    // Aggregate state type
    const CassDataType* state_datatype = cass_aggregate_meta_state_type(agg_meta);
    BOOST_CHECK_EQUAL(state_value_type, cass_data_type_type(state_datatype));

    // Aggregate state function
    const CassFunctionMeta* func_meta = cass_aggregate_meta_state_func(agg_meta);
    cass_function_meta_name(func_meta, &agg_meta_string_value.data, &agg_meta_string_value.length);
    BOOST_CHECK(test_utils::Value<CassString>::equal(CassString(udf_name.c_str()), agg_meta_string_value));

    // Aggregate final function
    func_meta = cass_aggregate_meta_final_func(agg_meta);
    cass_function_meta_name(func_meta, &agg_meta_string_value.data, &agg_meta_string_value.length);
    BOOST_CHECK(test_utils::Value<CassString>::equal(CassString(udf_final_name.c_str()), agg_meta_string_value));

    const CassValue* agg_init_cond = cass_aggregate_meta_init_cond(agg_meta);
    BOOST_REQUIRE(agg_init_cond);
    // Aggregate initial condition (type and value check)
    if (version >= "3.0.0") {
      std::stringstream ss;
      ss << init_cond_value;
      std::string s(ss.str());
      CassString agg_init_cond_value;
      BOOST_CHECK(cass_value_type(agg_init_cond) == CASS_VALUE_TYPE_VARCHAR);
      BOOST_CHECK(test_utils::Value<CassString>::get(agg_init_cond, &agg_init_cond_value) == CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassString>::equal(CassString(s.data(), s.length()), agg_init_cond_value));
    } else {
      T agg_init_cond_value;
      BOOST_CHECK(cass_value_type(agg_init_cond) == return_value_type);
      BOOST_CHECK(test_utils::Value<T>::get(agg_init_cond, &agg_init_cond_value) == CASS_OK);
      BOOST_REQUIRE(test_utils::Value<T>::equal(init_cond_value, agg_init_cond_value));
    }
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

  void create_simple_strategy_functions() {
    test_utils::execute_query(session,
      str(boost::format("CREATE OR REPLACE FUNCTION %s.%s(rhs int, lhs int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE javascript AS 'lhs + rhs';")
      % SIMPLE_STRATEGY_KEYSPACE_NAME
      % USER_DEFINED_FUNCTION_NAME));
  }

  void verify_user_defined_function() {
    create_simple_strategy_keyspace();

    // New UDF
    create_simple_strategy_functions();
    std::vector<std::string> udf_arguments;
    std::vector<std::string> udf_value_types;
    udf_arguments.push_back("lhs");
    udf_arguments.push_back("rhs");
    udf_value_types.push_back(test_utils::get_value_type(CASS_VALUE_TYPE_INT));
    udf_value_types.push_back(test_utils::get_value_type(CASS_VALUE_TYPE_INT));
    refresh_schema_meta();
    verify_user_function(SIMPLE_STRATEGY_KEYSPACE_NAME,
      USER_DEFINED_FUNCTION_NAME, udf_arguments, udf_value_types,
      test_utils::implode(udf_arguments, '+', " ", " "),
      "javascript", cass_false, CASS_VALUE_TYPE_INT);

    // Drop UDF
    test_utils::execute_query(session, str(boost::format("DROP FUNCTION %s.%s")
      % SIMPLE_STRATEGY_KEYSPACE_NAME
      % USER_DEFINED_FUNCTION_NAME));
    refresh_schema_meta();
    verify_function_dropped(SIMPLE_STRATEGY_KEYSPACE_NAME, USER_DEFINED_FUNCTION_NAME, udf_value_types);
  }

  void create_simple_strategy_aggregate() {
    create_simple_strategy_functions();
    test_utils::execute_query(session,
      str(boost::format("CREATE OR REPLACE FUNCTION %s.%s(val int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE javascript AS 'val * val';")
      % SIMPLE_STRATEGY_KEYSPACE_NAME
      % USER_DEFINED_AGGREGATE_FINAL_FUNCTION_NAME));

    test_utils::execute_query(session,
      str(boost::format("CREATE OR REPLACE AGGREGATE %s.%s(int) SFUNC %s STYPE int FINALFUNC %s INITCOND 0")
      % SIMPLE_STRATEGY_KEYSPACE_NAME
      % USER_DEFINED_AGGREGATE_NAME
      % USER_DEFINED_FUNCTION_NAME
      % USER_DEFINED_AGGREGATE_FINAL_FUNCTION_NAME));
  }

  void verify_user_defined_aggregate() {
    create_simple_strategy_keyspace();

    // New UDA
    create_simple_strategy_aggregate();
    std::vector<std::string> uda_value_types;
    uda_value_types.push_back(test_utils::get_value_type(CASS_VALUE_TYPE_INT));
    refresh_schema_meta();
    verify_user_aggregate<cass_int32_t>(SIMPLE_STRATEGY_KEYSPACE_NAME,
      USER_DEFINED_FUNCTION_NAME, USER_DEFINED_AGGREGATE_FINAL_FUNCTION_NAME,
      USER_DEFINED_AGGREGATE_NAME, uda_value_types,
      CASS_VALUE_TYPE_INT, CASS_VALUE_TYPE_INT,
      0);

    // Drop UDA
    test_utils::execute_query(session, str(boost::format("DROP AGGREGATE %s.%s")
      % SIMPLE_STRATEGY_KEYSPACE_NAME
      % USER_DEFINED_AGGREGATE_NAME));
    refresh_schema_meta();
    verify_aggregate_dropped(SIMPLE_STRATEGY_KEYSPACE_NAME, USER_DEFINED_AGGREGATE_NAME, uda_value_types);
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
  if (version >= "2.1.0") {
    verify_user_data_type();
  }
  if (version >= "2.2.0") {
    verify_user_defined_function();
    verify_user_defined_aggregate();
  }
}

/**
 * Test column partition and clustering keys
 *
 * Verifies that the partition and clustering keys are properly
 * categorized.
 *
 * @since 2.2.0
 * @jira_ticket CPP-301
 * @jira_ticket CPP-306
 * @test_category schema
 * @cassandra_version 1.2.x
 */
BOOST_AUTO_TEST_CASE(keys) {
  test_utils::execute_query(session, "CREATE KEYSPACE keys WITH replication = "
                                     "{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
  refresh_schema_meta();

  {
    test_utils::execute_query(session, "CREATE TABLE keys.single_partition_key (key text, value text, PRIMARY KEY(key))");
    refresh_schema_meta();

    const CassTableMeta* table_meta = schema_get_table("keys", "single_partition_key");

    BOOST_REQUIRE_EQUAL(cass_table_meta_partition_key_count(table_meta), 1);
    verify_partition_key(table_meta, 0, "key");

    verify_column_order(table_meta, 1, 0, 2);
  }

  {
    test_utils::execute_query(session, "CREATE TABLE keys.composite_partition_key (key1 text, key2 text, value text, "
                                       "PRIMARY KEY((key1, key2)))");
    refresh_schema_meta();

    const CassTableMeta* table_meta = schema_get_table("keys", "composite_partition_key");

    BOOST_REQUIRE_EQUAL(cass_table_meta_partition_key_count(table_meta), 2);
    verify_partition_key(table_meta, 0, "key1");
    verify_partition_key(table_meta, 1, "key2");

    verify_column_order(table_meta, 2, 0, 3);
  }

  {
    test_utils::execute_query(session, "CREATE TABLE keys.composite_key (key1 text, key2 text, value text, "
                                       "PRIMARY KEY(key1, key2))");
    refresh_schema_meta();

    const CassTableMeta* table_meta = schema_get_table("keys", "composite_key");

    BOOST_REQUIRE_EQUAL(cass_table_meta_partition_key_count(table_meta), 1);
    verify_partition_key(table_meta, 0, "key1");

    BOOST_REQUIRE_EQUAL(cass_table_meta_clustering_key_count(table_meta), 1);
    verify_clustering_key(table_meta, 0, "key2");

    verify_column_order(table_meta, 1, 1, 3);
  }

  {
    test_utils::execute_query(session, "CREATE TABLE keys.composite_clustering_key (key1 text, key2 text, key3 text, value text, "
                                       "PRIMARY KEY(key1, key2, key3))");
    refresh_schema_meta();

    const CassTableMeta* table_meta = schema_get_table("keys", "composite_clustering_key");

    BOOST_REQUIRE_EQUAL(cass_table_meta_partition_key_count(table_meta), 1);
    verify_partition_key(table_meta, 0, "key1");

    BOOST_REQUIRE_EQUAL(cass_table_meta_clustering_key_count(table_meta), 2);
    verify_clustering_key(table_meta, 0, "key2");
    verify_clustering_key(table_meta, 1, "key3");

    verify_column_order(table_meta, 1, 2, 4);
  }

  {
    test_utils::execute_query(session, "CREATE TABLE keys.composite_partition_and_clustering_key (key1 text, key2 text, key3 text, key4 text, value text, "
                                       "PRIMARY KEY((key1, key2), key3, key4))");
    refresh_schema_meta();

    const CassTableMeta* table_meta = schema_get_table("keys", "composite_partition_and_clustering_key");

    BOOST_REQUIRE_EQUAL(cass_table_meta_partition_key_count(table_meta), 2);
    verify_partition_key(table_meta, 0, "key1");
    verify_partition_key(table_meta, 1, "key2");

    BOOST_REQUIRE_EQUAL(cass_table_meta_clustering_key_count(table_meta), 2);
    verify_clustering_key(table_meta, 0, "key3");
    verify_clustering_key(table_meta, 1, "key4");

    verify_column_order(table_meta, 2, 2, 5);
  }
}

/**
 * Test dense table
 *
 * Verifies that the columns in dense table metadata excludes the surrogate column.
 *
 * @since 2.2.0
 * @jira_ticket CPP-432
 * @test_category schema
 * @cassandra_version 2.1.x
 */
BOOST_AUTO_TEST_CASE(dense_table) {
  test_utils::execute_query(session, "CREATE KEYSPACE dense WITH replication = "
    "{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
  refresh_schema_meta();

  test_utils::execute_query(session,
                            "CREATE TABLE dense.my_table (key text, value text, PRIMARY KEY(key, value)) WITH COMPACT STORAGE");
  refresh_schema_meta();

  const CassTableMeta *table_meta = schema_get_table("dense", "my_table");

  BOOST_REQUIRE_EQUAL(cass_table_meta_partition_key_count(table_meta), 1);
  verify_partition_key(table_meta, 0, "key");

  BOOST_REQUIRE_EQUAL(cass_table_meta_clustering_key_count(table_meta), 1);
  verify_clustering_key(table_meta, 0, "value");

  verify_column_order(table_meta, 1, 1, 2);
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
    refresh_schema_meta();
    test_utils::execute_query(session, "CREATE KEYSPACE ks1 WITH replication = "
                                       "{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
    refresh_schema_meta();
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

/**
 * Test Cassandra version.
 *
 * @since 2.3.0
 * @jira_ticket CPP-332
 * @test_category schema
 * @cassandra_version 1.2.x
 */
BOOST_AUTO_TEST_CASE(cassandra_version) {
  refresh_schema_meta();

  CassVersion cass_version = cass_schema_meta_version(schema_meta_);

  BOOST_CHECK(cass_version.major_version == version.major_version);
  BOOST_CHECK(cass_version.minor_version == version.minor_version);
  BOOST_CHECK(cass_version.patch_version == version.patch_version);
}

/**
 * Test clustering order.
 *
 * Verify that column clustering order is properly updated and returned.
 *
 * @since 2.3.0
 * @jira_ticket CPP-332
 * @test_category schema
 * @cassandra_version 1.2.x
 */
BOOST_AUTO_TEST_CASE(clustering_order) {
  test_utils::execute_query(session, "CREATE KEYSPACE clustering_order WITH replication = "
                                     "{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
  refresh_schema_meta();

  {
    test_utils::execute_query(session, "CREATE TABLE clustering_order.single_partition_key (key text, value text, PRIMARY KEY(key))");
    refresh_schema_meta();

    const CassTableMeta* table_meta = schema_get_table("clustering_order", "single_partition_key");

    BOOST_REQUIRE_EQUAL(cass_table_meta_clustering_key_count(table_meta), 0);
    BOOST_CHECK_EQUAL(cass_table_meta_clustering_key_order(table_meta, 0), CASS_CLUSTERING_ORDER_NONE);
  }

  {
    test_utils::execute_query(session, "CREATE TABLE clustering_order.composite_key (key1 int, key2 text, value text, "
                                       "PRIMARY KEY(key1, key2))");
    refresh_schema_meta();

    const CassTableMeta* table_meta = schema_get_table("clustering_order", "composite_key");

    BOOST_REQUIRE_EQUAL(cass_table_meta_clustering_key_count(table_meta), 1);
    BOOST_CHECK_EQUAL(cass_table_meta_clustering_key_order(table_meta, 0), CASS_CLUSTERING_ORDER_ASC);
  }

  {
    test_utils::execute_query(session, "CREATE TABLE clustering_order.composite_clustering_key (key1 text, key2 text, key3 text, value text, "
                                       "PRIMARY KEY(key1, key2, key3))");
    refresh_schema_meta();

    const CassTableMeta* table_meta = schema_get_table("clustering_order", "composite_clustering_key");

    BOOST_REQUIRE_EQUAL(cass_table_meta_clustering_key_count(table_meta), 2);
    BOOST_CHECK_EQUAL(cass_table_meta_clustering_key_order(table_meta, 0), CASS_CLUSTERING_ORDER_ASC);
    BOOST_CHECK_EQUAL(cass_table_meta_clustering_key_order(table_meta, 1), CASS_CLUSTERING_ORDER_ASC);
  }

  {
    test_utils::execute_query(session, "CREATE TABLE clustering_order.reversed_composite_key (key1 text, key2 text, value text, "
                                       "PRIMARY KEY(key1, key2)) "
                                       "WITH CLUSTERING ORDER BY (key2 DESC)");
    refresh_schema_meta();

    const CassTableMeta* table_meta = schema_get_table("clustering_order", "reversed_composite_key");

    BOOST_REQUIRE_EQUAL(cass_table_meta_clustering_key_count(table_meta), 1);
    BOOST_CHECK_EQUAL(cass_table_meta_clustering_key_order(table_meta, 0), CASS_CLUSTERING_ORDER_DESC);
  }

  {
    test_utils::execute_query(session, "CREATE TABLE clustering_order.reversed_composite_clustering_key (key1 text, key2 text, key3 text, value text, "
                                       "PRIMARY KEY(key1, key2, key3))"
                                       "WITH CLUSTERING ORDER BY (key2 DESC, key3 DESC)");
    refresh_schema_meta();

    const CassTableMeta* table_meta = schema_get_table("clustering_order", "reversed_composite_clustering_key");

    BOOST_REQUIRE_EQUAL(cass_table_meta_clustering_key_count(table_meta), 2);
    BOOST_CHECK_EQUAL(cass_table_meta_clustering_key_order(table_meta, 0), CASS_CLUSTERING_ORDER_DESC);
    BOOST_CHECK_EQUAL(cass_table_meta_clustering_key_order(table_meta, 1), CASS_CLUSTERING_ORDER_DESC);
  }

  {
    test_utils::execute_query(session, "CREATE TABLE clustering_order.mixed_composite_clustering_key ("
                                       "key1 text, key2 text, key3 text, key4 text, value text, "
                                       "PRIMARY KEY(key1, key2, key3, key4))"
                                       "WITH CLUSTERING ORDER BY (key2 DESC, key3 ASC, key4 DESC)");
    refresh_schema_meta();

    const CassTableMeta* table_meta = schema_get_table("clustering_order", "mixed_composite_clustering_key");

    BOOST_REQUIRE_EQUAL(cass_table_meta_clustering_key_count(table_meta), 3);
    BOOST_CHECK_EQUAL(cass_table_meta_clustering_key_order(table_meta, 0), CASS_CLUSTERING_ORDER_DESC);
    BOOST_CHECK_EQUAL(cass_table_meta_clustering_key_order(table_meta, 1), CASS_CLUSTERING_ORDER_ASC);
    BOOST_CHECK_EQUAL(cass_table_meta_clustering_key_order(table_meta, 2), CASS_CLUSTERING_ORDER_DESC);
  }

  {
    test_utils::execute_query(session, "CREATE TABLE clustering_order.mixed_order_composite_clustering_key ("
                                       "key1 text, key2 text, key3 text, key4 text, value text, "
                                       "PRIMARY KEY(key1, key4, key3, key2))"
                                       "WITH CLUSTERING ORDER BY (key4 DESC, key3 ASC, key2 ASC)");
    refresh_schema_meta();

    const CassTableMeta* table_meta = schema_get_table("clustering_order", "mixed_order_composite_clustering_key");

    BOOST_REQUIRE_EQUAL(cass_table_meta_clustering_key_count(table_meta), 3);
    BOOST_CHECK_EQUAL(cass_table_meta_clustering_key_order(table_meta, 0), CASS_CLUSTERING_ORDER_DESC);
    BOOST_CHECK_EQUAL(cass_table_meta_clustering_key_order(table_meta, 1), CASS_CLUSTERING_ORDER_ASC);
    BOOST_CHECK_EQUAL(cass_table_meta_clustering_key_order(table_meta, 2), CASS_CLUSTERING_ORDER_ASC);
  }
}

/**
 * Test frozen types.
 *
 * Verify that frozen types are properly updated and returned.
 *
 * @since 2.3.0
 * @jira_ticket CPP-332
 * @test_category schema
 * @cassandra_version 2.1.x
 */
BOOST_AUTO_TEST_CASE(frozen_types) {
  if (version < "2.1.0") return;

  test_utils::execute_query(session, "CREATE KEYSPACE frozen_types WITH replication = "
                                     "{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
  refresh_schema_meta();

  {
    test_utils::execute_query(session, "CREATE TABLE frozen_types.regular_map (key text PRIMARY KEY, value map<text, text>)");
    refresh_schema_meta();

    const CassColumnMeta* column_meta = schema_get_column("frozen_types", "regular_map", "value");
    const CassDataType* data_type = cass_column_meta_data_type(column_meta);
    BOOST_CHECK_EQUAL(cass_data_type_type(data_type), CASS_VALUE_TYPE_MAP);
    BOOST_CHECK_EQUAL(cass_data_type_is_frozen(data_type), cass_false);
  }

  {
    test_utils::execute_query(session, "CREATE TABLE frozen_types.frozen_map (key text PRIMARY KEY, value frozen<map<text, text>>)");
    refresh_schema_meta();

    const CassColumnMeta* column_meta = schema_get_column("frozen_types", "frozen_map", "value");
    const CassDataType* data_type = cass_column_meta_data_type(column_meta);
    BOOST_CHECK_EQUAL(cass_data_type_type(data_type), CASS_VALUE_TYPE_MAP);
    BOOST_CHECK_EQUAL(cass_data_type_is_frozen(data_type), cass_true);
  }

  {
    test_utils::execute_query(session, "CREATE TABLE frozen_types.regular_set (key text PRIMARY KEY, value set<text>)");
    refresh_schema_meta();

    const CassColumnMeta* column_meta = schema_get_column("frozen_types", "regular_set", "value");
    const CassDataType* data_type = cass_column_meta_data_type(column_meta);
    BOOST_CHECK_EQUAL(cass_data_type_type(data_type), CASS_VALUE_TYPE_SET);
    BOOST_CHECK_EQUAL(cass_data_type_is_frozen(data_type), cass_false);
  }

  {
    test_utils::execute_query(session, "CREATE TABLE frozen_types.frozen_set (key text PRIMARY KEY, value frozen<set<text>>)");
    refresh_schema_meta();

    const CassColumnMeta* column_meta = schema_get_column("frozen_types", "frozen_set", "value");
    const CassDataType* data_type = cass_column_meta_data_type(column_meta);
    BOOST_CHECK_EQUAL(cass_data_type_type(data_type), CASS_VALUE_TYPE_SET);
    BOOST_CHECK_EQUAL(cass_data_type_is_frozen(data_type), cass_true);
  }

  {
    test_utils::execute_query(session, "CREATE TABLE frozen_types.regular_list (key text PRIMARY KEY, value list<text>)");
    refresh_schema_meta();

    const CassColumnMeta* column_meta = schema_get_column("frozen_types", "regular_list", "value");
    const CassDataType* data_type = cass_column_meta_data_type(column_meta);
    BOOST_CHECK_EQUAL(cass_data_type_type(data_type), CASS_VALUE_TYPE_LIST);
    BOOST_CHECK_EQUAL(cass_data_type_is_frozen(data_type), cass_false);
  }

  {
    test_utils::execute_query(session, "CREATE TABLE frozen_types.frozen_list (key text PRIMARY KEY, value frozen<list<text>>)");
    refresh_schema_meta();

    const CassColumnMeta* column_meta = schema_get_column("frozen_types", "frozen_list", "value");
    const CassDataType* data_type = cass_column_meta_data_type(column_meta);
    BOOST_CHECK_EQUAL(cass_data_type_type(data_type), CASS_VALUE_TYPE_LIST);
    BOOST_CHECK_EQUAL(cass_data_type_is_frozen(data_type), cass_true);
  }

  {
    test_utils::execute_query(session, "CREATE TABLE frozen_types.regular_tuple (key text PRIMARY KEY, value tuple<text, int>)");
    refresh_schema_meta();

    const CassColumnMeta* column_meta = schema_get_column("frozen_types", "regular_tuple", "value");
    const CassDataType* data_type = cass_column_meta_data_type(column_meta);
    BOOST_CHECK_EQUAL(cass_data_type_type(data_type), CASS_VALUE_TYPE_TUPLE);
    // Note: As of C* 3.0 tuples<> are always frozen
    BOOST_CHECK_EQUAL(cass_data_type_is_frozen(data_type), cass_true);
  }

  {
    test_utils::execute_query(session, "CREATE TABLE frozen_types.frozen_tuple (key text PRIMARY KEY, value frozen<tuple<text, int>>)");
    refresh_schema_meta();

    const CassColumnMeta* column_meta = schema_get_column("frozen_types", "frozen_tuple", "value");
    const CassDataType* data_type = cass_column_meta_data_type(column_meta);
    BOOST_CHECK_EQUAL(cass_data_type_type(data_type), CASS_VALUE_TYPE_TUPLE);
    BOOST_CHECK_EQUAL(cass_data_type_is_frozen(data_type), cass_true);
  }

  // Note: Non-frozen UDTs are not supported as of C* 3.0

  {
    test_utils::execute_query(session, "CREATE TYPE frozen_types.type1 (field1 text, field2 frozen<set<text>>)");
    test_utils::execute_query(session, "CREATE TABLE frozen_types.frozen_udt (key text PRIMARY KEY, value frozen<type1>)");
    refresh_schema_meta();

    const CassColumnMeta* column_meta = schema_get_column("frozen_types", "frozen_udt", "value");
    const CassDataType* data_type = cass_column_meta_data_type(column_meta);
    BOOST_CHECK_EQUAL(cass_data_type_type(data_type), CASS_VALUE_TYPE_UDT);
    BOOST_CHECK_EQUAL(cass_data_type_is_frozen(data_type), cass_true);
    BOOST_REQUIRE_EQUAL(cass_data_type_sub_type_count(data_type), 2);

    const CassDataType* key_data_type = cass_data_type_sub_data_type(data_type, 0);
    BOOST_CHECK_EQUAL(cass_data_type_type(key_data_type), CASS_VALUE_TYPE_TEXT);
    BOOST_CHECK_EQUAL(cass_data_type_is_frozen(key_data_type), cass_false);

    const CassDataType* value_data_type = cass_data_type_sub_data_type(data_type, 1);
    BOOST_CHECK_EQUAL(cass_data_type_type(value_data_type), CASS_VALUE_TYPE_SET);
    // Note: C* < 3.0.0 doesn't keep the frozen<> information for types inside UDTs
    BOOST_CHECK_EQUAL(cass_data_type_is_frozen(value_data_type), version < "3.0.0" ? cass_false : cass_true);
  }

  {
    test_utils::execute_query(session, "CREATE TABLE frozen_types.frozen_nested_map (key text PRIMARY KEY, "
                                       "value map<frozen<set<text>>, frozen<list<text>>>)");
    refresh_schema_meta();

    const CassColumnMeta* column_meta = schema_get_column("frozen_types", "frozen_nested_map", "value");
    const CassDataType* data_type = cass_column_meta_data_type(column_meta);

    BOOST_CHECK_EQUAL(cass_data_type_type(data_type), CASS_VALUE_TYPE_MAP);
    BOOST_REQUIRE_EQUAL(cass_data_type_sub_type_count(data_type), 2);
    BOOST_CHECK_EQUAL(cass_data_type_is_frozen(data_type), cass_false);

    const CassDataType* key_data_type = cass_data_type_sub_data_type(data_type, 0);
    BOOST_CHECK_EQUAL(cass_data_type_type(key_data_type), CASS_VALUE_TYPE_SET);
    BOOST_CHECK_EQUAL(cass_data_type_is_frozen(key_data_type), cass_true);

    const CassDataType* value_data_type = cass_data_type_sub_data_type(data_type, 1);
    BOOST_CHECK_EQUAL(cass_data_type_type(value_data_type), CASS_VALUE_TYPE_LIST);
    BOOST_CHECK_EQUAL(cass_data_type_is_frozen(value_data_type), cass_true);
  }

  {
    test_utils::execute_query(session, "CREATE TABLE frozen_types.frozen_nested_tuple (key text PRIMARY KEY, "
                                       "value tuple<int, text, frozen<set<text>>, frozen<list<text>>>)");
    refresh_schema_meta();

    const CassColumnMeta* column_meta = schema_get_column("frozen_types", "frozen_nested_tuple", "value");
    const CassDataType* data_type = cass_column_meta_data_type(column_meta);

    BOOST_CHECK_EQUAL(cass_data_type_type(data_type), CASS_VALUE_TYPE_TUPLE);
    BOOST_REQUIRE_EQUAL(cass_data_type_sub_type_count(data_type), 4);
    BOOST_CHECK_EQUAL(cass_data_type_is_frozen(data_type), cass_true);

    const CassDataType* key_data_type;
    const CassDataType* value_data_type ;

    key_data_type = cass_data_type_sub_data_type(data_type, 0);
    BOOST_CHECK_EQUAL(cass_data_type_type(key_data_type), CASS_VALUE_TYPE_INT);
    BOOST_CHECK_EQUAL(cass_data_type_is_frozen(key_data_type), cass_false);

    value_data_type = cass_data_type_sub_data_type(data_type, 1);
    BOOST_CHECK_EQUAL(cass_data_type_type(value_data_type), CASS_VALUE_TYPE_TEXT);
    BOOST_CHECK_EQUAL(cass_data_type_is_frozen(value_data_type), cass_false);

    key_data_type = cass_data_type_sub_data_type(data_type, 2);
    BOOST_CHECK_EQUAL(cass_data_type_type(key_data_type), CASS_VALUE_TYPE_SET);
    // Note: C* < 3.0.0 doesn't keep the frozen<> information for types inside tuple<>
    BOOST_CHECK_EQUAL(cass_data_type_is_frozen(key_data_type), version < "3.0.0" ? cass_false : cass_true);

    value_data_type = cass_data_type_sub_data_type(data_type, 3);
    BOOST_CHECK_EQUAL(cass_data_type_type(value_data_type), CASS_VALUE_TYPE_LIST);
    // Note: C* < 3.0.0 doesn't keep the frozen<> information for types inside tuple<>
    BOOST_CHECK_EQUAL(cass_data_type_is_frozen(value_data_type), version < "3.0.0" ? cass_false : cass_true);
  }
}

/**
 * Ensure UDA/UDF lookups are possible against C* 2.2+
 *
 * This test will ensure that UDA and UDF metadata can be accessed through the
 * by_name lookup methods. C* 2.2.x does not augment the arguments or return
 * types of collections with the frozen<> where C* 3.x does. This tests ensures
 * that regardless of the version of C*, lookup is still possible given the
 * correct arguments (frozen<> or not) for aggregates and functions.
 *
 * @test_category functions
 * @since 2.4.0
 * @expected_result UDA and UDF can be looked up correctly
 */
BOOST_AUTO_TEST_CASE(lookup) {
  if (version < "2.2.0") return;

  test_utils::execute_query(session, "CREATE KEYSPACE lookup WITH replication = "
    "{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
  refresh_schema_meta();

  // See https://docs.datastax.com/en/cql/3.3/cql/cql_using/useCreateUDF.html
  // and https://docs.datastax.com/en/cql/3.3/cql/cql_using/useCreateUDA.html
  {
    // Frozen added to arguments and return types in C* 3.0.0 by default for collections
    test_utils::execute_query(session, "CREATE OR REPLACE FUNCTION lookup.avg_state(state tuple<int, bigint>, val int) "
      "CALLED ON NULL INPUT RETURNS tuple<int, bigint> "
      "LANGUAGE java AS 'if (val !=null) { state.setInt(0, state.getInt(0) + 1); state.setLong(1, state.getLong(1) + val.intValue()); } return state;'");
    refresh_schema_meta();

    // Create the function arguments for the avg_state function
    std::string func_args = "tuple<int, bigint>";
    if (version >= "3.0.0") {
      // Argument data stored in C* 3.0.0 is frozen<tuple<int, bigint>>, int
      func_args = "frozen<" + func_args + ">";
    }
    func_args += ", int";

    // Ensure the function can be looked up and validate arguments and return
    const CassFunctionMeta* func_meta = cass_keyspace_meta_function_by_name(
      schema_get_keyspace("lookup"),
      "avg_state",
      func_args.c_str());
    BOOST_REQUIRE(func_meta);
    const CassDataType* datatype = cass_function_meta_argument_type_by_name(func_meta, "state");
    BOOST_CHECK_EQUAL(CASS_VALUE_TYPE_TUPLE, cass_data_type_type(datatype));
    BOOST_CHECK(cass_data_type_is_frozen(datatype));
    BOOST_CHECK_EQUAL(CASS_VALUE_TYPE_INT, cass_data_type_type(cass_data_type_sub_data_type(datatype, 0)));
    BOOST_CHECK_EQUAL(CASS_VALUE_TYPE_BIGINT, cass_data_type_type(cass_data_type_sub_data_type(datatype, 1)));
    datatype = cass_function_meta_argument_type_by_name(func_meta, "val");
    BOOST_CHECK_EQUAL(CASS_VALUE_TYPE_INT, cass_data_type_type(datatype));
    datatype = cass_function_meta_return_type(func_meta);
    BOOST_CHECK_EQUAL(CASS_VALUE_TYPE_TUPLE, cass_data_type_type(datatype));
    BOOST_CHECK_EQUAL(CASS_VALUE_TYPE_INT, cass_data_type_type(cass_data_type_sub_data_type(datatype, 0)));
    BOOST_CHECK_EQUAL(CASS_VALUE_TYPE_BIGINT, cass_data_type_type(cass_data_type_sub_data_type(datatype, 1)));

    test_utils::execute_query(session, "CREATE OR REPLACE FUNCTION lookup.avg_final(state tuple<int, bigint>) "
      "CALLED ON NULL INPUT RETURNS double "
      "LANGUAGE java AS 'double r = 0; if (state.getInt(0) == 0) return null; r = state.getLong(1); r /= state.getInt(0); return Double.valueOf(r);'");
    test_utils::execute_query(session, "CREATE AGGREGATE IF NOT EXISTS lookup.average (int) "
      "SFUNC avg_state STYPE tuple<int, bigint> "
      "FINALFUNC avg_final INITCOND (0, 0);");
    refresh_schema_meta();

    // Ensure the aggregate can be looked up and validated
    const CassAggregateMeta* agg_meta = cass_keyspace_meta_aggregate_by_name(
      schema_get_keyspace("lookup"),
      "average",
      "int");
    BOOST_REQUIRE(agg_meta);
    datatype = cass_aggregate_meta_argument_type(agg_meta, 0);
    BOOST_CHECK_EQUAL(CASS_VALUE_TYPE_INT, cass_data_type_type(datatype));
    datatype = cass_aggregate_meta_state_type(agg_meta);
    BOOST_CHECK_EQUAL(CASS_VALUE_TYPE_TUPLE, cass_data_type_type(datatype));
    BOOST_CHECK(cass_data_type_is_frozen(datatype));
    BOOST_CHECK_EQUAL(CASS_VALUE_TYPE_INT, cass_data_type_type(cass_data_type_sub_data_type(datatype, 0)));
    BOOST_CHECK_EQUAL(CASS_VALUE_TYPE_BIGINT, cass_data_type_type(cass_data_type_sub_data_type(datatype, 1)));
    datatype = cass_aggregate_meta_return_type(agg_meta);
    BOOST_CHECK_EQUAL(CASS_VALUE_TYPE_DOUBLE, cass_data_type_type(datatype));
    func_meta = cass_aggregate_meta_final_func(agg_meta);
    BOOST_CHECK_EQUAL(1, cass_function_meta_argument_count(func_meta));
    datatype = cass_function_meta_argument_type_by_name(func_meta, "state");
    BOOST_CHECK_EQUAL(CASS_VALUE_TYPE_TUPLE, cass_data_type_type(datatype));
    BOOST_CHECK(cass_data_type_is_frozen(datatype));
    BOOST_CHECK_EQUAL(CASS_VALUE_TYPE_INT, cass_data_type_type(cass_data_type_sub_data_type(datatype, 0)));
    BOOST_CHECK_EQUAL(CASS_VALUE_TYPE_BIGINT, cass_data_type_type(cass_data_type_sub_data_type(datatype, 1)));
  }
}

/**
 * Test secondary indexes
 *
 * Verifies that index metadata is correctly updated and returned.
 *
 * @since 2.3.0
 * @jira_ticket CPP-321
 * @test_category schema
 * @cassandra_version 1.2.x
 */
BOOST_AUTO_TEST_CASE(indexes) {
  {
    test_utils::execute_query(session, "CREATE KEYSPACE indexes WITH replication = "
                                       "{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");

    test_utils::execute_query(session, "CREATE TABLE indexes.table1 (key1 text, value1 int, value2 map<text, text>,  PRIMARY KEY(key1))");

    refresh_schema_meta();
    const CassTableMeta* table_meta = schema_get_table("indexes", "table1");

    BOOST_CHECK(cass_table_meta_index_count(table_meta) == 0);
    BOOST_CHECK(cass_table_meta_index_by_name(table_meta, "invalid") == NULL);
    BOOST_CHECK(cass_table_meta_index(table_meta, 0) == NULL);
  }

  // Index
  {
    test_utils::execute_query(session, "CREATE INDEX index1 ON indexes.table1 (value1)");

    refresh_schema_meta();
    const CassTableMeta* table_meta = schema_get_table("indexes", "table1");

    BOOST_REQUIRE(cass_table_meta_index_count(table_meta) == 1);
    std::map<std::string, std::string> index_options;
    if (version >= "3.0.0") {
      index_options["target"] = "value1";
    }
    verify_index(cass_table_meta_index_by_name(table_meta, "index1"),
                 "index1", CASS_INDEX_TYPE_COMPOSITES, "value1", index_options);
    verify_index(cass_table_meta_index(table_meta, 0),
                 "index1", CASS_INDEX_TYPE_COMPOSITES, "value1", index_options);
  }

  // Index on map keys (C* >= 2.1)
  {
    if (version >= "2.1.0") {
      test_utils::execute_query(session, "CREATE INDEX index2 ON indexes.table1 (KEYS(value2))");

      refresh_schema_meta();
      const CassTableMeta* table_meta = schema_get_table("indexes", "table1");

      BOOST_REQUIRE(cass_table_meta_index_count(table_meta) == 2);

      std::map<std::string, std::string> index_options;
      if (version >= "3.0.0") {
        index_options["target"] = "keys(value2)";
      } else {
        index_options["index_keys"] = "";
      }
      verify_index(cass_table_meta_index_by_name(table_meta, "index2"),
        "index2", CASS_INDEX_TYPE_COMPOSITES, "keys(value2)", index_options);
      verify_index(cass_table_meta_index(table_meta, 1),
        "index2", CASS_INDEX_TYPE_COMPOSITES, "keys(value2)", index_options);
    }
  }

  // Iterator
  {
    const CassTableMeta* table_meta = schema_get_table("indexes", "table1");

    test_utils::CassIteratorPtr iterator(cass_iterator_indexes_from_table_meta(table_meta));
    while (cass_iterator_next(iterator.get())) {
      const CassIndexMeta* index_meta = cass_iterator_get_index_meta(iterator.get());
      BOOST_REQUIRE(index_meta != NULL);

      CassString name;
      cass_index_meta_name(index_meta, &name.data, &name.length);
      BOOST_CHECK(name == CassString("index1") || name == CassString("index2"));
    }
  }
}

/**
 * Test materialized views.
 *
 * Verifies that materialized view metadata is correctly updated and returned.
 *
 * @since 2.3.0
 * @jira_ticket CPP-331, CPP-501, CPP-503, CPP-535
 * @test_category schema
 * @cassandra_version 3.0.x
 */
BOOST_AUTO_TEST_CASE(materialized_views) {
  {
    if (version < "3.0.0") return;

    test_utils::execute_query(session, "CREATE KEYSPACE materialized_views WITH replication = "
                                       "{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");

    test_utils::execute_query(session, "CREATE TABLE materialized_views.table1 (key1 text, value1 int, PRIMARY KEY(key1))");
    test_utils::execute_query(session, "CREATE TABLE materialized_views.table2 (key1 text, key2 int, value1 int, PRIMARY KEY(key1, key2))");

    refresh_schema_meta();

    verify_materialized_view_count("materialized_views", 0);

    const CassTableMeta* table_meta = schema_get_table("materialized_views", "table1");
    BOOST_CHECK(cass_table_meta_materialized_view_count(table_meta) == 0);
    BOOST_CHECK(cass_table_meta_materialized_view_by_name(table_meta, "invalid") == NULL);
    BOOST_CHECK(cass_table_meta_materialized_view(table_meta, 0) == NULL);
  }

  // Simple materialized view
  {
    test_utils::execute_query(session, "CREATE MATERIALIZED VIEW materialized_views.view1 AS "
                                       "SELECT key1 FROM materialized_views.table1 WHERE value1 IS NOT NULL "
                                       "PRIMARY KEY(value1, key1)");
    refresh_schema_meta();

    verify_materialized_view_count("materialized_views", 1);

    const CassTableMeta* table_meta = schema_get_table("materialized_views", "table1");
    BOOST_CHECK(cass_table_meta_materialized_view_count(table_meta) == 1);

    verify_materialized_view(cass_table_meta_materialized_view_by_name(table_meta, "view1"),
                             "view1", "table1",
                             "value1,key1", "value1", "key1");
  }

  // Materialized view with composite partition key
  {
    test_utils::execute_query(session, "CREATE MATERIALIZED VIEW materialized_views.view2 AS "
                                       "SELECT key1 FROM materialized_views.table2 WHERE key2 IS NOT NULL AND value1 IS NOT NULL "
                                       "PRIMARY KEY((value1, key2), key1)");

    refresh_schema_meta();

    verify_materialized_view_count("materialized_views", 2);

    const CassTableMeta* table_meta = schema_get_table("materialized_views", "table2");
    BOOST_CHECK(cass_table_meta_materialized_view_count(table_meta) == 1);

    verify_materialized_view(cass_table_meta_materialized_view_by_name(table_meta, "view2"),
                             "view2", "table2",
                             "value1,key2,key1", "value1,key2", "key1");
  }

  // Materialized view with composite clustering key
  {
    test_utils::execute_query(session, "CREATE MATERIALIZED VIEW materialized_views.view3 AS "
                                       "SELECT key1 FROM materialized_views.table2 WHERE key2 IS NOT NULL AND value1 IS NOT NULL "
                                       "PRIMARY KEY(value1, key2, key1) "
                                       "WITH CLUSTERING ORDER BY (key2 DESC)");

    refresh_schema_meta();

    verify_materialized_view_count("materialized_views", 3);

    const CassTableMeta* table_meta = schema_get_table("materialized_views", "table2");
    BOOST_CHECK(cass_table_meta_materialized_view_count(table_meta) == 2);

    verify_materialized_view(cass_table_meta_materialized_view_by_name(table_meta, "view3"),
                             "view3", "table2",
                             "value1,key2,key1", "value1", "key2,key1");
  }

  // Iterator
  {
    const CassTableMeta* table_meta = schema_get_table("materialized_views", "table2");

    test_utils::CassIteratorPtr iterator(cass_iterator_materialized_views_from_table_meta(table_meta));

    while (cass_iterator_next(iterator.get())) {
      const CassMaterializedViewMeta* view_meta = cass_iterator_get_materialized_view_meta(iterator.get());
      BOOST_REQUIRE(view_meta != NULL);

      CassString name;
      cass_materialized_view_meta_name(view_meta, &name.data, &name.length);

      if (name == CassString("view2")) {
        verify_materialized_view(view_meta, "view2", "table2",
                                 "value1,key2,key1", "value1,key2", "key1");
      } else if (name == CassString("view3")) {
        verify_materialized_view(view_meta, "view3", "table2",
                                 "value1,key2,key1", "value1", "key2,key1");
      } else {
        BOOST_CHECK(false);
      }
    }
  }

  // Drop views
  // (CPP-503: Schema metadata race condition when a view is dropped)
  {
    const CassTableMeta* table_meta = schema_get_table("materialized_views", "table2");
    schema_get_view("materialized_views", "view2"); // Ensures view not NULL
    test_utils::execute_query(session, "DROP MATERIALIZED VIEW materialized_views.view2");

    refresh_schema_meta();
    verify_materialized_view_count("materialized_views", 2);
    BOOST_REQUIRE(cass_keyspace_meta_materialized_view_by_name(schema_get_keyspace("materialized_views"), "view2") == NULL);

    const CassTableMeta* new_table_meta = schema_get_table("materialized_views", "table2");
    schema_get_view("materialized_views", "view1"); // Ensures view not NULL
    BOOST_CHECK(cass_table_meta_materialized_view_count(new_table_meta) == 1);
    BOOST_CHECK_NE(table_meta, new_table_meta);

    table_meta = schema_get_table("materialized_views", "table1");
    test_utils::execute_query(session, "DROP MATERIALIZED VIEW materialized_views.view1");

    refresh_schema_meta();
    verify_materialized_view_count("materialized_views", 1);
    BOOST_REQUIRE(cass_keyspace_meta_materialized_view_by_name(schema_get_keyspace("materialized_views"), "view1") == NULL);

    new_table_meta = schema_get_table("materialized_views", "table1");
    BOOST_CHECK(cass_table_meta_materialized_view_count(new_table_meta) == 0);
    BOOST_CHECK_NE(table_meta, new_table_meta);
  }

  // Alter view (CPP-501: Ensure schema metadata is not corrupted)
  {
    const CassMaterializedViewMeta* view = schema_get_view("materialized_views", "view3");
    test_utils::execute_query(session, "ALTER MATERIALIZED VIEW materialized_views.view3 "
                              "WITH comment = 'my view rocks'");
    refresh_schema_meta();
    verify_materialized_view_count("materialized_views", 1);
    const CassMaterializedViewMeta* new_view = schema_get_view("materialized_views", "view3");
    BOOST_CHECK_NE(view, new_view);
  }

  // Note: Cassandra doesn't allow for dropping tables with active views.
  // It's also difficult and unpredictable to get DROP TABLE/MATERIALIZE VIEW
  // events to reorder so that the DROP TABLE event happens before the
  // DROP MATERIALIZE VIEW event.
}

/**
 * Test materialized view clustering order.
 *
 * Verify that column clustering order is properly updated and returned.
 *
 * @since 2.3.0
 * @jira_ticket CPP-332
 * @test_category schema
 * @cassandra_version 3.0.x
 */
BOOST_AUTO_TEST_CASE(materialized_view_clustering_order) {
  if (version < "3.0.0") return;

  {
    test_utils::execute_query(session, "CREATE KEYSPACE materialized_view_clustering_order WITH replication = "
                                       "{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");

    test_utils::execute_query(session, "CREATE TABLE materialized_view_clustering_order.table1 (key1 text, value1 text, PRIMARY KEY(key1))");
    test_utils::execute_query(session, "CREATE TABLE materialized_view_clustering_order.table2 (key1 text, key2 text, value1 text, PRIMARY KEY(key1, key2))");

    refresh_schema_meta();
  }

  {
    test_utils::execute_query(session, "CREATE MATERIALIZED VIEW materialized_view_clustering_order.composite_key AS "
                                       "SELECT key1 FROM materialized_view_clustering_order.table1 WHERE value1 IS NOT NULL "
                                       "PRIMARY KEY(value1, key1)");
    refresh_schema_meta();

    const CassMaterializedViewMeta* view_meta = schema_get_view("materialized_view_clustering_order", "composite_key");

    BOOST_REQUIRE_EQUAL(cass_materialized_view_meta_clustering_key_count(view_meta), 1);
    BOOST_CHECK_EQUAL(cass_materialized_view_meta_clustering_key_order(view_meta, 0), CASS_CLUSTERING_ORDER_ASC);
  }

  {
    test_utils::execute_query(session, "CREATE MATERIALIZED VIEW materialized_view_clustering_order.reversed_composite_key AS "
                                       "SELECT key1 FROM materialized_view_clustering_order.table1 WHERE value1 IS NOT NULL "
                                       "PRIMARY KEY(value1, key1)"
                                       "WITH CLUSTERING ORDER BY (key1 DESC)");
    refresh_schema_meta();

    const CassMaterializedViewMeta* view_meta = schema_get_view("materialized_view_clustering_order", "reversed_composite_key");

    BOOST_REQUIRE_EQUAL(cass_materialized_view_meta_clustering_key_count(view_meta), 1);
    BOOST_CHECK_EQUAL(cass_materialized_view_meta_clustering_key_order(view_meta, 0), CASS_CLUSTERING_ORDER_DESC);
  }

  {
    test_utils::execute_query(session, "CREATE MATERIALIZED VIEW materialized_view_clustering_order.composite_clustering_key AS "
                                       "SELECT key1 FROM materialized_view_clustering_order.table2 WHERE key2 IS NOT NULL AND value1 IS NOT NULL "
                                       "PRIMARY KEY(value1, key2, key1)");
    refresh_schema_meta();

    const CassMaterializedViewMeta* view_meta = schema_get_view("materialized_view_clustering_order", "composite_clustering_key");

    BOOST_REQUIRE_EQUAL(cass_materialized_view_meta_clustering_key_count(view_meta), 2);
    BOOST_CHECK_EQUAL(cass_materialized_view_meta_clustering_key_order(view_meta, 0), CASS_CLUSTERING_ORDER_ASC);
    BOOST_CHECK_EQUAL(cass_materialized_view_meta_clustering_key_order(view_meta, 1), CASS_CLUSTERING_ORDER_ASC);
  }

  {
    test_utils::execute_query(session, "CREATE MATERIALIZED VIEW materialized_view_clustering_order.reversed_composite_clustering_key AS "
                                       "SELECT key1 FROM materialized_view_clustering_order.table2 WHERE key2 IS NOT NULL AND value1 IS NOT NULL "
                                       "PRIMARY KEY(value1, key2, key1) "
                                       "WITH CLUSTERING ORDER BY (key2 DESC, key1 DESC)");
    refresh_schema_meta();

    const CassMaterializedViewMeta* view_meta = schema_get_view("materialized_view_clustering_order", "reversed_composite_clustering_key");

    BOOST_REQUIRE_EQUAL(cass_materialized_view_meta_clustering_key_count(view_meta), 2);
    BOOST_CHECK_EQUAL(cass_materialized_view_meta_clustering_key_order(view_meta, 0), CASS_CLUSTERING_ORDER_DESC);
    BOOST_CHECK_EQUAL(cass_materialized_view_meta_clustering_key_order(view_meta, 1), CASS_CLUSTERING_ORDER_DESC);
  }

  {
    test_utils::execute_query(session, "CREATE MATERIALIZED VIEW materialized_view_clustering_order.mixed_composite_clustering_key AS "
                                       "SELECT key1 FROM materialized_view_clustering_order.table2 WHERE key2 IS NOT NULL AND value1 IS NOT NULL "
                                       "PRIMARY KEY(value1, key2, key1) "
                                       "WITH CLUSTERING ORDER BY (key2 DESC, key1 ASC)");
    refresh_schema_meta();

    const CassMaterializedViewMeta* view_meta = schema_get_view("materialized_view_clustering_order", "mixed_composite_clustering_key");

    BOOST_REQUIRE_EQUAL(cass_materialized_view_meta_clustering_key_count(view_meta), 2);
    BOOST_CHECK_EQUAL(cass_materialized_view_meta_clustering_key_order(view_meta, 0), CASS_CLUSTERING_ORDER_DESC);
    BOOST_CHECK_EQUAL(cass_materialized_view_meta_clustering_key_order(view_meta, 1), CASS_CLUSTERING_ORDER_ASC);
  }
}

/**
 * Test duplicate table name in mulitple keyspaces.
 *
 * Verifies the case where two tables have the same name and their keyspaces
 * are adjacent. There was an issue where columns and indexes from the second
 * table would be added to previous keyspace's table.
 *
 * @since 2.3.0
 * @jira_ticket CPP-348
 * @test_category schema
 * @cassandra_version 1.2.x
 */
BOOST_AUTO_TEST_CASE(duplicate_table_name) {
  test_utils::execute_query(session, "CREATE KEYSPACE test14 WITH replication = "
                                     "{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");

  test_utils::execute_query(session, "CREATE TABLE test14.table1 (key1 TEXT PRIMARY KEY, value1 INT)" );

  test_utils::execute_query(session, "CREATE INDEX index1 ON test14.table1 (value1)");

  test_utils::execute_query(session, "CREATE KEYSPACE test15 WITH replication = "
                                     "{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");

  test_utils::execute_query(session, "CREATE TABLE test15.table1 (key1 TEXT PRIMARY KEY, value1 INT)" );

  test_utils::execute_query(session, "CREATE INDEX index1 ON test15.table1 (value1)");

  close_session();
  create_session();

  refresh_schema_meta();

  {
    const CassTableMeta* table_meta = schema_get_table("test14", "table1");
    BOOST_CHECK(cass_table_meta_column_by_name(table_meta, "key1") != NULL);
    BOOST_CHECK(cass_table_meta_index_by_name(table_meta, "index1") != NULL);
  }

  {
    const CassTableMeta* table_meta = schema_get_table("test15", "table1");
    BOOST_CHECK(cass_table_meta_column_by_name(table_meta, "key1") != NULL);
    BOOST_CHECK(cass_table_meta_index_by_name(table_meta, "index1") != NULL);
  }
}

/**
 * Ensure integer type is returned as a varint.
 *
 * Verifies the case the Cassandra marshal type is IntegerType and maps to a
 * CASS_VALUE_TYPE_VARINT.
 *
 * @since 2.6.0
 * @jira_ticket CPP-419
 * @test_category schema
 * @cassandra_version 1.2.x
 */
BOOST_AUTO_TEST_CASE(integer_type_varint_mapping) {
  test_utils::execute_query(session, "CREATE KEYSPACE varint_type WITH replication = "
                                     "{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
  test_utils::execute_query(session, "CREATE TABLE varint_type.table1 (key1 TEXT PRIMARY KEY, value1 VARINT)" );
  refresh_schema_meta();

  {
    const CassColumnMeta* col_meta = schema_get_column("varint_type", "table1", "value1");
    const CassValueType value_type = cass_data_type_type(cass_column_meta_data_type(col_meta));
    BOOST_CHECK_EQUAL(value_type, CASS_VALUE_TYPE_VARINT);
  }
}

/**
 * Ensure custom types with single quotes are parsed properly.
 *
 * @since 2.6.0
 * @jira_ticket CPP-431
 * @test_category schema
 * @cassandra_version 2.1.x
 */
BOOST_AUTO_TEST_CASE(single_quote_custom_type) {
  if (version < "2.1.0") return;

  test_utils::execute_query(session, "CREATE KEYSPACE single_quote_custom_type WITH replication = "
                                     "{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
  test_utils::execute_query(session, "CREATE TABLE single_quote_custom_type.table1 (key1 TEXT PRIMARY KEY, value1 'org.apache.cassandra.db.marshal.LexicalUUIDType')");

  refresh_schema_meta();

  {
    const CassColumnMeta* col_meta = schema_get_column("single_quote_custom_type", "table1", "value1");
    const CassDataType* data_type = cass_column_meta_data_type(col_meta);
    BOOST_REQUIRE(data_type != NULL);
    const CassValueType value_type = cass_data_type_type(data_type);
    BOOST_CHECK_EQUAL(value_type, CASS_VALUE_TYPE_CUSTOM);
    CassString class_name;
    cass_data_type_class_name(data_type, &class_name.data, &class_name.length);
    BOOST_CHECK_EQUAL(class_name, "org.apache.cassandra.db.marshal.LexicalUUIDType");
  }
}

BOOST_AUTO_TEST_SUITE_END()
