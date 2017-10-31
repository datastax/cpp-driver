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

#include "integration.hpp"
#include "cassandra.h"

// Name of materialized view used in this test file.
#define VIEW_NAME "my_view"

class SchemaMetadataTest : public Integration {
public:
  SchemaMetadataTest() {
    is_schema_metadata_ = true;
  }

  void SetUp() {
    Integration::SetUp();
    populateSchema();
    schema_meta_ = session_.schema();
  }

  void populateSchema() {
    session_.execute(format_string("CREATE TABLE %s (key text, value bigint, "
                                     "PRIMARY KEY (key))", table_name_.c_str()));

    session_.execute(
      "CREATE FUNCTION avg_state(state tuple<int, bigint>, val int) "
        "CALLED ON NULL INPUT RETURNS tuple<int, bigint> "
        "LANGUAGE java AS "
        "  'if (val != null) { "
        "    state.setInt(0, state.getInt(0) + 1); "
        "    state.setLong(1, state.getLong(1) + val.intValue()); "
        "  } ;"
        "  return state;'"
        ";");
    session_.execute(
      "CREATE FUNCTION avg_final (state tuple<int, bigint>) "
        "CALLED ON NULL INPUT RETURNS double "
        "LANGUAGE java AS "
        "  'double r = 0; "
        "  if (state.getInt(0) == 0) return null; "
        "  r = state.getLong(1); "
        "  r /= state.getInt(0); "
        "  return Double.valueOf(r);' "
        ";");

    session_.execute(
      "CREATE AGGREGATE average(int) "
        "SFUNC avg_state STYPE tuple<int, bigint> FINALFUNC avg_final "
        "INITCOND(0, 0);");

    if (server_version_ >= "3.0.0") {
      session_.execute(format_string("CREATE MATERIALIZED VIEW %s "
                                       "AS SELECT value "
                                       "   FROM %s"
                                       "   WHERE value IS NOT NULL and key IS NOT NULL "
                                       "PRIMARY KEY(value, key)", VIEW_NAME, table_name_.c_str()));
    }
    session_.execute(
      "CREATE TYPE address (street text, city text)"
    );

    session_.execute(
      format_string("CREATE INDEX schema_meta_index ON %s (value)",
                    table_name_.c_str())
    );
  }

protected:
  Schema schema_meta_;
};

CASSANDRA_INTEGRATION_TEST_F(SchemaMetadataTest, Views) {
  Keyspace keyspace_meta = schema_meta_.keyspace(keyspace_name_);
  Table table_meta = keyspace_meta.table(table_name_);

  // Verify that the view exists in the keyspace and table.
  const CassMaterializedViewMeta *view_from_keyspace =
    cass_keyspace_meta_materialized_view_by_name(keyspace_meta.get(), VIEW_NAME);
  EXPECT_TRUE(view_from_keyspace != NULL);

  // Now from the table, and it should be the same CassMaterializedViewMeta object.
  const CassMaterializedViewMeta *view_from_table =
    cass_table_meta_materialized_view_by_name(table_meta.get(), VIEW_NAME);
  EXPECT_EQ(view_from_keyspace, view_from_table);

  // Verify that the view's back-pointer references this table object.
  EXPECT_EQ(table_meta.get(),
    cass_materialized_view_meta_base_table(view_from_keyspace));

  // Alter the view, which will cause a new event, and make sure the new
  // view object is available in our metadata (in a new schema snapshot).

  session_.execute(format_string("ALTER MATERIALIZED VIEW %s "
                                  "WITH comment = 'my view rocks'", VIEW_NAME));

  Schema new_schema = session_.schema();
  Keyspace new_keyspace_meta = new_schema.keyspace(keyspace_name_);

  const CassMaterializedViewMeta *updated_view =
    cass_keyspace_meta_materialized_view_by_name(new_keyspace_meta.get(), VIEW_NAME);
  EXPECT_NE(updated_view, view_from_keyspace);
}

CASSANDRA_INTEGRATION_TEST_F(SchemaMetadataTest, DropView) {
  Table table_meta = schema_meta_.keyspace(keyspace_name_).table(table_name_);

  // Verify that the table contains the view
  EXPECT_TRUE(cass_table_meta_materialized_view_by_name(table_meta.get(), VIEW_NAME) != NULL);

  session_.execute(format_string("DROP MATERIALIZED VIEW %s", VIEW_NAME));

  Schema new_schema = session_.schema();
  Table new_table_meta = new_schema.keyspace(keyspace_name_).table(table_name_);

  // Verify that the view has been removed from the table
  EXPECT_TRUE(cass_table_meta_materialized_view_by_name(new_table_meta.get(), VIEW_NAME) == NULL);

  // Verify that a new table metadata instance has been created
  EXPECT_NE(table_meta.get(), new_table_meta.get());
}
