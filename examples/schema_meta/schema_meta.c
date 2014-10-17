/*
  Copyright (c) 2014 DataStax

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

#include <stdio.h>
#include <cassandra.h>

int main() {
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, "127.0.0.1");

  connect_future = cass_cluster_connect(cluster);

  if (cass_future_error_code(connect_future) == CASS_OK) {
    CassFuture* close_future = NULL;
    CassSession* session = cass_future_get_session(connect_future);

    {
      CassSchemaMetadata* schema = cass_session_get_schema_meta(session);
      CassKeyspaceMeta ks_meta;
      CassColumnFamilyMeta cf_meta;
      CassColumnMeta col_meta;
      printf("Known entities:\n============\n");
      if (cass_meta_get_keyspace2(schema, "system_traces", &ks_meta) == CASS_OK) {
        printf("keyspace: %.*s\n", (int)ks_meta.name.length, ks_meta.name.data);
        printf("  replication strategy: %.*s\n", (int)ks_meta.replication_strategy.length, ks_meta.replication_strategy.data);
        printf("      strategy options: %.*s\n", (int)ks_meta.strategy_options.length, ks_meta.strategy_options.data);
        printf("  durable writes: %s\n", ks_meta.durable_writes?"true":"false");
      }

      if (cass_meta_get_column_family2(schema, "system_traces", "sessions", &cf_meta) == CASS_OK) {
        printf("column family: %.*s\n", (int)cf_meta.name.length, cf_meta.name.data);
        printf("  comment: %.*s\n", (int)cf_meta.comment.length, cf_meta.comment.data);
      }

      if (cass_meta_get_column2(schema, "system_traces", "events", "event_id", &col_meta) == CASS_OK) {
        printf("column: %.*s\n", (int)col_meta.name.length, col_meta.name.data);
        printf("  validator: %.*s\n", (int)col_meta.validator.length, col_meta.validator.data);
      }
      cass_meta_free(schema);
    }

    {
      CassKeyspaceMeta ks_meta;
      CassColumnFamilyMeta cf_meta;
      CassColumnMeta col_meta;
      CassSchemaMetadata* schema_meta = cass_session_get_schema_meta(session);
      CassIterator* ks_itr = cass_iterator_keyspaces(schema_meta);
      CassIterator* cf_itr;
      CassIterator* col_itr;
      printf("\nIterators:\n============\n");
      while (cass_iterator_next(ks_itr)) {
        cass_iterator_get_keyspace_meta(ks_itr, &ks_meta);
        printf("keyspace: %.*s\n", (int)ks_meta.name.length, ks_meta.name.data);
        printf("  replication strategy: %.*s\n", (int)ks_meta.replication_strategy.length, ks_meta.replication_strategy.data);
        printf("      strategy options: %.*s\n", (int)ks_meta.strategy_options.length, ks_meta.strategy_options.data);
        printf("  durable writes: %s\n", ks_meta.durable_writes?"true":"false");
        printf("  column families:\n");
        cf_itr = cass_iterator_column_families_from_keyspace(ks_itr);
        while (cass_iterator_next(cf_itr)) {
          cass_iterator_get_column_family_meta(cf_itr, &cf_meta);
          printf("      column family: %.*s: \"%.*s\"\n",
                 (int)cf_meta.name.length, cf_meta.name.data,
                 (int)cf_meta.comment.length, cf_meta.comment.data);
          col_itr = cass_iterator_columns_from_column_family(cf_itr);
          while (cass_iterator_next(col_itr)) {
            cass_iterator_get_column_meta(col_itr, &col_meta);
            printf("        column: %.*s (%.*s)\n",
                   (int)col_meta.name.length, col_meta.name.data,
                   (int)col_meta.validator.length, col_meta.validator.data);
          }
          cass_iterator_free(col_itr);
        }
        cass_iterator_free(cf_itr);
      }
      cass_iterator_free(ks_itr);
      cass_meta_free(schema_meta);
    }

    /* Close the session */
    close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);
  } else {
      /* Handle error */
      CassString message = cass_future_error_message(connect_future);
      fprintf(stderr, "Unable to connect: '%.*s'\n", (int)message.length, 
                                                          message.data);
  }

  cass_future_free(connect_future);
  cass_cluster_free(cluster);

  return 0;
}
