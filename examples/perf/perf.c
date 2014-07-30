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

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include <uv.h>

#include "cassandra.h"

/*
 *  Don't use this example. It's just used as a scratch example for debugging and
 * roughly analyzing performance.
 */

#define NUM_THREADS 1
#define NUM_CONCURRENT_REQUESTS 10000

typedef struct ThreadStats_ {
  long total_average_count;
  double total_averages;
} ThreadStats;

void print_error(CassFuture* future) {
  CassString message = cass_future_error_message(future);
  fprintf(stderr, "Error: %.*s\n", (int)message.length, message.data);
}

CassCluster* create_cluster() {
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, "127.0.0.1");
  cass_cluster_set_credentials(cluster, "cassandra", "cassandra");
  cass_cluster_set_log_level(cluster, CASS_LOG_INFO);
  cass_cluster_set_queue_size_io(cluster, 8*16384);
  cass_cluster_set_num_threads_io(cluster, 1);
  cass_cluster_set_max_pending_requests(cluster, 100000);
  cass_cluster_set_core_connections_per_host(cluster, 2);
  cass_cluster_set_max_connections_per_host(cluster, 8);
  return cluster;
}

CassError connect_session(CassCluster* cluster, CassSession** output) {
  CassError rc = 0;
  CassFuture* future = cass_cluster_connect_keyspace(cluster, "examples");

  *output = NULL;

  cass_future_wait(future);
  rc = cass_future_error_code(future);
  if(rc != CASS_OK) {
    print_error(future);
  } else {
    *output = cass_future_get_session(future);
  }
  cass_future_free(future);

  return rc;
}

CassError execute_query(CassSession* session, const char* query) {
  CassError rc = 0;
  CassFuture* future = NULL;
  CassStatement* statement = cass_statement_new(cass_string_init(query), 0);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if(rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError prepare_query(CassSession* session, CassString query, const CassPrepared** prepared) {
  CassError rc = 0;
  CassFuture* future = NULL;

  future = cass_session_prepare(session, query);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if(rc != CASS_OK) {
    print_error(future);
  } else {
    *prepared = cass_future_get_prepared(future);
  }

  cass_future_free(future);

  return rc;
}

void select_from_perf(CassSession* session, CassString query, const CassPrepared* prepared,
                      ThreadStats* thread_stats) {
  size_t i;
  uint64_t start, elapsed;
  size_t num_requests = 0;
  CassFuture* futures[NUM_CONCURRENT_REQUESTS];

  unsigned long thread_id = uv_thread_self();

  start = uv_hrtime();

  for(i = 0; i < NUM_CONCURRENT_REQUESTS; ++i) {
    CassStatement* statement;

    if (prepared != NULL) {
      statement = cass_prepared_bind(prepared);
    } else {
      statement = cass_statement_new(query, 0);
    }

    futures[i] = cass_session_execute(session, statement);

    cass_statement_free(statement);
  }

  for(i = 0; i < NUM_CONCURRENT_REQUESTS; ++i) {
    CassFuture* future = futures[i];
    CassError rc = cass_future_error_code(future);
    if(rc != CASS_OK) {
      print_error(future);
    } else {
      const CassResult* result = cass_future_get_result(future);
      assert(cass_result_column_count(result) == 6);
      cass_result_free(result);
      num_requests++;
    }
    cass_future_free(future);
  }

  elapsed = uv_hrtime() - start;

  thread_stats->total_averages += num_requests /  ((double)elapsed / 1000000000.0);
  thread_stats->total_average_count++;

  printf("%ld: average %lf selects/sec (%lu)\n", thread_id, thread_stats->total_averages / thread_stats->total_average_count, num_requests);
}

void run_select_queries(void* data) {
  int i;
  CassSession* session = (CassSession*)data;
  const CassPrepared* select_prepared = NULL;
  CassString select_query = cass_string_init("SELECT * FROM songs WHERE id = a98d21b2-1900-11e4-b97b-e5e358e71e0d");

  ThreadStats thread_stats;
  thread_stats.total_average_count = 0;
  thread_stats.total_averages = 0.0;

#define USE_PREPARED

#ifdef USE_PREPARED
  if (prepare_query(session, select_query, &select_prepared) == CASS_OK) {
#endif
    for (i = 0; i < 1000; ++i) {
      select_from_perf(session, select_query, select_prepared, &thread_stats);
    }
#ifdef USE_PREPARED
    cass_prepared_free(select_prepared);
  }
#endif
}

int main() {
  int i;
  uv_thread_t threads[NUM_THREADS];
  CassError rc = 0;
  CassCluster* cluster = create_cluster();
  CassSession* session = NULL;
  CassFuture* close_future = NULL;


  rc = connect_session(cluster, &session);
  if(rc != CASS_OK) {
    return -1;
  }

  execute_query(session,
                "INSERT INTO songs (id, title, album, artist, tags) VALUES "
                "(a98d21b2-1900-11e4-b97b-e5e358e71e0d, "
                "'La Petite Tonkinoise', 'Bye Bye Blackbird', 'Joséphine Baker', { 'jazz', '2013' });");


  for (i = 0; i < NUM_THREADS; ++i) {
    uv_thread_create(&threads[i], run_select_queries, (void*)session);
  }

  for (i = 0; i < NUM_THREADS; ++i) {
    uv_thread_join(&threads[i]);
  }

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);
  cass_cluster_free(cluster);

  return 0;
}
