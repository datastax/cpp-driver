/*
  This is free and unencumbered software released into the public domain.

  Anyone is free to copy, modify, publish, use, compile, sell, or
  distribute this software, either in source code form or as a compiled
  binary, for any purpose, commercial or non-commercial, and by any
  means.

  In jurisdictions that recognize copyright laws, the author or authors
  of this software dedicate any and all copyright interest in the
  software to the public domain. We make this dedication for the benefit
  of the public at large and to the detriment of our heirs and
  successors. We intend this dedication to be an overt act of
  relinquishment in perpetuity of all present and future rights to this
  software under copyright law.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
  OTHER DEALINGS IN THE SOFTWARE.

  For more information, please refer to <http://unlicense.org/>
*/

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include <uv.h>

#include "cassandra.h"

/*
 * Use this example with caution. It's just used as a scratch example for debugging and
 * roughly analyzing performance.
 */

#define NUM_THREADS 1
#define NUM_IO_WORKER_THREADS 4
#define NUM_CONCURRENT_REQUESTS 10000
#define NUM_SAMPLES 1000

#define DO_SELECTS 1
#define USE_PREPARED 1

const char* big_string = "0123456701234567012345670123456701234567012345670123456701234567"
                         "0123456701234567012345670123456701234567012345670123456701234567"
                         "0123456701234567012345670123456701234567012345670123456701234567"
                         "0123456701234567012345670123456701234567012345670123456701234567"
                         "0123456701234567012345670123456701234567012345670123456701234567"
                         "0123456701234567012345670123456701234567012345670123456701234567"
                         "0123456701234567012345670123456701234567012345670123456701234567";

CassUuidGen* uuid_gen;

typedef struct ThreadState_ {
  int id;
  uv_thread_t thread;
  CassSession* session;
  int count;
  double total_averages;
  double samples[NUM_SAMPLES];
} ThreadState;

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CassCluster* create_cluster() {
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, "127.0.0.1");
  cass_cluster_set_credentials(cluster, "cassandra", "cassandra");
  cass_cluster_set_num_threads_io(cluster, NUM_IO_WORKER_THREADS);
  cass_cluster_set_queue_size_io(cluster, 10000);
  cass_cluster_set_pending_requests_low_water_mark(cluster, 5000);
  cass_cluster_set_pending_requests_high_water_mark(cluster, 10000);
  cass_cluster_set_core_connections_per_host(cluster, 1);
  cass_cluster_set_max_connections_per_host(cluster, 2);
  return cluster;
}

CassError connect_session(CassSession* session, const CassCluster* cluster) {
  CassError rc = CASS_OK;
  CassFuture* future = cass_session_connect_keyspace(session, cluster, "examples");

  cass_future_wait(future);
  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }
  cass_future_free(future);

  return rc;
}

CassError execute_query(CassSession* session, const char* query) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  CassStatement* statement = cass_statement_new(query, 0);

  future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError prepare_query(CassSession* session, const char* query, const CassPrepared** prepared) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;

  future = cass_session_prepare(session, query);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    *prepared = cass_future_get_prepared(future);
  }

  cass_future_free(future);

  return rc;
}

int compare_dbl(const void* d1, const void* d2) {
  if (*((double*)d1) < *((double*)d2)) {
    return -1;
  } else if (*((double*)d1) > *((double*)d2)) {
    return 1;
  } else {
    return 0;
  }
}

void print_stats(ThreadState* state) {
  double throughput_avg = 0.0;
  double throughput_min = 0.0;
  double throughput_median = 0.0;
  double throughput_max = 0.0;
  int index_median = ceil(0.5 * NUM_SAMPLES);

  qsort(state->samples, NUM_SAMPLES, sizeof(double), compare_dbl);
  throughput_avg = state->total_averages / state->count;
  throughput_min = state->samples[0];
  throughput_median = state->samples[index_median];
  throughput_max = state->samples[NUM_SAMPLES - 1];

  printf("%d IO threads, %d requests/batch:\navg: %f\nmin: %f\nmedian: %f\nmax: %f\n",
         NUM_IO_WORKER_THREADS,
         NUM_CONCURRENT_REQUESTS,
         throughput_avg,
         throughput_min,
         throughput_median,
         throughput_max);
}

void insert_into_perf(ThreadState* state, const char* query, const CassPrepared* prepared) {
  int i;
  double elapsed, throughput;
  uint64_t start;
  int num_requests = 0;
  CassSession* session = state->session;
  CassFuture* futures[NUM_CONCURRENT_REQUESTS];

  CassCollection* collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, 2);
  cass_collection_append_string(collection, "jazz");
  cass_collection_append_string(collection, "2013");

  start = uv_hrtime();

  for (i = 0; i < NUM_CONCURRENT_REQUESTS; ++i) {
    CassUuid id;
    CassStatement* statement;

    if (prepared != NULL) {
      statement = cass_prepared_bind(prepared);
    } else {
      statement = cass_statement_new(query, 5);
    }

    cass_uuid_gen_time(uuid_gen, &id);
    cass_statement_bind_uuid(statement, 0, id);
    cass_statement_bind_string(statement, 1, big_string);
    cass_statement_bind_string(statement, 2, big_string);
    cass_statement_bind_string(statement, 3, big_string);
    cass_statement_bind_collection(statement, 4, collection);

    futures[i] = cass_session_execute(session, statement);

    cass_statement_free(statement);
  }

  for (i = 0; i < NUM_CONCURRENT_REQUESTS; ++i) {
    CassFuture* future = futures[i];
    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
      print_error(future);
    } else {
      num_requests++;
    }
    cass_future_free(future);
  }

  elapsed = (double)(uv_hrtime() - start) / 1000000000.0;
  throughput = (double)num_requests / elapsed;
  state->samples[state->count++] = throughput;
  state->total_averages += throughput;

  printf("%d: average %f inserts/sec (%d, %f)\n", state->id, state->total_averages / state->count, num_requests, elapsed);

  cass_collection_free(collection);
}

void run_insert_queries(void* data) {
  int i;
  ThreadState* state = (ThreadState*)data;

  const CassPrepared* insert_prepared = NULL;
  const char* insert_query = "INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?);";

#if USE_PREPARED
  if (prepare_query(state->session, insert_query, &insert_prepared) == CASS_OK) {
#endif
    for (i = 0; i < NUM_SAMPLES; ++i) {
      insert_into_perf(state, insert_query, insert_prepared);
    }
#if USE_PREPARED
    cass_prepared_free(insert_prepared);
  }
#endif

  print_stats(state);
}

void select_from_perf(ThreadState* state, const char* query, const CassPrepared* prepared) {
  int i;
  double elapsed, throughput;
  uint64_t start;
  int num_requests = 0;
  CassSession* session = state->session;
  CassFuture* futures[NUM_CONCURRENT_REQUESTS];

  start = uv_hrtime();

  for (i = 0; i < NUM_CONCURRENT_REQUESTS; ++i) {
    CassStatement* statement;

    if (prepared != NULL) {
      statement = cass_prepared_bind(prepared);
    } else {
      statement = cass_statement_new(query, 0);
    }

    futures[i] = cass_session_execute(session, statement);

    cass_statement_free(statement);
  }

  for (i = 0; i < NUM_CONCURRENT_REQUESTS; ++i) {
    CassFuture* future = futures[i];
    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
      print_error(future);
    } else {
      const CassResult* result = cass_future_get_result(future);
      assert(cass_result_column_count(result) == 6);
      cass_result_free(result);
      num_requests++;
    }
    cass_future_free(future);
  }

  elapsed = (double)(uv_hrtime() - start) / 1000000000.0;
  throughput = (double)num_requests / elapsed;
  state->samples[state->count++] = throughput;
  state->total_averages += throughput;

  printf("%d: average %f selects/sec (%d, %f)\n", state->id, state->total_averages / state->count, num_requests, elapsed);
}

void run_select_queries(void* data) {
  int i;
  ThreadState* state = (ThreadState*)data;
  const CassPrepared* select_prepared = NULL;
  const char* select_query = "SELECT * FROM songs WHERE id = a98d21b2-1900-11e4-b97b-e5e358e71e0d";

#if USE_PREPARED
  if (prepare_query(state->session, select_query, &select_prepared) == CASS_OK) {
#endif
    for (i = 0; i < NUM_SAMPLES; ++i) {
      select_from_perf(state, select_query, select_prepared);
    }
#if USE_PREPARED
    cass_prepared_free(select_prepared);
  }
#endif

  print_stats(state);
}

int main() {
  int i;
  ThreadState states[NUM_THREADS];
  CassCluster* cluster = NULL;
  CassSession* session = NULL;
  CassFuture* close_future = NULL;

  cass_log_set_level(CASS_LOG_INFO);

  cluster = create_cluster();
  uuid_gen = cass_uuid_gen_new();
  session = cass_session_new();

  if (connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }

  execute_query(session,
                "INSERT INTO songs (id, title, album, artist, tags) VALUES "
                "(a98d21b2-1900-11e4-b97b-e5e358e71e0d, "
                "'La Petite Tonkinoise', 'Bye Bye Blackbird', 'Joséphine Baker', { 'jazz', '2013' });");


  for (i = 0; i < NUM_THREADS; ++i) {
    states[i].id = i;
    states[i].session = session;
    states[i].count = 0;
    states[i].total_averages = 0.0;

#if DO_SELECTS
    uv_thread_create(&states[i].thread, run_select_queries, (void*)&states[i]);
#else
    uv_thread_create(&states[i].thread, run_insert_queries, (void*)&states[i]);
#endif
  }

  for (i = 0; i < NUM_THREADS; ++i) {
    uv_thread_join(&states[i].thread);
  }

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);
  cass_cluster_free(cluster);
  cass_uuid_gen_free(uuid_gen);

  return 0;
}
