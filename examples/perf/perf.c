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
#define NUM_ITERATIONS 1000

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

typedef struct Status_ {
  uv_mutex_t mutex;
  uv_cond_t cond;
  int count;
} Status;

Status status;

int status_init(Status* status, int initial_count) {
  int rc;
  rc = uv_mutex_init(&status->mutex);
  if (rc != 0) return rc;
  rc = uv_cond_init(&status->cond);
  if (rc != 0) return rc;
  status->count = initial_count;
  return rc;
}

void status_destroy(Status* status) {
  uv_mutex_destroy(&status->mutex);
  uv_cond_destroy(&status->cond);
}

void status_notify(Status* status) {
  uv_mutex_lock(&status->mutex);
  status->count--;
  uv_cond_signal(&status->cond);
  uv_mutex_unlock(&status->mutex);
}

int status_wait(Status* status, uint64_t timeout_secs) {
  int count;
  uv_mutex_lock(&status->mutex);
  uv_cond_timedwait(&status->cond, &status->mutex, timeout_secs * 1000 * 1000 * 1000);
  count = status->count;
  uv_mutex_unlock(&status->mutex);
  return count;
}

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CassCluster* create_cluster(const char* hosts) {
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, hosts);
  cass_cluster_set_credentials(cluster, "cassandra", "cassandra");
  cass_cluster_set_num_threads_io(cluster, NUM_IO_WORKER_THREADS);
  cass_cluster_set_queue_size_io(cluster, 10000);
  cass_cluster_set_pending_requests_low_water_mark(cluster, 5000);
  cass_cluster_set_pending_requests_high_water_mark(cluster, 10000);
  cass_cluster_set_core_connections_per_host(cluster, 1);
  cass_cluster_set_max_connections_per_host(cluster, 2);
  cass_cluster_set_max_requests_per_flush(cluster, 10000);
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

void insert_into_perf(CassSession* session, const char* query, const CassPrepared* prepared) {
  int i;
  CassFuture* futures[NUM_CONCURRENT_REQUESTS];

  CassCollection* collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, 2);
  cass_collection_append_string(collection, "jazz");
  cass_collection_append_string(collection, "2013");

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
    }
    cass_future_free(future);
  }

  cass_collection_free(collection);
}

void run_insert_queries(void* data) {
  int i;
  CassSession* session = (CassSession*)data;

  const CassPrepared* insert_prepared = NULL;
  const char* insert_query = "INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?);";

#if USE_PREPARED
  if (prepare_query(session, insert_query, &insert_prepared) == CASS_OK) {
#endif
    for (i = 0; i < NUM_ITERATIONS; ++i) {
      insert_into_perf(session, insert_query, insert_prepared);
    }
#if USE_PREPARED
    cass_prepared_free(insert_prepared);
  }
#endif

  status_notify(&status);
}

void select_from_perf(CassSession* session, const char* query, const CassPrepared* prepared) {
  int i;
  CassFuture* futures[NUM_CONCURRENT_REQUESTS];

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
    }
    cass_future_free(future);
  }
}

void run_select_queries(void* data) {
  int i;
  CassSession* session = (CassSession*)data;
  const CassPrepared* select_prepared = NULL;
  const char* select_query = "SELECT * FROM songs WHERE id = a98d21b2-1900-11e4-b97b-e5e358e71e0d";

#if USE_PREPARED
  if (prepare_query(session, select_query, &select_prepared) == CASS_OK) {
#endif
    for (i = 0; i < NUM_ITERATIONS; ++i) {
      select_from_perf(session, select_query, select_prepared);
    }
#if USE_PREPARED
    cass_prepared_free(select_prepared);
  }
#endif

  status_notify(&status);
}

int main(int argc, char* argv[]) {
  int i;
  CassMetrics metrics;
  uv_thread_t threads[NUM_THREADS];
  CassCluster* cluster = NULL;
  CassSession* session = NULL;
  CassFuture* close_future = NULL;
  char* hosts = "127.0.0.1";
  if (argc > 1) {
    hosts = argv[1];
  }

  status_init(&status, NUM_THREADS);

  cass_log_set_level(CASS_LOG_INFO);

  cluster = create_cluster(hosts);
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
#if DO_SELECTS
    uv_thread_create(&threads[i], run_select_queries, (void*)session);
#else
    uv_thread_create(&threads[i], run_insert_queries, (void*)session);
#endif
  }

  while (status_wait(&status, 5) > 0) {
    cass_session_get_metrics(session, &metrics);
    printf("rate stats (requests/second): mean %f 1m %f 5m %f 10m %f\n",
           metrics.requests.mean_rate,
           metrics.requests.one_minute_rate,
           metrics.requests.five_minute_rate,
           metrics.requests.fifteen_minute_rate);
  }

  cass_session_get_metrics(session, &metrics);
  printf("final stats (microseconds): min %llu max %llu median %llu 75th %llu 95th %llu 98th %llu 99th %llu 99.9th %llu\n",
         (unsigned long long int)metrics.requests.min, (unsigned long long int)metrics.requests.max,
         (unsigned long long int)metrics.requests.median, (unsigned long long int)metrics.requests.percentile_75th,
         (unsigned long long int)metrics.requests.percentile_95th, (unsigned long long int)metrics.requests.percentile_98th,
         (unsigned long long int)metrics.requests.percentile_99th, (unsigned long long int)metrics.requests.percentile_999th);

  for (i = 0; i < NUM_THREADS; ++i) {
    uv_thread_join(&threads[i]);
  }

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);
  cass_cluster_free(cluster);
  cass_uuid_gen_free(uuid_gen);

  status_destroy(&status);

  return 0;
}
