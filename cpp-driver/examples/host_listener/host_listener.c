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

#include <cassandra.h>


#include <stdio.h>
#include <uv.h>

void on_signal(uv_signal_t* handle, int signum) {
  uv_signal_stop(handle);
}

void on_host_listener(CassHostListenerEvent event, CassInet inet, void* data);

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CassCluster* create_cluster(const char* hosts) {
  CassCluster* cluster = cass_cluster_new();
  cass_log_set_level(CASS_LOG_DISABLED);
  cass_cluster_set_contact_points(cluster, hosts);
  cass_cluster_set_host_listener_callback(cluster, on_host_listener, NULL);
  return cluster;
}

CassError connect_session(CassSession* session, const CassCluster* cluster) {
  CassError rc = CASS_OK;
  CassFuture* future = cass_session_connect(session, cluster);

  cass_future_wait(future);
  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }
  cass_future_free(future);

  return rc;
}

void on_host_listener(CassHostListenerEvent event, CassInet inet, void* data) {
  char address[CASS_INET_STRING_LENGTH];

  cass_inet_string(inet, address);
  if (event == CASS_HOST_LISTENER_EVENT_ADD) {
    printf("Host %s has been ADDED\n", address);
  } else if (event == CASS_HOST_LISTENER_EVENT_REMOVE) {
    printf("Host %s has been REMOVED\n", address);
  } else if (event == CASS_HOST_LISTENER_EVENT_UP) {
    printf("Host %s is UP\n", address);
  } else if (event == CASS_HOST_LISTENER_EVENT_DOWN) {
    printf("Host %s is DOWN\n", address);
  }
}

int main(int argc, char* argv[]) {
  CassCluster* cluster = NULL;
  CassSession* session = cass_session_new();
  char* hosts = "127.0.0.1";
  uv_loop_t loop;
  uv_signal_t signal;
  if (argc > 1) {
    hosts = argv[1];
  }

  cluster = create_cluster(hosts);

  if (connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }

  uv_loop_init(&loop);
  uv_signal_init(&loop, &signal);
  uv_signal_start(&signal, on_signal, SIGINT);
  fprintf(stderr, "Press CTRL+C to exit ...\n");
  uv_run(&loop, UV_RUN_DEFAULT);
  uv_loop_close(&loop);

  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
