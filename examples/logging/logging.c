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

#include "cassandra.h"

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CassCluster* create_cluster(const char* hosts) {
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, hosts);
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

void on_log(const CassLogMessage* message, void* data) {
  FILE* log_file = (FILE*)data;
  fprintf(log_file, "%u.%03u [%s] (%s:%d:%s): %s\n",
          (unsigned int)(message->time_ms / 1000),
          (unsigned int)(message->time_ms % 1000),
          cass_log_level_string(message->severity),
          message->file, message->line, message->function,
          message->message);
}

int main(int argc, char* argv[]) {
  CassCluster* cluster = NULL;
  CassSession* session = NULL;
  CassFuture* close_future = NULL;
  char* hosts = "127.0.0.1";
  if (argc > 1) {
    hosts = argv[1];
  }

  FILE* log_file = fopen("driver.log", "w+");
  if (log_file == NULL) {
    fprintf(stderr, "Unable to open log file\n");
  }

  /* Log configuration *MUST* be done before any other driver call */
  cass_log_set_level(CASS_LOG_INFO);
  cass_log_set_callback(on_log, (void*)log_file);

  cluster = create_cluster(hosts);
  session = cass_session_new();

  if (connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }

  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);

  cass_cluster_free(cluster);
  cass_session_free(session);

  /* Stop all sessions before cleaning up log resources */

  fclose(log_file);

  return 0;
}
