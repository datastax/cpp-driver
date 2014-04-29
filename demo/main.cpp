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
#include <thread>

#include <iostream>
#include <string>
#include <vector>
#include <iomanip>

#include "cql.h"

void
print_log(
    int         level,
    const char* message,
    size_t      size) {
  (void) level;
  (void) size;
  std::cout << "LOG: " << message << std::endl;
}

void print_error(const char* message, int err) {
  printf("%s: %s (%d)\n", message, cql_error_desc(err), err);
}

int
main() {
  CqlCluster* cluster = cql_cluster_new();
  CqlSession* session = NULL;
  CqlFuture* session_future = NULL;
  CqlFuture* shutdown_future = NULL;

  int err;
  err = cql_session_connect(cluster, &session_future);
  if (err != 0) {
    print_error("Error creating session", err);
    goto cleanup;
  }

  cql_future_wait(session_future);
  cql_future_free(session_future);

  err = cql_session_shutdown(session, &shutdown_future);
  if(err != 0) {
    print_error("Error on shutdown", err);
    goto cleanup;
  }

  cql_future_wait(shutdown_future);
  cql_future_free(shutdown_future);

cleanup:
  if(session_future != NULL) {
    cql_session_free(session);
  }
  cql_cluster_free(cluster);
}
