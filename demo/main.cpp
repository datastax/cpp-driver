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

int
main() {
  CqlCluster* cluster = cql_cluster_new();
  CqlSession* session = NULL;
  CqlError*   err     = NULL;

  err = cql_session_new(cluster, &session);
  if (err) {
    // TODO(mstump)
    printf("Error do something\n");
  }



  CqlSessionFuture* shutdown_future = NULL;
  cql_session_shutdown(session, &shutdown_future);
  cql_session_future_wait(shutdown_future);
  cql_session_future_free(shutdown_future);
  cql_session_free(session);
  cql_cluster_free(cluster);
}
