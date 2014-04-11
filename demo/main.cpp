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

// void
// ready(
//     cql::ClientConnection* connection,
//     cql::Error*            err) {

//   if (err) {
//     std::cout << err->message << std::endl;
//   } else {
//     std::cout << "ready" << std::endl;
//     connection->set_keyspace("system");
//   }
// }

// void
// keyspace_set(
//     cql::ClientConnection* connection,
//     const char*            keyspace,
//     size_t                 size) {
//   (void) connection;
//   (void) size;
//   std::cout << "keyspace_set: " << keyspace << std::endl;
// }

// void
// error(
//     cql::ClientConnection* connection,
//     cql::Error*            err) {
//   (void) connection;
//   std::cout << "error: " << err->message << std::endl;
// }

void
print_log(
    int         level,
    const char* message,
    size_t      size) {
  (void) level;
  (void) size;
  std::cout << "log: " << message << std::endl;
}

int
main() {
  // cql::Cluster cluster;
  // cluster.log_callback(print_log);
  // cql::Session* session = cluster.connect();
  // session->shutdown();
}
