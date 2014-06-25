/*
  Copyright 2014 DataStax

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

#include "balancing.hpp"
#include "cassandra.hpp"

extern "C" {

void cass_balancing_set_session_data(CassBalancing* balancing,
                                     void* session_data) {
  balancing->set_session_data(session_data);
}

void* cass_balancing_session_data(CassBalancing* balancing) {
  return balancing->session_data();
}

cass_size_t cass_balancing_hosts_count(CassBalancing* balancing) {
  return 0;
}

CassHost cass_balancing_host(CassBalancing* balancing,
                             cass_size_t index) {
  CassHost host;
  return host;
}

void cass_balancing_set_host_distance(CassBalancing* balancing,
                                      cass_size_t index,
                                      CassHostDistance distance) {
}

void cass_balancing_add_host_to_query(CassBalancing* balancing,
                                      CassInet host) {
}

} // extern "C"

namespace cass {

} // namespace cass

