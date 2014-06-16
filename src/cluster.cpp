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

#include "cassandra.hpp"
#include "cluster.hpp"

extern "C" {

CassCluster* cass_cluster_new() {
  return CassCluster::to(new cass::Cluster());
}

CassError cass_cluster_setopt(CassCluster* cluster, CassOption option,
                              const void* data, cass_size_t data_length) {
  return cluster->config().set_option(option, data, data_length);
}

CassError cass_cluster_getopt(const CassCluster* cluster, CassOption option,
                              void* data, cass_size_t* data_length) {
  return cluster->config().option(option, data, data_length);
}

CassFuture* cass_cluster_connect(CassCluster* cluster) {
  return cass_cluster_connect_keyspace(cluster, "");
}

CassFuture* cass_cluster_connect_keyspace(CassCluster* cluster,
                                          const char* keyspace) {
  cass::Session* session = new cass::Session(cluster->config());
  cass::SessionConnectFuture* connect_future =
      new cass::SessionConnectFuture(session);

  if(!session->connect_async(std::string(keyspace), connect_future)) {
    connect_future->set_error(CASS_ERROR_LIB_UNABLE_TO_INIT,
                              "Error initializing session");
    delete session;
  }

  return CassFuture::to(connect_future);
}

void cass_cluster_free(CassCluster* cluster) {
  delete cluster->from();
}

} // extern "C"

namespace cass {} // namespace cass
