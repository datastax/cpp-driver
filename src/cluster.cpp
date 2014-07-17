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

#include "common.hpp"
#include "types.hpp"
#include "cluster.hpp"

#include <sstream>

extern "C" {

CassCluster* cass_cluster_new() {
  return CassCluster::to(new cass::Cluster());
}

CassError cass_cluster_set_port(CassCluster* cluster,
                                int port) {
  if (port <= 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_port(port);
  return CASS_OK;
}

CassError cass_cluster_set_protocol_version(CassCluster* cluster,
                                            int protocol_version) {
  if (protocol_version != 1 && protocol_version != 2) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_protocol_version(protocol_version);
  return CASS_OK;
}

CassError cass_cluster_set_num_threads_io(CassCluster* cluster,
                                          unsigned num_threads) {
  if (num_threads == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_thread_count_io(num_threads);
  return CASS_OK;
}

CassError cass_cluster_set_queue_size_io(CassCluster* cluster,
                                         unsigned queue_size) {
  if (queue_size == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_queue_size_io(queue_size);
  return CASS_OK;
}

CassError cass_cluster_set_contact_points(CassCluster* cluster,
                                          CassString contact_points) {
  if (contact_points.length == 0) {
    cluster->config().contact_points().clear();
  } else {
    std::istringstream stream(
        std::string(contact_points.data, contact_points.length));
    while (!stream.eof()) {
      std::string contact_point;
      std::getline(stream, contact_point, ',');
      if (!cass::trim(contact_point).empty()) {
        cluster->config().contact_points().push_back(contact_point);
      }
    }
  }
  return CASS_OK;
}

CassError cass_cluster_set_core_connections_per_host(CassCluster* cluster,
                                                     unsigned num_connections) {
  if (num_connections == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_core_connections_per_host(num_connections);
  return CASS_OK;
}

CassError cass_cluster_set_max_connections_per_host(CassCluster* cluster,
                                                    unsigned num_connections) {
  if (num_connections == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_max_connections_per_host(num_connections);
  return CASS_OK;
}

CassError cass_cluster_set_max_simultaneous_creation(CassCluster* cluster,
                                                     unsigned num_connections) {
  if (num_connections == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_max_simultaneous_creation(num_connections);
  return CASS_OK;
}

CassError cass_cluster_set_max_pending_request(CassCluster* cluster,
                                               unsigned num_requests) {
  if (num_requests == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_max_pending_requests(num_requests);
  return CASS_OK;
}

CassError cass_cluster_set_max_simultaneous_requests_threshold(CassCluster* cluster,
                                                               unsigned num_requests) {
  if (num_requests == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_max_simultaneous_requests_threshold(num_requests);
  return CASS_OK;
}

CassError cass_cluster_set_connect_timeout(CassCluster* cluster,
                                              unsigned timeout) {
  cluster->config().set_connect_timeout(timeout);
  return CASS_OK;
}

CassError cass_cluster_set_write_timeout(CassCluster* cluster,
                                         unsigned timeout) {
  cluster->config().set_write_timeout(timeout);
  return CASS_OK;
}

CassError cass_cluster_set_read_timeout(CassCluster* cluster,
                                        unsigned timeout) {
  cluster->config().set_read_timeout(timeout);
  return CASS_OK;
}

CassError cass_cluster_set_log_level(CassCluster* cluster,
                                     CassLogLevel level) {
  cluster->config().set_log_level(level);
  return CASS_OK;
}

CassError cass_cluster_set_log_callback(CassCluster* cluster,
                                        void* data,
                                        CassLogCallback callback) {
  cluster->config().set_log_callback(data, callback);
  return CASS_OK;
}

CassFuture* cass_cluster_connect(CassCluster* cluster) {
  return cass_cluster_connect_keyspace(cluster, "");
}

CassFuture* cass_cluster_connect_keyspace(CassCluster* cluster,
                                          const char* keyspace) {
  cass::Session* session = new cass::Session(cluster->config());
  cass::SessionConnectFuture* connect_future =
      new cass::SessionConnectFuture(session);

  if (!session->connect_async(std::string(keyspace), connect_future)) {
    connect_future->set_error(CASS_ERROR_LIB_UNABLE_TO_INIT,
                              "Error initializing session");
  }

  return CassFuture::to(connect_future);
}

void cass_cluster_free(CassCluster* cluster) {
  delete cluster->from();
}

} // extern "C"

namespace cass {} // namespace cass
