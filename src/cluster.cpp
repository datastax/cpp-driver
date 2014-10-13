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

#include "cluster.hpp"

#include "common.hpp"
#include "dc_aware_policy.hpp"
#include "round_robin_policy.hpp"
#include "types.hpp"


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
  // 0 is okay, it means use a thread per core
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

CassError cass_cluster_set_queue_size_event(CassCluster* cluster,
                                            unsigned queue_size) {
  if (queue_size == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_queue_size_event(queue_size);
  return CASS_OK;
}

CassError cass_cluster_set_queue_size_log(CassCluster* cluster,
                                          unsigned queue_size) {
  if (queue_size == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_queue_size_log(queue_size);
  return CASS_OK;
}

CassError cass_cluster_set_contact_points(CassCluster* cluster,
                                          const char* contact_points) {
  size_t length = strlen(contact_points);
  if (length == 0) {
    cluster->config().contact_points().clear();
  } else {
    std::istringstream stream(
        std::string(contact_points, length));
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

CassError cass_cluster_set_reconnect_wait_time(CassCluster* cluster,
                                               unsigned wait_time) {
  if (wait_time == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_reconnect_wait_time(wait_time);
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

CassError cass_cluster_set_max_simultaneous_requests_threshold(CassCluster* cluster,
                                                               unsigned num_requests) {
  if (num_requests == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_max_simultaneous_requests_threshold(num_requests);
  return CASS_OK;
}

CassError cass_cluster_set_max_requests_per_flush(CassCluster* cluster,
                                                      unsigned num_requests) {
  if (num_requests == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_max_requests_per_flush(num_requests);
  return CASS_OK;
}

CassError cass_cluster_set_write_bytes_high_water_mark(CassCluster* cluster,
                                                        unsigned num_bytes) {
  if (num_bytes == 0 ||
      num_bytes < cluster->config().write_bytes_low_water_mark()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_write_bytes_high_water_mark(num_bytes);
  return CASS_OK;
}

CassError cass_cluster_set_write_bytes_low_water_mark(CassCluster* cluster,
                                                       unsigned num_bytes) {
  if (num_bytes == 0 ||
      num_bytes > cluster->config().write_bytes_high_water_mark()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_write_bytes_low_water_mark(num_bytes);
  return CASS_OK;
}

CassError cass_cluster_set_pending_requests_high_water_mark(CassCluster* cluster,
                                                            unsigned num_requests) {
  if (num_requests == 0 ||
      num_requests < cluster->config().pending_requests_low_water_mark()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_pending_requests_high_water_mark(num_requests);
  return CASS_OK;
}

CassError cass_cluster_set_pending_requests_low_water_mark(CassCluster* cluster,
                                                           unsigned num_requests) {
  if (num_requests == 0 ||
      num_requests > cluster->config().pending_requests_high_water_mark()) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_pending_requests_low_water_mark(num_requests);
  return CASS_OK;
}

CassError cass_cluster_set_connect_timeout(CassCluster* cluster,
                                              unsigned timeout) {
  cluster->config().set_connect_timeout(timeout);
  return CASS_OK;
}

CassError cass_cluster_set_request_timeout(CassCluster* cluster,
                                        unsigned timeout) {
  cluster->config().set_request_timeout(timeout);
  return CASS_OK;
}

CassError cass_cluster_set_log_level(CassCluster* cluster,
                                     CassLogLevel level) {
  cluster->config().set_log_level(level);
  return CASS_OK;
}

CassError cass_cluster_set_log_callback(CassCluster* cluster,
                                        CassLogCallback callback,
                                        void* data) {
  cluster->config().set_log_callback(callback, data);
  return CASS_OK;
}

CassError cass_cluster_set_credentials(CassCluster* cluster,
                                       const char* username,
                                       const char* password) {
  cluster->config().set_credentials(username, password);
  return CASS_OK;
}

CassError cass_cluster_set_load_balance_round_robin(CassCluster* cluster) {
  cluster->config().set_load_balancing_policy(new cass::RoundRobinPolicy());
  return CASS_OK;
}

CassError cass_cluster_set_load_balance_dc_aware(CassCluster* cluster,
                                                 const char* local_dc) {
  cluster->config().set_load_balancing_policy(new cass::DCAwarePolicy(local_dc));
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
  connect_future->inc_ref();

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
