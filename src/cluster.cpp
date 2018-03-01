/*
  Copyright (c) DataStax, Inc.

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

#include "constants.hpp"
#include "dc_aware_policy.hpp"
#include "external.hpp"
#include "logger.hpp"
#include "round_robin_policy.hpp"
#include "speculative_execution.hpp"
#include "utils.hpp"

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

void cass_cluster_set_ssl(CassCluster* cluster,
                          CassSsl* ssl) {
  cluster->config().set_ssl_context(ssl->from());
}

CassError cass_cluster_set_protocol_version(CassCluster* cluster,
                                            int protocol_version) {
  if (protocol_version < 1) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  if (cluster->config().use_beta_protocol_version()) {
    LOG_ERROR("The protocol version is already set to the newest beta version v%d "
              "and cannot be explicitly set.", CASS_NEWEST_BETA_PROTOCOL_VERSION);
    return CASS_ERROR_LIB_BAD_PARAMS;
  } else if (protocol_version > CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION) {
    LOG_ERROR("Protocol version v%d is higher than the highest supported "
              "protocol version v%d (consider using the newest beta protocol version).",
              protocol_version, CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION);
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_protocol_version(protocol_version);
  return CASS_OK;
}

CassError cass_cluster_set_use_beta_protocol_version(CassCluster* cluster,
                                                     cass_bool_t enable) {
  cluster->config().set_use_beta_protocol_version(enable == cass_true);
  cluster->config().set_protocol_version(enable ? CASS_NEWEST_BETA_PROTOCOL_VERSION
                                                : CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION);
  return CASS_OK;
}

CassError cass_cluster_set_consistency(CassCluster* cluster,
                                       CassConsistency consistency) {
  if (consistency == CASS_CONSISTENCY_UNKNOWN) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_consistency(consistency);
  return CASS_OK;
}

CassError cass_cluster_set_serial_consistency(CassCluster* cluster,
                                              CassConsistency consistency) {
  if (consistency == CASS_CONSISTENCY_UNKNOWN) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_serial_consistency(consistency);
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

CassError cass_cluster_set_queue_size_event(CassCluster* cluster,
                                            unsigned queue_size) {
  if (queue_size == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_queue_size_event(queue_size);
  return CASS_OK;
}

CassError cass_cluster_set_contact_points(CassCluster* cluster,
                                          const char* contact_points) {
  return cass_cluster_set_contact_points_n(cluster,
                                           contact_points,
                                           SAFE_STRLEN(contact_points));
}

CassError cass_cluster_set_contact_points_n(CassCluster* cluster,
                                            const char* contact_points,
                                            size_t contact_points_length) {
  if (contact_points_length == 0) {
    cluster->config().contact_points().clear();
  } else {
    cass::explode(std::string(contact_points, contact_points_length),
                  cluster->config().contact_points());
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

void cass_cluster_set_reconnect_wait_time(CassCluster* cluster,
                                          unsigned wait_time_ms) {
  cluster->config().set_reconnect_wait_time(wait_time_ms);
}

CassError cass_cluster_set_max_concurrent_creation(CassCluster* cluster,
                                                   unsigned num_connections) {
  if (num_connections == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_max_concurrent_creation(num_connections);
  return CASS_OK;
}

CassError cass_cluster_set_max_concurrent_requests_threshold(CassCluster* cluster,
                                                             unsigned num_requests) {
  if (num_requests == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_max_concurrent_requests_threshold(num_requests);
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
  // Deprecated
  return CASS_OK;
}

CassError cass_cluster_set_write_bytes_low_water_mark(CassCluster* cluster,
                                                      unsigned num_bytes) {
  // Deprecated
  return CASS_OK;
}

CassError cass_cluster_set_pending_requests_high_water_mark(CassCluster* cluster,
                                                            unsigned num_requests) {
  // Deprecated
  return CASS_OK;
}

CassError cass_cluster_set_pending_requests_low_water_mark(CassCluster* cluster,
                                                           unsigned num_requests) {
  // Deprecated
  return CASS_OK;
}

void cass_cluster_set_connect_timeout(CassCluster* cluster,
                                      unsigned timeout_ms) {
  cluster->config().set_connect_timeout(timeout_ms);
}

void cass_cluster_set_request_timeout(CassCluster* cluster,
                                      unsigned timeout_ms) {
  cluster->config().set_request_timeout(timeout_ms);
}

void cass_cluster_set_resolve_timeout(CassCluster* cluster,
                                      unsigned timeout_ms) {
  cluster->config().set_resolve_timeout(timeout_ms);
}

void cass_cluster_set_credentials(CassCluster* cluster,
                                  const char* username,
                                  const char* password) {
  return cass_cluster_set_credentials_n(cluster,
                                        username, SAFE_STRLEN(username),
                                        password, SAFE_STRLEN(password));
}

void cass_cluster_set_credentials_n(CassCluster* cluster,
                                    const char* username,
                                    size_t username_length,
                                    const char* password,
                                    size_t password_length) {
  cluster->config().set_credentials(std::string(username, username_length),
                                    std::string(password, password_length));
}

void cass_cluster_set_load_balance_round_robin(CassCluster* cluster) {
  cluster->config().set_load_balancing_policy(new cass::RoundRobinPolicy());
}

CassError cass_cluster_set_load_balance_dc_aware(CassCluster* cluster,
                                                 const char* local_dc,
                                                 unsigned used_hosts_per_remote_dc,
                                                 cass_bool_t allow_remote_dcs_for_local_cl) {
  if (local_dc == NULL) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  return cass_cluster_set_load_balance_dc_aware_n(cluster,
                                                  local_dc,
                                                  SAFE_STRLEN(local_dc),
                                                  used_hosts_per_remote_dc,
                                                  allow_remote_dcs_for_local_cl);
}

CassError cass_cluster_set_load_balance_dc_aware_n(CassCluster* cluster,
                                                   const char* local_dc,
                                                   size_t local_dc_length,
                                                   unsigned used_hosts_per_remote_dc,
                                                   cass_bool_t allow_remote_dcs_for_local_cl) {
  if (local_dc == NULL || local_dc_length == 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_load_balancing_policy(
        new cass::DCAwarePolicy(std::string(local_dc, local_dc_length),
                                used_hosts_per_remote_dc,
                                !allow_remote_dcs_for_local_cl));
  return CASS_OK;
}

void cass_cluster_set_token_aware_routing(CassCluster* cluster,
                                          cass_bool_t enabled) {
  cluster->config().set_token_aware_routing(enabled == cass_true);
}

void cass_cluster_set_latency_aware_routing(CassCluster* cluster,
                                            cass_bool_t enabled) {
  cluster->config().set_latency_aware_routing(enabled == cass_true);
}

void cass_cluster_set_latency_aware_routing_settings(CassCluster* cluster,
                                                     cass_double_t exclusion_threshold,
                                                     cass_uint64_t scale_ms,
                                                     cass_uint64_t retry_period_ms,
                                                     cass_uint64_t update_rate_ms,
                                                     cass_uint64_t min_measured) {
  cass::LatencyAwarePolicy::Settings settings;
  settings.exclusion_threshold = exclusion_threshold;
  settings.scale_ns = scale_ms * 1000 * 1000;
  settings.retry_period_ns = retry_period_ms * 1000 * 1000;
  settings.update_rate_ms = update_rate_ms;
  settings.min_measured = min_measured;
  cluster->config().set_latency_aware_routing_settings(settings);
}

void cass_cluster_set_whitelist_filtering(CassCluster* cluster,
                                          const char* hosts) {
  cass_cluster_set_whitelist_filtering_n(cluster,
                                         hosts,
                                         SAFE_STRLEN(hosts));
}

void cass_cluster_set_whitelist_filtering_n(CassCluster* cluster,
                                            const char* hosts,
                                            size_t hosts_length) {
  if (hosts_length == 0) {
    cluster->config().whitelist().clear();
  } else {
    cass::explode(std::string(hosts, hosts_length),
                  cluster->config().whitelist());
  }
}

void cass_cluster_set_blacklist_filtering(CassCluster* cluster,
                                          const char* hosts) {
  cass_cluster_set_blacklist_filtering_n(cluster,
                                         hosts,
                                         SAFE_STRLEN(hosts));
}

void cass_cluster_set_blacklist_filtering_n(CassCluster* cluster,
                                            const char* hosts,
                                            size_t hosts_length) {
  if (hosts_length == 0) {
    cluster->config().blacklist().clear();
  } else {
    cass::explode(std::string(hosts, hosts_length),
                  cluster->config().blacklist());
  }
}

void cass_cluster_set_whitelist_dc_filtering(CassCluster* cluster,
                                             const char* dcs) {
  cass_cluster_set_whitelist_dc_filtering_n(cluster,
                                            dcs,
                                            SAFE_STRLEN(dcs));
}

void cass_cluster_set_whitelist_dc_filtering_n(CassCluster* cluster,
                                               const char* dcs,
                                               size_t dcs_length) {
  if (dcs_length == 0) {
    cluster->config().whitelist_dc().clear();
  } else {
    cass::explode(std::string(dcs, dcs_length),
                  cluster->config().whitelist_dc());
  }
}

void cass_cluster_set_blacklist_dc_filtering(CassCluster* cluster,
                                             const char* dcs) {
  cass_cluster_set_blacklist_dc_filtering_n(cluster,
                                            dcs,
                                            SAFE_STRLEN(dcs));
}

void cass_cluster_set_blacklist_dc_filtering_n(CassCluster* cluster,
                                               const char* dcs,
                                               size_t dcs_length) {
  if (dcs_length == 0) {
    cluster->config().blacklist_dc().clear();
  } else {
    cass::explode(std::string(dcs, dcs_length),
                  cluster->config().blacklist_dc());
  }
}

void cass_cluster_set_tcp_nodelay(CassCluster* cluster,
                                  cass_bool_t enabled) {
  cluster->config().set_tcp_nodelay(enabled == cass_true);
}

void cass_cluster_set_tcp_keepalive(CassCluster* cluster,
                                    cass_bool_t enabled,
                                    unsigned delay_secs) {
  cluster->config().set_tcp_keepalive(enabled == cass_true, delay_secs);
}

CassError cass_cluster_set_authenticator_callbacks(CassCluster* cluster,
                                                   const CassAuthenticatorCallbacks* exchange_callbacks,
                                                   CassAuthenticatorDataCleanupCallback cleanup_callback,
                                                   void* data) {
  cluster->config().set_auth_provider(cass::AuthProvider::Ptr(
                                        new cass::ExternalAuthProvider(exchange_callbacks,
                                                                       cleanup_callback, data)));
  return CASS_OK;
}

void cass_cluster_set_connection_heartbeat_interval(CassCluster* cluster,
                                                    unsigned interval_secs) {
  cluster->config().set_connection_heartbeat_interval_secs(interval_secs);
}

void cass_cluster_set_connection_idle_timeout(CassCluster* cluster,
                                              unsigned timeout_secs) {
  cluster->config().set_connection_idle_timeout_secs(timeout_secs);
}

void cass_cluster_set_retry_policy(CassCluster* cluster,
                                   CassRetryPolicy* retry_policy) {
  cluster->config().set_retry_policy(retry_policy);
}

void cass_cluster_set_timestamp_gen(CassCluster* cluster,
                                    CassTimestampGen* timestamp_gen) {
  cluster->config().set_timestamp_gen(timestamp_gen);
}

void cass_cluster_set_use_schema(CassCluster* cluster,
                                 cass_bool_t enabled) {
  cluster->config().set_use_schema(enabled == cass_true);
}

CassError cass_cluster_set_use_hostname_resolution(CassCluster* cluster,
                                              cass_bool_t enabled) {
#if UV_VERSION_MAJOR >= 1
  cluster->config().set_use_hostname_resolution(enabled == cass_true);
  return CASS_OK;
#else
  return CASS_ERROR_LIB_NOT_IMPLEMENTED;
#endif
}

CassError cass_cluster_set_use_randomized_contact_points(CassCluster* cluster,
                                                         cass_bool_t enabled) {
  cluster->config().set_use_randomized_contact_points(enabled);
  return CASS_OK;
}

CassError cass_cluster_set_constant_speculative_execution_policy(CassCluster* cluster,
                                                                 cass_int64_t constant_delay_ms,
                                                                 int max_speculative_executions) {
  if (constant_delay_ms < 0 || max_speculative_executions < 0) {
    return CASS_ERROR_LIB_BAD_PARAMS;
  }
  cluster->config().set_speculative_execution_policy(
        new cass::ConstantSpeculativeExecutionPolicy(constant_delay_ms,
                                                     max_speculative_executions));
  return CASS_OK;
}

CassError cass_cluster_set_no_speculative_execution_policy(CassCluster* cluster) {
  cluster->config().set_speculative_execution_policy(
        new cass::NoSpeculativeExecutionPolicy());
  return CASS_OK;
}

CassError cass_cluster_set_prepare_on_all_hosts(CassCluster* cluster,
                                                cass_bool_t enabled){
  cluster->config().set_prepare_on_all_hosts(enabled == cass_true);
  return CASS_OK;
}

CassError cass_cluster_set_prepare_on_up_or_add_host(CassCluster* cluster,
                                                     cass_bool_t enabled) {
  cluster->config().set_prepare_on_up_or_add_host(enabled == cass_true);
  return CASS_OK;
}

CassError cass_cluster_set_no_compact(CassCluster* cluster,
                                 cass_bool_t enabled) {
  cluster->config().set_no_compact(enabled == cass_true);
  return CASS_OK;
}


void cass_cluster_free(CassCluster* cluster) {
  delete cluster->from();
}

} // extern "C"
