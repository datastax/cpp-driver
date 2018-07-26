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

#ifndef __CASS_CONFIG_HPP_INCLUDED__
#define __CASS_CONFIG_HPP_INCLUDED__

#include "auth.hpp"
#include "cassandra.h"
#include "constants.hpp"
#include "execution_profile.hpp"
#include "ssl.hpp"
#include "timestamp_generator.hpp"
#include "speculative_execution.hpp"
#include "string.hpp"

#include <climits>

namespace cass {

void stderr_log_callback(const CassLogMessage* message, void* data);

class Config {
public:
  Config()
      : port_(CASS_DEFAULT_PORT)
      , protocol_version_(CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION)
      , use_beta_protocol_version_(CASS_DEFAULT_USE_BETA_PROTOCOL_VERSION)
      , thread_count_io_(CASS_DEFAULT_THREAD_COUNT_IO)
      , queue_size_io_(CASS_DEFAULT_QUEUE_SIZE_IO)
      , core_connections_per_host_(CASS_DEFAULT_NUM_CONNECTIONS_PER_HOST)
      , reconnect_wait_time_ms_(CASS_DEFAULT_RECONNECT_WAIT_TIME_MS)
      , connect_timeout_ms_(CASS_DEFAULT_CONNECT_TIMEOUT_MS)
      , resolve_timeout_ms_(CASS_DEFAULT_RESOLVE_TIMEOUT_MS)
      , max_schema_wait_time_ms_(CASS_DEFAULT_MAX_SCHEMA_WAIT_TIME_MS)
      , coalesce_delay_us_(CASS_DEFAULT_COALESCE_DELAY)
      , new_request_ratio_(CASS_DEFAULT_NEW_REQUEST_RATIO)
      , log_level_(CASS_DEFAULT_LOG_LEVEL)
      , log_callback_(stderr_log_callback)
      , log_data_(NULL)
      , auth_provider_(Memory::allocate<AuthProvider>())
      , tcp_nodelay_enable_(CASS_DEFAULT_TCP_NO_DELAY_ENABLED)
      , tcp_keepalive_enable_(CASS_DEFAULT_TCP_KEEPALIVE_ENABLED)
      , tcp_keepalive_delay_secs_(CASS_DEFAULT_TCP_KEEPALIVE_DELAY_SECS)
      , connection_idle_timeout_secs_(CASS_DEFAULT_IDLE_TIMEOUT_SECS)
      , connection_heartbeat_interval_secs_(CASS_DEFAULT_HEARTBEAT_INTERVAL_SECS)
      , timestamp_gen_(Memory::allocate<ServerSideTimestampGenerator>())
      , use_schema_(CASS_DEFAULT_USE_SCHEMA)
      , use_hostname_resolution_(CASS_DEFAULT_HOSTNAME_RESOLUTION_ENABLED)
      , use_randomized_contact_points_(CASS_DEFAULT_USE_RANDOMIZED_CONTACT_POINTS)
      , max_reusable_write_objects_(CASS_DEFAULT_MAX_REUSABLE_WRITE_OBJECTS)
      , prepare_on_all_hosts_(CASS_DEFAULT_PREPARE_ON_ALL_HOSTS)
      , prepare_on_up_or_add_host_(CASS_DEFAULT_PREPARE_ON_UP_OR_ADD_HOST)
      , no_compact_(CASS_DEFAULT_NO_COMPACT) {
    profiles_.set_empty_key(String());

    // Assign the defaults to the cluster profile
    default_profile_.set_consistency(CASS_DEFAULT_CONSISTENCY);
    default_profile_.set_serial_consistency(CASS_DEFAULT_SERIAL_CONSISTENCY);
    default_profile_.set_request_timeout(CASS_DEFAULT_REQUEST_TIMEOUT_MS);
    default_profile_.set_load_balancing_policy(Memory::allocate<DCAwarePolicy>());
    default_profile_.set_retry_policy(Memory::allocate<DefaultRetryPolicy>());
    default_profile_.set_speculative_execution_policy(Memory::allocate<NoSpeculativeExecutionPolicy>());
  }

  Config new_instance() const {
    Config config = *this;
    config.default_profile_.build_load_balancing_policy();
    config.init_profiles(); // Initializes the profiles from default (if needed)
    config.set_speculative_execution_policy(default_profile_.speculative_execution_policy()->new_instance());

    return config;
  }

  void set_consistency(CassConsistency consistency) {
    default_profile_.set_consistency(consistency);
  }

  void set_serial_consistency(CassConsistency serial_consistency) {
    default_profile_.set_serial_consistency(serial_consistency);
  }

  unsigned thread_count_io() const { return thread_count_io_; }

  void set_thread_count_io(unsigned num_threads) {
    thread_count_io_ = num_threads;
  }

  unsigned queue_size_io() const { return queue_size_io_; }

  void set_queue_size_io(unsigned queue_size) {
    queue_size_io_ = queue_size;
  }

  unsigned core_connections_per_host() const {
    return core_connections_per_host_;
  }

  void set_core_connections_per_host(unsigned num_connections) {
    core_connections_per_host_ = num_connections;
  }

  unsigned reconnect_wait_time_ms() const { return reconnect_wait_time_ms_; }

  void set_reconnect_wait_time(unsigned wait_time_ms) {
    reconnect_wait_time_ms_ = wait_time_ms;
  }

  unsigned connect_timeout_ms() const { return connect_timeout_ms_; }

  void set_connect_timeout(unsigned timeout_ms) {
    connect_timeout_ms_ = timeout_ms;
  }

  unsigned max_schema_wait_time_ms() const { return max_schema_wait_time_ms_; }

  void set_max_schema_wait_time_ms(unsigned time_ms) {
    max_schema_wait_time_ms_ = time_ms;
  }

  uint64_t coalesce_delay_us() const { return coalesce_delay_us_; }

  void set_coalesce_delay_us(uint64_t delay_us) {
    coalesce_delay_us_ = delay_us;
  }

  int new_request_ratio() const { return new_request_ratio_; }

  void set_new_request_ratio(int ratio) {
    new_request_ratio_ = ratio;
  }

  void set_request_timeout(unsigned timeout_ms) {
    default_profile_.set_request_timeout(timeout_ms);
  }

  unsigned resolve_timeout_ms() const { return resolve_timeout_ms_; }

  void set_resolve_timeout(unsigned timeout_ms) {
    resolve_timeout_ms_ = timeout_ms;
  }

  const ContactPointList& contact_points() const {
    return contact_points_;
  }

  ContactPointList& contact_points() {
    return contact_points_;
  }

  int port() const { return port_; }

  void set_port(int port) {
    port_ = port;
  }

  int protocol_version() const { return protocol_version_; }

  void set_protocol_version(int protocol_version) {
    protocol_version_ = protocol_version;
  }

  bool use_beta_protocol_version() const {
    return use_beta_protocol_version_;
  }

  void set_use_beta_protocol_version(bool enable) {
    use_beta_protocol_version_ = enable;
  }

  CassLogLevel log_level() const { return log_level_; }

  void set_log_level(CassLogLevel log_level) {
    log_level_ = log_level;
  }

  void* log_data() const { return log_data_; }

  CassLogCallback log_callback() const { return log_callback_; }

  void set_log_callback(CassLogCallback callback, void* data) {
    log_callback_ = callback;
    log_data_ = data;
  }

  const AuthProvider::Ptr& auth_provider() const { return auth_provider_; }

  void set_auth_provider(const AuthProvider::Ptr& auth_provider) {
    auth_provider_ = (!auth_provider ? AuthProvider::Ptr(Memory::allocate<AuthProvider>()) : auth_provider);
  }

  void set_credentials(const String& username, const String& password) {
    auth_provider_.reset(Memory::allocate<PlainTextAuthProvider>(username, password));
  }

  const LoadBalancingPolicy::Ptr& load_balancing_policy() const {
    return default_profile().load_balancing_policy();
  }

  LoadBalancingPolicy::Vec load_balancing_policies() const {
    LoadBalancingPolicy::Vec policies;
    for (ExecutionProfile::Map::const_iterator it = profiles_.begin(),
         end = profiles_.end(); it != end; ++it) {
      if (it->second.load_balancing_policy()) {
        policies.push_back(it->second.load_balancing_policy());
      }
    }
    return policies;
  }

  void set_load_balancing_policy(LoadBalancingPolicy* lbp) {
    default_profile_.set_load_balancing_policy(lbp);
  }

  void set_speculative_execution_policy(SpeculativeExecutionPolicy* sep) {
    default_profile_.set_speculative_execution_policy(sep);
  }

  const SslContext::Ptr& ssl_context() const { return ssl_context_; }

  void set_ssl_context(SslContext* ssl_context) {
    ssl_context_.reset(ssl_context);
  }

  bool token_aware_routing() const {
    return default_profile().token_aware_routing();
  }

  void set_token_aware_routing(bool is_token_aware) {
    default_profile_.set_token_aware_routing(is_token_aware);
  }

  void set_token_aware_routing_shuffle_replicas(bool shuffle_replicas) {
    default_profile_.set_token_aware_routing_shuffle_replicas(shuffle_replicas);
  }

  void set_latency_aware_routing(bool is_latency_aware) {
    default_profile_.set_latency_aware_routing(is_latency_aware);
  }

  void set_host_targeting(bool is_host_targeting) {
    default_profile_.set_host_targeting(is_host_targeting);
  }

  void set_latency_aware_routing_settings(const LatencyAwarePolicy::Settings& settings) {
    default_profile_.set_latency_aware_routing_settings(settings);
  }

  bool tcp_nodelay_enable() const { return tcp_nodelay_enable_; }

  void set_tcp_nodelay(bool enable) {
    tcp_nodelay_enable_ = enable;
  }

  bool tcp_keepalive_enable() const { return tcp_keepalive_enable_; }
  unsigned tcp_keepalive_delay_secs() const { return tcp_keepalive_delay_secs_; }

  void set_tcp_keepalive(bool enable, unsigned delay_secs) {
    tcp_keepalive_enable_ = enable;
    tcp_keepalive_delay_secs_ = delay_secs;
  }

  unsigned connection_idle_timeout_secs() const {
    return connection_idle_timeout_secs_;
  }

  void set_connection_idle_timeout_secs(unsigned timeout_secs) {
    connection_idle_timeout_secs_ = timeout_secs;
  }

  unsigned connection_heartbeat_interval_secs() const {
    return connection_heartbeat_interval_secs_;
  }

  void set_connection_heartbeat_interval_secs(unsigned interval_secs) {
    connection_heartbeat_interval_secs_ = interval_secs;
  }

  TimestampGenerator* timestamp_gen() const {
    return timestamp_gen_.get();
  }

  void set_timestamp_gen(TimestampGenerator* timestamp_gen) {
    if (timestamp_gen == NULL) return;
    timestamp_gen_.reset(timestamp_gen);
  }

  void set_retry_policy(RetryPolicy* retry_policy) {
    default_profile_.set_retry_policy(retry_policy);
  }

  bool use_schema() const { return use_schema_; }
  void set_use_schema(bool enable) {
    use_schema_ = enable;
  }

  bool use_hostname_resolution() const { return use_hostname_resolution_; }
  void set_use_hostname_resolution(bool enable) {
    use_hostname_resolution_ = enable;
  }

  bool use_randomized_contact_points() const { return use_randomized_contact_points_; }
  void set_use_randomized_contact_points(bool enable) {
    use_randomized_contact_points_ = enable;
  }

  unsigned max_reusable_write_objects() const { return max_reusable_write_objects_; }
  void set_max_reusable_write_objects(unsigned max_reusable_write_objects) { max_reusable_write_objects_ = max_reusable_write_objects; }

  const ExecutionProfile& default_profile() const {
    return default_profile_;
  }

  ExecutionProfile& default_profile() {
    return default_profile_;
  }

  const ExecutionProfile::Map& profiles() const {
    return profiles_;
  }

  void set_execution_profile(const String& name,
                             const ExecutionProfile* profile) {
    // Assign the host targeting profile based on the cluster profile
    // This is required as their is no exposed API to set this chained policy
    ExecutionProfile copy = *profile;
    copy.set_host_targeting(default_profile_.host_targeting());
    copy.build_load_balancing_policy();
    profiles_[name] = copy;
  }

  bool prepare_on_all_hosts() const { return prepare_on_all_hosts_; }

  void set_prepare_on_all_hosts(bool enabled) { prepare_on_all_hosts_ = enabled; }

  bool prepare_on_up_or_add_host() const { return prepare_on_up_or_add_host_; }

  void set_prepare_on_up_or_add_host(bool enabled) {
    prepare_on_up_or_add_host_ = enabled;
  }

  const Address& local_address() const { return local_address_; }

  void set_local_address(const Address& address) {
    local_address_ = address;
  }

  bool no_compact() const { return no_compact_; }

  void set_no_compact(bool enabled) {
    no_compact_ = enabled;
  }

private:
  void init_profiles();

private:
  int port_;
  int protocol_version_;
  bool use_beta_protocol_version_;
  ContactPointList contact_points_;
  unsigned thread_count_io_;
  unsigned queue_size_io_;
  unsigned core_connections_per_host_;
  unsigned reconnect_wait_time_ms_;
  unsigned connect_timeout_ms_;
  unsigned resolve_timeout_ms_;
  unsigned max_schema_wait_time_ms_;
  uint64_t coalesce_delay_us_;
  int new_request_ratio_;
  CassLogLevel log_level_;
  CassLogCallback log_callback_;
  void* log_data_;
  AuthProvider::Ptr auth_provider_;
  SslContext::Ptr ssl_context_;
  bool tcp_nodelay_enable_;
  bool tcp_keepalive_enable_;
  unsigned tcp_keepalive_delay_secs_;
  unsigned connection_idle_timeout_secs_;
  unsigned connection_heartbeat_interval_secs_;
  SharedRefPtr<TimestampGenerator> timestamp_gen_;
  bool use_schema_;
  bool use_hostname_resolution_;
  bool use_randomized_contact_points_;
  unsigned max_reusable_write_objects_;
  ExecutionProfile default_profile_;
  ExecutionProfile::Map profiles_;
  bool prepare_on_all_hosts_;
  bool prepare_on_up_or_add_host_;
  Address local_address_;
  bool no_compact_;
};

} // namespace cass

#endif
