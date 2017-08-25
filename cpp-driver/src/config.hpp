/*
  Copyright (c) 2014-2017 DataStax

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

namespace cass {

void stderr_log_callback(const CassLogMessage* message, void* data);

class Config {
public:
  Config()
      : port_(9042)
      , protocol_version_(DSE_HIGHEST_SUPPORTED_PROTOCOL_VERSION)
      , use_beta_protocol_version_(false)
      , thread_count_io_(1)
      , queue_size_io_(8192)
      , queue_size_event_(8192)
      , queue_size_log_(8192)
      , core_connections_per_host_(1)
      , max_connections_per_host_(2)
      , reconnect_wait_time_ms_(2000)
      , max_concurrent_creation_(1)
      , max_requests_per_flush_(128)
      , max_concurrent_requests_threshold_(100)
      , connect_timeout_ms_(5000)
      , resolve_timeout_ms_(2000)
      , log_level_(CASS_LOG_WARN)
      , log_callback_(stderr_log_callback)
      , log_data_(NULL)
      , auth_provider_(Memory::allocate<AuthProvider>())
      , speculative_execution_policy_(Memory::allocate<NoSpeculativeExecutionPolicy>())
      , tcp_nodelay_enable_(true)
      , tcp_keepalive_enable_(false)
      , tcp_keepalive_delay_secs_(0)
      , connection_idle_timeout_secs_(60)
      , connection_heartbeat_interval_secs_(30)
      , timestamp_gen_(Memory::allocate<ServerSideTimestampGenerator>())
      , use_schema_(true)
      , use_hostname_resolution_(false)
      , use_randomized_contact_points_(true)
      , max_reusable_write_objects_(UINT_MAX)
      , prepare_on_all_hosts_(true)
      , prepare_on_up_or_add_host_(true) {
    profiles_.set_empty_key(String());

    // Assign the defaults to the cluster profile
    default_profile_.set_consistency(CASS_DEFAULT_CONSISTENCY);
    default_profile_.set_serial_consistency(CASS_DEFAULT_SERIAL_CONSISTENCY);
    default_profile_.set_request_timeout(CASS_DEFAULT_REQUEST_TIMEOUT_MS);
    default_profile_.set_load_balancing_policy(Memory::allocate<DCAwarePolicy>());
    default_profile_.set_retry_policy(Memory::allocate<DefaultRetryPolicy>());
  }

  Config new_instance() const {
    Config config = *this;
    config.init_profiles(); // Builds the LBPs and updates the profiles
    config.set_speculative_execution_policy(speculative_execution_policy_->new_instance());

    return config;
  }

  CassConsistency consistency() const { return default_profile_.consistency(); }

  void set_consistency(CassConsistency consistency) {
    default_profile_.set_consistency(consistency);
  }

  CassConsistency serial_consistency() const {
    return default_profile_.serial_consistency();
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

  unsigned queue_size_event() const { return queue_size_event_; }

  void set_queue_size_event(unsigned queue_size) {
    queue_size_event_ = queue_size;
  }

  unsigned queue_size_log() const { return queue_size_log_; }

  void set_queue_size_log(unsigned queue_size) {
    queue_size_log_ = queue_size;
  }

  unsigned core_connections_per_host() const {
    return core_connections_per_host_;
  }

  void set_core_connections_per_host(unsigned num_connections) {
    core_connections_per_host_ = num_connections;
  }

  unsigned max_connections_per_host() const { return max_connections_per_host_; }

  void set_max_connections_per_host(unsigned num_connections) {
    max_connections_per_host_ = num_connections;
  }

  unsigned max_concurrent_creation() const {
    return max_concurrent_creation_;
  }

  void set_max_concurrent_creation(unsigned num_connections) {
    max_concurrent_creation_ = num_connections;
  }

  unsigned reconnect_wait_time_ms() const { return reconnect_wait_time_ms_; }

  void set_reconnect_wait_time(unsigned wait_time_ms) {
    reconnect_wait_time_ms_ = wait_time_ms;
  }

  unsigned max_requests_per_flush() const { return max_requests_per_flush_; }

  void set_max_requests_per_flush(unsigned num_requests) {
    max_requests_per_flush_ = num_requests;
  }

  unsigned max_concurrent_requests_threshold() const {
    return max_concurrent_requests_threshold_;
  }

  void set_max_concurrent_requests_threshold(unsigned num_requests) {
    max_concurrent_requests_threshold_ = num_requests;
  }

  unsigned connect_timeout_ms() const { return connect_timeout_ms_; }

  void set_connect_timeout(unsigned timeout_ms) {
    connect_timeout_ms_ = timeout_ms;
  }

  unsigned request_timeout_ms() const {
    return default_profile_.request_timeout_ms();
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

  const LoadBalancingPolicy::Vec& load_balancing_policies() const {
    return load_balancing_policies_;
  }

  const LoadBalancingPolicy::Ptr& load_balancing_policy() const {
    return default_profile_.load_balancing_policy();
  }

  void set_load_balancing_policy(LoadBalancingPolicy* lbp) {
    default_profile_.set_load_balancing_policy(lbp);
  }

  const SpeculativeExecutionPolicy::Ptr& speculative_execution_policy() const {
    return speculative_execution_policy_;
  }

  void set_speculative_execution_policy(SpeculativeExecutionPolicy* sep) {
    if (sep == NULL) return;
    speculative_execution_policy_.reset(sep);
  }

  SslContext* ssl_context() const { return ssl_context_.get(); }

  void set_ssl_context(SslContext* ssl_context) {
    ssl_context_.reset(ssl_context);
  }

  bool token_aware_routing() const {
    return default_profile_.token_aware_routing();
  }

  void set_token_aware_routing(bool is_token_aware) {
    default_profile_.set_token_aware_routing(is_token_aware);
  }

  void set_token_aware_routing_shuffle_replicas(bool shuffle_replicas) {
    default_profile_.set_token_aware_routing_shuffle_replicas(shuffle_replicas);
  }

  bool latency_aware() const {
    return default_profile_.latency_aware();
  }

  void set_latency_aware_routing(bool is_latency_aware) {
    default_profile_.set_latency_aware_routing(is_latency_aware);
  }

  bool host_targeting() const {
    return default_profile_.host_targeting();
  }

  void set_host_targeting(bool is_host_targeting) {
    default_profile_.set_host_targeting(is_host_targeting);
  }

  void set_latency_aware_routing_settings(const LatencyAwarePolicy::Settings& settings) {
    default_profile_.set_latency_aware_routing_settings(settings);
  }

  ContactPointList& whitelist() {
    return default_profile_.whitelist();
  }

  ContactPointList& blacklist() {
    return default_profile_.blacklist();
  }

  DcList& whitelist_dc() {
    return default_profile_.whitelist_dc();
  }

  DcList& blacklist_dc() {
    return default_profile_.blacklist_dc();
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

  const RetryPolicy::Ptr& retry_policy() const {
    return default_profile_.retry_policy();
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

  const ExecutionProfile& default_profile() {
    return default_profile_;
  }

  bool profile(const String& name, ExecutionProfile& profile) const {
    // Determine if cluster profile should be used
    if (name.empty()) {
      profile = default_profile_;
      return true;
    }

    // Handle profile lookup
    ExecutionProfile::Map::const_iterator it = profiles_.find(name);
    if (it != profiles_.end()) {
      profile = it->second;
      return true;
    }
    return false;
  }

  void set_execution_profile(const String& name,
                             const ExecutionProfile* profile) {
    // Assign the host targeting profile based on the cluster profile
    // This is required as their is no exposed API to set this chained policy
    ExecutionProfile copy = *profile;
    copy.set_host_targeting(default_profile_.host_targeting());
    profiles_[name] = copy;
  }

  bool prepare_on_all_hosts() const { return prepare_on_all_hosts_; }

  void set_prepare_on_all_hosts(bool enabled) { prepare_on_all_hosts_ = enabled; }

  bool prepare_on_up_or_add_host() const { return prepare_on_up_or_add_host_; }

  void set_prepare_on_up_or_add_host(bool enabled) {
    prepare_on_up_or_add_host_ = enabled;
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
  unsigned queue_size_event_;
  unsigned queue_size_log_;
  unsigned core_connections_per_host_;
  unsigned max_connections_per_host_;
  unsigned reconnect_wait_time_ms_;
  unsigned max_concurrent_creation_;
  unsigned max_requests_per_flush_;
  unsigned max_concurrent_requests_threshold_;
  unsigned connect_timeout_ms_;
  unsigned resolve_timeout_ms_;
  CassLogLevel log_level_;
  CassLogCallback log_callback_;
  void* log_data_;
  AuthProvider::Ptr auth_provider_;
  SpeculativeExecutionPolicy::Ptr speculative_execution_policy_;
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
  LoadBalancingPolicy::Vec load_balancing_policies_;
  bool prepare_on_all_hosts_;
  bool prepare_on_up_or_add_host_;
};

} // namespace cass

#endif
