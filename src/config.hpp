/*
  Copyright (c) 2014-2015 DataStax

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
#include "dc_aware_policy.hpp"
#include "latency_aware_policy.hpp"
#include "ssl.hpp"
#include "token_aware_policy.hpp"

#include <list>
#include <string>

namespace cass {

void stderr_log_callback(const CassLogMessage* message, void* data);

class Config {
public:
  typedef std::list<std::string> ContactPointList;

  Config()
      : port_(9042)
      , protocol_version_(2)
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
      , write_bytes_high_water_mark_(64 * 1024)
      , write_bytes_low_water_mark_(32 * 1024)
      , pending_requests_high_water_mark_(128 * max_connections_per_host_)
      , pending_requests_low_water_mark_(pending_requests_high_water_mark_ / 2)
      , connect_timeout_ms_(5000)
      , request_timeout_ms_(12000)
      , log_level_(CASS_LOG_WARN)
      , log_callback_(stderr_log_callback)
      , log_data_(NULL)
      , auth_provider_(new AuthProvider())
      , load_balancing_policy_(new DCAwarePolicy())
      , token_aware_routing_(true)
      , latency_aware_routing_(false)
      , tcp_nodelay_enable_(false)
      , tcp_keepalive_enable_(false)
      , tcp_keepalive_delay_secs_(0) {}

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

  unsigned write_bytes_high_water_mark() const {
    return write_bytes_high_water_mark_;
  }

  void set_write_bytes_high_water_mark(unsigned write_bytes_high_water_mark) {
    write_bytes_high_water_mark_ = write_bytes_high_water_mark;
  }

  unsigned write_bytes_low_water_mark() const {
    return write_bytes_low_water_mark_;
  }

  void set_write_bytes_low_water_mark(unsigned write_bytes_low_water_mark) {
    write_bytes_low_water_mark_ = write_bytes_low_water_mark;
  }

  unsigned pending_requests_high_water_mark() const {
    return pending_requests_high_water_mark_;
  }

  void set_pending_requests_high_water_mark(unsigned pending_requests_high_water_mark) {
    pending_requests_high_water_mark_ = pending_requests_high_water_mark;
  }

  unsigned pending_requests_low_water_mark() const {
    return pending_requests_low_water_mark_;
  }

  void set_pending_requests_low_water_mark(unsigned pending_requests_low_water_mark) {
    pending_requests_low_water_mark_ = pending_requests_low_water_mark;
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

  unsigned request_timeout_ms() const { return request_timeout_ms_; }

  void set_request_timeout(unsigned timeout_ms) {
    request_timeout_ms_ = timeout_ms;
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

  const SharedRefPtr<AuthProvider>& auth_provider() const { return auth_provider_; }

  void set_auth_provider(AuthProvider* auth_provider) {
    if (auth_provider == NULL) return;
    auth_provider_.reset(auth_provider);
  }

  void set_credentials(const std::string& username, const std::string& password) {
    auth_provider_.reset(new PlainTextAuthProvider(username, password));
  }

  LoadBalancingPolicy* load_balancing_policy() const {
    // base LBP can be augmented by special wrappers (whitelist, token aware, latency aware)
    LoadBalancingPolicy* chain = load_balancing_policy_->new_instance();
    if (token_aware_routing()) {
      chain = new TokenAwarePolicy(chain);
    }
    if (latency_aware()) {
      chain = new LatencyAwarePolicy(chain, latency_aware_routing_settings_);
    }
    return chain;
  }

  void set_load_balancing_policy(LoadBalancingPolicy* lbp) {
    if (lbp == NULL) return;
    load_balancing_policy_.reset(lbp);
  }

  SslContext* ssl_context() const { return ssl_context_.get(); }

  void set_ssl_context(SslContext* ssl_context) {
    ssl_context_.reset(ssl_context);
  }

  bool token_aware_routing() const { return token_aware_routing_; }

  void set_token_aware_routing(bool is_token_aware) { token_aware_routing_ = is_token_aware; }

  bool latency_aware() const { return latency_aware_routing_; }

  void set_latency_aware_routing(bool is_latency_aware) { latency_aware_routing_ = is_latency_aware; }

  void set_latency_aware_routing_settings(const LatencyAwarePolicy::Settings& settings) {
    latency_aware_routing_settings_ = settings;
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

private:
  int port_;
  int protocol_version_;
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
  unsigned write_bytes_high_water_mark_;
  unsigned write_bytes_low_water_mark_;
  unsigned pending_requests_high_water_mark_;
  unsigned pending_requests_low_water_mark_;
  unsigned connect_timeout_ms_;
  unsigned request_timeout_ms_;
  CassLogLevel log_level_;
  CassLogCallback log_callback_;
  void* log_data_;
  SharedRefPtr<AuthProvider> auth_provider_;
  SharedRefPtr<LoadBalancingPolicy> load_balancing_policy_;
  SharedRefPtr<SslContext> ssl_context_;
  bool token_aware_routing_;
  bool latency_aware_routing_;
  LatencyAwarePolicy::Settings latency_aware_routing_settings_;
  bool tcp_nodelay_enable_;
  bool tcp_keepalive_enable_;
  unsigned tcp_keepalive_delay_secs_;
};

} // namespace cass

#endif
