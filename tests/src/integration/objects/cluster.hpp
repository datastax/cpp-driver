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

#ifndef __TEST_CLUSTER_HPP__
#define __TEST_CLUSTER_HPP__
#include "cassandra.h"

#include "object_base.hpp"

#include "session.hpp"
#include "ssl.hpp"
#include "timestamp_generator.hpp"

#include <string>

#include <gtest/gtest.h>

namespace test { namespace driver {

/**
 * Wrapped cluster object (builder)
 */
class Cluster : public Object<CassCluster, cass_cluster_free> {
public:
  /**
   * Create the cluster for the builder object
   */
  Cluster()
      : Object<CassCluster, cass_cluster_free>(cass_cluster_new()) {}

  /**
   * Create the cluster for the builder object
   *
   * @param cluster Already defined cluster object to utilize
   */
  Cluster(CassCluster* cluster)
      : Object<CassCluster, cass_cluster_free>(cluster) {}

  /**
   * Create the cluster object from a shared reference
   *
   * @param cluster Shared reference
   */
  Cluster(Ptr cluster)
      : Object<CassCluster, cass_cluster_free>(cluster) {}

  /**
   * Destroy the cluster
   */
  virtual ~Cluster(){};

  /**
   * Build/Create the cluster
   *
   * @return Cluster object
   */
  static Cluster build() { return Cluster(); }

  /**
   * Sets the custom authenticator
   */
  Cluster& with_authenticator_callbacks(const CassAuthenticatorCallbacks* exchange_callbacks,
                                        CassAuthenticatorDataCleanupCallback cleanup_callback,
                                        void* data) {
    EXPECT_EQ(CASS_OK, cass_cluster_set_authenticator_callbacks(get(), exchange_callbacks,
                                                                cleanup_callback, data));
    return *this;
  }

  /**
   * Sets the timeout for connecting to a node
   *
   * @param timeout_ms Connect timeout in milliseconds
   * @return Cluster object
   */
  Cluster& with_connect_timeout(unsigned int timeout_ms) {
    cass_cluster_set_connect_timeout(get(), timeout_ms);
    return *this;
  }

  /**
   * Sets the amount of time between heartbeat messages and controls the amount
   * of time the connection must be idle before sending heartbeat messages. This
   * is useful for preventing intermediate network devices from dropping
   * connections
   *
   * @param interval_s Heartbeat time interval (in seconds); 0 to disable
   *                   heartbeat messages (default: 30s)
   * @return Cluster object
   */
  Cluster& with_connection_heartbeat_interval(unsigned int interval_s = 30u) {
    cass_cluster_set_connection_heartbeat_interval(get(), interval_s);
    return *this;
  }

  /**
   * Sets the amount of time a connection is allowed to be without a successful
   * heartbeat response before being terminated and scheduled for reconnection.
   *
   * @param interval_s Idle timeout (in seconds); 0 to disable heartbeat messages (default: 60s)
   * @return Cluster object
   */
  Cluster& with_connection_idle_timeout(unsigned int interval_s = 60u) {
    cass_cluster_set_connection_idle_timeout(get(), interval_s);
    return *this;
  }

  /**
   * Assign/Append the contact points; passing an empty string will clear
   * the contact points
   *
   * @param contact_points A comma delimited list of hosts (addresses or
   *                       names)
   * @return Cluster object
   */
  Cluster& with_contact_points(const std::string& contact_points) {
    EXPECT_EQ(CASS_OK, cass_cluster_set_contact_points(get(), contact_points.c_str()));
    return *this;
  }

  /**
   * Assign the local address to bind; passing an empty string will clear
   * the local address.
   *
   * @param name An IP address or hostname
   * @return Cluster object
   */
  Cluster& with_local_address(const std::string& name) {
    EXPECT_EQ(CASS_OK, cass_cluster_set_local_address(get(), name.c_str()));
    return *this;
  }

  /**
   * Assign the number of connections made to each node/server for each
   * connections thread
   *
   * NOTE: One extra connection is established (the control connection)
   *
   * @param connections Number of connection per host (default: 1)
   * @return Cluster object
   */
  Cluster& with_core_connections_per_host(unsigned int connections = 1u) {
    EXPECT_EQ(CASS_OK, cass_cluster_set_core_connections_per_host(get(), connections));
    return *this;
  }

  /**
   * Sets credentials for plain text authentication
   *
   * @param username Username
   * param password Password
   * @return Cluster object
   */
  Cluster& with_credentials(const char* username, const char* password) {
    cass_cluster_set_credentials(get(), username, password);
    return *this;
  }

  /**
   * Set/Add a execution profile
   *
   * @param name Name for the execution profile
   * @param profile Execution profile to add to the cluster
   * @return Cluster object
   */
  Cluster& with_execution_profile(const std::string& name, ExecutionProfile profile) {
    EXPECT_EQ(CASS_OK, cass_cluster_set_execution_profile(get(), name.c_str(), profile.get()));
    return *this;
  }

  /**
   * Sets a callback for handling host state changes in the cluster
   *
   * @param callback Callback to use for cluster host state changes
   * @param data User data supplied to the callback (default: NULL)
   * @return Cluster object
   */
  Cluster& with_host_listener_callback(CassHostListenerCallback callback, void* data = NULL) {
    EXPECT_EQ(CASS_OK, cass_cluster_set_host_listener_callback(get(), callback, data));
    return *this;
  }

  /**
   * Sets the number of I/O threads. This is the number of threads that will
   * handle query requests
   *
   * @param num_threads Number of thread that will handle query requests
   * @return Cluster object
   */
  Cluster& with_num_threads_io(unsigned int num_threads) {
    EXPECT_EQ(CASS_OK, cass_cluster_set_num_threads_io(get(), num_threads));
    return *this;
  }

  /**
   * Enable data center aware load balance policy for statement/batch execution
   *
   * @param local_dc The primary data center to try first
   * @param used_hosts_per_remote_dc The number of hosts used in each remote
   *                                 data center if no hosts are available in
   *                                 the local data center
   * @param allow_remote_dcs_for_local_cl True if remote hosts are to be used as
   *                                      local data centers when no local data
   *                                      center is available and consistency
   *                                      levels are LOCAL_ONE or LOCAL_QUORUM;
   *                                      otherwise false
   * @return Cluster object
   */
  Cluster& with_load_balance_dc_aware(const std::string& local_dc, size_t used_hosts_per_remote_dc,
                                      bool allow_remote_dcs_for_local_cl) {
    EXPECT_EQ(CASS_OK, cass_cluster_set_load_balance_dc_aware(
                           get(), local_dc.c_str(), used_hosts_per_remote_dc,
                           (allow_remote_dcs_for_local_cl == true ? cass_true : cass_false)));
    return *this;
  }

  /**
   * Enable round robin load balance policy for statement/batch execution
   *
   * @return Cluster object
   */
  Cluster& with_load_balance_round_robin() {
    cass_cluster_set_load_balance_round_robin(get());
    return *this;
  }

  /**
   * Enable NO_COMPACT in the STARTUP OPTIONS for the connection
   *
   * @param enable True if NO_COMPACT should be enable; false otherwise
   *              (default: true)
   * @return Cluster object
   */
  Cluster& with_no_compact(bool enable = true) {
    EXPECT_EQ(CASS_OK, cass_cluster_set_no_compact(get(), enable == true ? cass_true : cass_false));
    return *this;
  }

  /**
   * Sets the port
   *
   * @param port Port number to set
   * @return Cluster object
   */
  Cluster& with_port(int port) {
    EXPECT_EQ(CASS_OK, cass_cluster_set_port(get(), port));
    return *this;
  }

  /**
   * Assign the use of a particular binary protocol version; driver will
   * automatically downgrade to the lowest server supported version on
   * connection
   *
   * @param protocol_version Binary protocol version
   * @return Cluster object
   */
  Cluster& with_protocol_version(int protocol_version) {
    EXPECT_EQ(CASS_OK, cass_cluster_set_protocol_version(get(), protocol_version));
    return *this;
  }

  /**
   * Enable/Disable the randomization of the contact points list
   *
   * @param enable True if contact points should be randomized false otherwise
   *               (default: true)
   * @return Cluster object
   */
  Cluster& with_randomized_contact_points(bool enable = true) {
    cass_cluster_set_use_randomized_contact_points(get(),
                                                   (enable == true ? cass_true : cass_false));
    return *this;
  }

  /**
   * Sets the constant reconnection policy.
   *
   * @param delay_ms Delay in milliseconds (default: 2000)
   * @return Cluster object
   */
  Cluster& with_constant_reconnect(unsigned int delay_ms) {
    cass_cluster_set_constant_reconnect(get(), delay_ms);
    return *this;
  }

  /**
   * Sets the timeout (in milliseconds) for waiting for a response from a node
   *
   * @param timeout_ms Request timeout in milliseconds; 0 to disable timeout
   *                   (default: 12s)
   */
  Cluster& with_request_timeout(unsigned int timeout_ms = 12000u) {
    cass_cluster_set_request_timeout(get(), timeout_ms);
    return *this;
  }

  /**
   * Sets the retry policy used for all requests unless overridden by setting
   * a retry policy on a statement or a batch.
   *
   * @param retry_policy Retry policy to assign to the cluster profile
   */
  Cluster& with_retry_policy(RetryPolicy retry_policy) {
    cass_cluster_set_retry_policy(get(), retry_policy.get());
    return *this;
  }

  /**
   * Enable/Disable the schema metadata
   *
   * If disabled this allows the driver to skip over retrieving and
   * updating schema metadata, but it also disables the usage of token-aware
   * routing and session->schema() will always return an empty object. This
   * can be useful for reducing the startup overhead of short-lived sessions
   *
   * @param enable True if schema metada should be enabled; false otherwise
   *               (default: true)
   * @return Cluster object
   */
  Cluster& with_schema_metadata(bool enable = true) {
    cass_cluster_set_use_schema(get(), (enable == true ? cass_true : cass_false));
    return *this;
  }

  /**
   * Enable whitelist filtering.
   *
   * @param hosts A comma delimited list of hosts (addresses or
   *              names)
   * @return Cluster object
   */
  Cluster& with_whitelist_filtering(const std::string& hosts) {
    cass_cluster_set_whitelist_filtering(get(), hosts.c_str());
    return *this;
  }

  /**
   * Enable/Disable preparing all hosts when preparing a new statement.
   *
   * @param enable
   * @return Cluster object
   */
  Cluster& with_prepare_on_all_hosts(bool enable) {
    EXPECT_EQ(CASS_OK,
              cass_cluster_set_prepare_on_all_hosts(get(), enable ? cass_true : cass_false));
    return *this;
  }

  /**
   * Enable/Disable preparing existing statements on new or down hosts.
   *
   * @param enable
   * @return Cluster object
   */
  Cluster& with_prepare_on_up_or_add_host(bool enable) {
    EXPECT_EQ(CASS_OK,
              cass_cluster_set_prepare_on_up_or_add_host(get(), enable ? cass_true : cass_false));
    return *this;
  }

  /**
   * Enable constant speculative execution
   *
   *
   * @param constant_delay_ms Constant delay (in milliseconds)
   * @param max_speculative_executions Maximum number of speculative executions
   * @return Cluster object
   */
  Cluster& with_constant_speculative_execution_policy(int64_t constant_delay_ms,
                                                      int max_speculative_executions) {
    EXPECT_EQ(CASS_OK, cass_cluster_set_constant_speculative_execution_policy(
                           get(), constant_delay_ms, max_speculative_executions));
    return *this;
  }

  /**
   * Sets the SSL context and enables SSL
   *
   * @param ssl Ssl object
   * @return Cluster object
   */
  Cluster& with_ssl(Ssl ssl) {
    cass_cluster_set_ssl(get(), ssl.get());
    return *this;
  }

  /**
   * Set the timestamp generator
   *
   * @param timestamp_generator Timestamp generator
   * @return Cluster object
   */
  Cluster& with_timestamp_generator(TimestampGenerator timestamp_generator) {
    cass_cluster_set_timestamp_gen(get(), timestamp_generator.get());
    return *this;
  }

  /**
   * Create a new session and establish a connection to the server;
   * synchronously
   *
   * @param keyspace Keyspace to use (default: None)
   * @return Session object
   * @throws Session::Exception If session could not be established
   */
  Session connect(const std::string& keyspace = "", bool assert_ok = true) {
    return Session::connect(get(), keyspace, assert_ok);
  }

  /**
   * Asynchronously connect the provided session with the settings of this cluster object.
   *
   * @param session The session to connect.
   * @param keyspace Keyspace to use (default: None)
   * @return A future to track the connection process of the session.
   */
  Future connect_async(Session& session, const std::string& keyspace = "") {
    Future future;
    if (keyspace.empty()) {
      future = cass_session_connect(session.get(), get());
    } else {
      future = cass_session_connect_keyspace(session.get(), get(), keyspace.c_str());
    }
    return future;
  }
};

}} // namespace test::driver

#endif // __TEST_CLUSTER_HPP__
