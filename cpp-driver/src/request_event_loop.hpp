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

#ifndef __CASS_REQUEST_EVENT_LOOP_HPP_INCLUDED__
#define __CASS_REQUEST_EVENT_LOOP_HPP_INCLUDED__

#include "atomic.hpp"
#include "config.hpp"
#include "connection_pool_manager.hpp"
#include "event_loop.hpp"
#include "host.hpp"
#include "random.hpp"
#include "scoped_ptr.hpp"
#include "timer.hpp"
#include "token_map.hpp"

namespace cass {

class ConnectionPoolManagerInitializer;
class Session;

/**
 * Request event loop for processing client session request(s). This event loop
 * will fetch a request from the queue and process them accordingly by applying
 * the load balancing policy, executing and routing the request to the
 * appropriate node and perform the callback to the client.
 */
class RequestEventLoop : public EventLoop
                       , public ConnectionPoolManagerListener {
public:
  /**
   * Create the request event loop making copies of the cluster configuration
   * settings.
   */
  RequestEventLoop();

  /**
   * Initialize the request event loop
   *
   * @param config Cluster configuration settings
   * @param connect_keyspace Keyspace session is connecting to; may be empty
   * @param session Session instance
   * @return Returns 0 if successful, otherwise an error occurred
   */
  int init(const Config& config,
           const String& connect_keyspace,
           Session* session);
  /**
   * Connect the request event loop to the pre-established hosts using the
   * given protocol version and initialize the local token map
   *
   * @param connected_host Current connected host
   * @param protocol_version Protocol version established
   * @param hosts Map of addresses with their associated hosts
   * @param token_map Initial calculated token map
   */
  void connect(const Host::Ptr& connected_host,
               int protocol_version,
               const HostMap& hosts,
               const TokenMap* token_map);
  /**
   * Update the current keyspace being used for requests (thread-safe)
   *
   * @param keyspace New current keyspace to utilize
   */
  void keyspace_update(const String& keyspace);
  /**
   * Terminate the request event loops
   */
  void terminate();

  /* Notifications to be performed by the request event loop thread */
  void notify_host_add_async(const Host::Ptr& host);
  void notify_host_remove_async(const Host::Ptr& host);
  void notify_token_map_update_async(const TokenMap* token_map);
  void notify_request_async();

  /**
   * Get addresses for all available hosts (thread-safe).
   *
   * @return A vector of addresses.
   */
  AddressVec available() const;
  /**
   * Find the least busy connection for a given host (thread-safe).
   *
   * @param address The address of the host to find a least busy connection.
   * @return The least busy connection for a host or null if no connections are
   *         available.
   */
  PooledConnection::Ptr find_least_busy(const Address& address) const;

  /* Connection pool manager listener callbacks */
  virtual void on_up(const Address& address);
  virtual void on_down(const Address& address);
  virtual void on_critical_error(const Address& address,
                                 Connector::ConnectionError code,
                                 const String& message);
  virtual void on_close();

private:
  void internal_connect(const Host::Ptr& current_host,
                        int protocol_version,
                        const HostMap& hosts);
  void internal_close();
  void internal_terminate();
  void internal_token_map_update(const TokenMap* token_map);

  Host::Ptr get_host(const Address& address);
  const LoadBalancingPolicy::Vec& load_balancing_policies() const;

private:
  // Connection pool manager initializer listener callback task for async operations
  class NotifyConnectionPoolManagerInitalize : public Task {
  public:
    NotifyConnectionPoolManagerInitalize(const ConnectionPoolManager::Ptr& manager,
                                         const ConnectionPoolConnector::Vec& failures)
      : manager_(manager)
      , failures_(failures) { }
    virtual void run(EventLoop* event_loop);
  private:
    ConnectionPoolManager::Ptr manager_;
    ConnectionPoolConnector::Vec failures_;
  };

  // Session/RoundRobinRequestEventLoopGroup tasks for async operations
  class NotifyHostAdd : public Task {
  public:
    NotifyHostAdd(const Host::Ptr host)
      : host_(host) { }
    virtual void run(EventLoop* event_loop);
  private:
    const Host::Ptr host_;
  };

  class NotifyHostRemove : public Task {
  public:
    NotifyHostRemove(const Host::Ptr host)
      : host_(host) { }
    virtual void run(EventLoop* event_loop);
  private:
    const Host::Ptr host_;
  };

  class NotifyTokenMapUpdate : public Task {
  public:
    NotifyTokenMapUpdate(const TokenMap* token_map)
      : token_map_(token_map) { }
    virtual void run(EventLoop* event_loop);
  private:
    const TokenMap* token_map_;
  };

  class NotifyRequest : public Task {
  public:
    NotifyRequest() { }
    virtual void run(EventLoop* event_loop);
  };

  // Connection pool manager listener callback tasks for async operations
  class NotifyHostDown : public Task {
  public:
    NotifyHostDown(const Address& address)
      : address_(address) { }
    virtual void run(EventLoop* event_loop);
  private:
    const Address address_;
  };

  class NotifyHostUp : public Task {
  public:
    NotifyHostUp(const Address& address)
      : address_(address) { }
    virtual void run(EventLoop* event_loop);
  private:
    const Address address_;
  };

  static void on_connection_pool_manager_initialize(ConnectionPoolManagerInitializer* initializer);
  void internal_connection_pool_manager_initialize(const ConnectionPoolManager::Ptr& manager,
                                                   const ConnectionPoolConnector::Vec& failures);

  void internal_host_add_down_up(const Host::Ptr& host, Host::HostState state);
  void internal_host_remove(const Host::Ptr& host);

  static void on_flush_timer(Timer* timer);
  void internal_flush_requests();

private:
  Config config_;
  String connect_keyspace_;
  HostMap hosts_;
  Metrics* metrics_;
  ScopedPtr<Random> random_;
  Session* session_;
  ScopedPtr<const TokenMap> token_map_;

  ConnectionPoolManager::Ptr manager_;

  Atomic<bool> is_flushing_;
  Timer timer_;
};

/**
 * A group of request event loops where pre-defined tasks are assigned to all or
 * a specific request event loop using round-robin.
 */
class RoundRobinRequestEventLoopGroup {
public:
  /**
   * Constructor
   *
   * @param num_threads Number of request event loop threads to handle
   *                    processing of the client requests
   */
  RoundRobinRequestEventLoopGroup(size_t num_threads)
    : current_(0)
    , threads_(num_threads) { }

   /**
    * Initialize the request event loop group
    *
    * @param config Cluster configuration settings
    * @param keyspace Keyspace session is connected to; may be empty
    * @param session Session instance
    * @return Returns 0 if successful, otherwise an error occurred
    */
  int init(const Config& config, const String& keyspace, Session* session);
  /**
   * Start the request event loop thread.
   *
   * @return Returns 0 if successful, otherwise an error occurred.
   */
  void run();
  /**
   * Waits for the request event loop thread to exit (thread-safe).
   */
  void join();

  /**
   * Get addresses for all available hosts (thread-safe).
   *
   * @return A vector of addresses.
   */
  AddressVec available();
  /**
   * Find the least busy connection for a given host (thread-safe).
   *
   * @param address The address of the host to find a least busy connection.
   * @return The least busy connection for a host or null if no connections are
   * available.
   */
  PooledConnection::Ptr find_least_busy(const Address& address);

  /**
   * Connect the request event loops to the pre-established hosts using the
   * given protocol version and initialize the local token map
   *
   * @param connected_host Current connected host
   * @param protocol_version Protocol version established
   * @param hosts Map of addresses with their associated hosts
   * @param token_map Initial calculated token map (do not clone)
   */
  void connect(const Host::Ptr& current_host,
               int protocol_version,
               const HostMap& hosts,
               const TokenMap* token_map);
  /**
   * Update the current keyspace being used for requests
   *
   * @param keyspace New current keyspace to utilize
   */
  void keyspace_update(const String& keyspace);
  /**
   * Terminate the request event loops
   */
  void terminate();

  /**
   * Add a new host to the request event loops
   *
   * @param host New host to be added
   */
  void notify_host_add_async(const Host::Ptr& host);
  /**
   * Remove a host from the request event loops
   *
   * @param host Host to be removed
   */
  void notify_host_remove_async(const Host::Ptr& host);
  /**
   * Update the token map being used for the requests
   *
   * @param token_map Update token map (do not clone)
   */
  void notify_token_map_update_async(const TokenMap* token_map);
  /**
   * Notify one of the request event loops that a new request is available.
   *
   * NOTE: The request event loop selected during the round robin process may or
   *       may not be notified if it is currently flushing requests from the
   *       queue.
   */
  void notify_request_async();

private:
  Atomic<size_t> current_;
  DynamicArray<RequestEventLoop> threads_;
};

} // namespace cass

#endif // __CASS_REQUEST_EVENT_LOOP_HPP_INCLUDED__
