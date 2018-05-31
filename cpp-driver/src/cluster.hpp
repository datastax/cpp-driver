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

#ifndef __CASS_CLUSTER_HPP_INCLUDED__
#define __CASS_CLUSTER_HPP_INCLUDED__

#include "config.hpp"
#include "control_connector.hpp"
#include "memory.hpp"
#include "external.hpp"
#include "event_loop.hpp"
#include "metadata.hpp"
#include "prepared.hpp"
#include "prepare_host_handler.hpp"

#include <uv.h>

namespace cass {

class Cluster;

class LockedHostMap {
public:
  typedef HostMap::iterator iterator;
  typedef HostMap::const_iterator const_iterator;

  LockedHostMap();
  ~LockedHostMap();

  operator const HostMap&() const { return hosts_; }
  const_iterator begin() const { return hosts_.begin(); }
  const_iterator end() const { return hosts_.end(); }

  const_iterator find(const Address& address) const;

  Host::Ptr get(const Address& address) const;

  void erase(const Address& address);

  Host::Ptr& operator[](const Address& address);
  LockedHostMap& operator=(const HostMap& hosts);

private:
  mutable uv_mutex_t mutex_;
  HostMap hosts_;
};

/**
 * A listener that handles token map updates.
 */
class TokenMapListener {
public:
  virtual ~TokenMapListener() { }

  /**
   * A callback that's called when the token map has changed. This happens as
   * a result of the token map being rebuilt which can happen if keyspace metadata
   * has changed or if node is added/removed from a cluster.
   *
   * @param token_map The updated token map.
   */
  virtual void on_update_token_map(const TokenMap::Ptr& token_map) = 0;
};

/**
 * A listener that handles cluster events.
 */
class ClusterListener
  : public HostListener
  , public TokenMapListener {
public:
  typedef Vector<ClusterListener*> Vec;

  virtual ~ClusterListener() { }

  /**
   * A callback that's called when the cluster object connects or reconnects
   * to a host.
   *
   * Note: This is mostly for testing.
   *
   * @param cluster The cluster object.
   */
  virtual void on_reconnect(Cluster* cluster) { }

  /**
   * A callback that's called when the cluster has closed.
   *
   * @param cluster The cluster object.
   */
  virtual void on_close(Cluster* cluster) = 0;
};

/**
 * Cluster settings.
 */
struct ClusterSettings {
  /**
   * Constructor. Initialize with default settings.
   */
  ClusterSettings();

  /**
   * Constructor. Initialize with a config object.
   *
   * @param config The config object.
   */
  ClusterSettings(const Config& config);

  /**
   * Determine if host is ignored by load balancing policies.
   *
   * @param host A host
   * @return true if the host is ignored, otherwise false.
   */
  bool is_host_ignored(const Host::Ptr& host) const;

  /**
   * The settings for the underlying control connection.
   */
  ControlConnectionSettings control_connection_settings;

  /**
   * The load balancing policy to use for reconnecting the control
   * connection.
   */
  LoadBalancingPolicy::Ptr load_balancing_policy;

  /**
   * Load balancing policies for all profiles.
   */
  LoadBalancingPolicy::Vec load_balancing_polices;

  /**
   * The port to use for the contact points. This setting is spread to
   * the other hosts using the contact point hosts.
   */
  int port;

  /**
   * Time to wait before attempting to reconnect the control connection.
   */
  uint64_t reconnect_timeout_ms;

  /**
   * If true then cached prepared statements are prepared when a host is brought
   * up or is added.
   */
  bool prepare_on_up_or_add_host;


  /**
   * Max number of requests to be written out to the socket per write system call.
   */
  unsigned max_prepares_per_flush;
};

/**
 * A cluster connection. This wraps and maintains a control connection to a
 * cluster. If a host in the cluster fails then it re-establishes a new control
 * connection to a different host. A cluster will never close without an
 * explicit call to close because it repeatedly tries to re-establish its
 * connection even if no hosts are available.
 */
class Cluster : public RefCounted<Cluster>
              , public ControlConnectionListener {
public:
  typedef SharedRefPtr<Cluster> Ptr;

  /**
   * Constructor. Don't use directly.
   *
   * @param connector The connector for the control connection.
   * @param listener A listener to handle events.
   * @param load_balancing_policy A load balancing policy that's used to handle
   * reconnecting the control connection.
   * @param settings The control connection settings to use for reconnecting the
   * control connection.
   */
  Cluster(ControlConnector* connector,
          ClusterListener* listener,
          EventLoop* event_loop,
          Random* random,
          const ClusterSettings& settings);

  /**
   * Close the current connection and stop the re-connection process (thread-safe).
   */
  void close();

  /**
   * Notify that a node has been determined to be available via an external
   * source (thread-safe).
   *
   * @param address The address of the host that is now available.
   */
  void notify_up(const Address& address);

  /**
   * Notify that a node has been determined to be down via an external source.
   * DOWN events from the control connection are ignored so it is up to other
   * sources to determine a host is unavailable (thread-safe).
   *
   * @param address That address of the host that is now unavailable.
   */
  void notify_down(const Address& address);

  /**
   * Get the latest snapshot of the schema metadata (thread-safe).
   *
   * @return A schema metadata snapshot.
   */
  Metadata::SchemaSnapshot schema_snapshot();

  /**
   * Look up a host by address (thread-safe).
   *
   * @param address The address of the host.
   * @return The host object for the specified address or a null object pointer
   * if the host doesn't exist.
   */
  Host::Ptr host(const Address& address) const;

  /**
   * Get a prepared metadata entry for a prepared ID (thread-safe).
   *
   * @param id A prepared ID
   * @return The prepare metadata object for the specified ID or a null object
   * pointer if the entry doesn't exist.
   */
  PreparedMetadata::Entry::Ptr prepared(const String& id) const;

  /**
   * Set the prepared metadata for a given prepared ID (thread-safe).
   *
   * @param id A prepared ID.
   * @param entry A prepared metadata entry.
   */
  void prepared(const String& id,
                const PreparedMetadata::Entry::Ptr& entry);

public:
  int protocol_version() const { return connection_->protocol_version(); }
  const Host::Ptr& connected_host() const { return connected_host_; }
  const HostMap& hosts() const { return hosts_; }
  const TokenMap::Ptr& token_map() const { return token_map_; }

private:
  friend class RunCloseCluster;
  friend class NotifyUpCluster;
  friend class NotifyDownCluster;

private:
  void initialize(ControlConnector* connector);

  void update_hosts(const HostMap& hosts);
  void update_schema(const ControlConnectionSchema& schema);
  void update_token_map(const HostMap& hosts,
                        const String& partitioner,
                        const ControlConnectionSchema& schema);

  void schedule_reconnect();

  static void on_schedule_reconnect(Timer* timer);
  void handle_schedule_reconnect();

  static void on_reconnect(ControlConnector* connector);
  void handle_reconnect(ControlConnector* connector);

private:
  void internal_close();

  void internal_notify_up(const Address& address, const Host::Ptr& refreshed = Host::Ptr());
  void notify_up_after_prepare(const Host::Ptr& host);

  void internal_notify_down(const Address& address);

  void notify_add(const Host::Ptr& host);
  void notify_add_after_prepare(const Host::Ptr& host);

  void notify_remove(const Address& address);

private:
  bool prepare_host(const Host::Ptr& host,
                    PrepareHostHandler::Callback callback);

  static void on_prepare_host_add(const PrepareHostHandler* handler);
  static void on_prepare_host_up(const PrepareHostHandler* handler);

private:
  virtual void on_update_schema(SchemaType type,
                                const ResultResponse::Ptr& result,
                                const String& keyspace_name,
                                const String& target_name);

  virtual void on_drop_schema(SchemaType type,
                              const String& keyspace_name,
                              const String& target_name);

  virtual void on_up(const Address& address, const Host::Ptr& refreshed);
  virtual void on_down(const Address& address);

  virtual void on_add(const Host::Ptr& host);
  virtual void on_remove(const Address& address);

  virtual void on_close(ControlConnection* connection);

private:
  ControlConnection::Ptr connection_;
  ControlConnector::Ptr reconnector_;
  ClusterListener* const listener_;
  EventLoop* const event_loop_;
  const LoadBalancingPolicy::Ptr load_balancing_policy_;
  const ClusterSettings settings_;
  ScopedPtr<QueryPlan> query_plan_;
  bool is_closing_;
  Host::Ptr connected_host_;
  LockedHostMap hosts_;
  Metadata metadata_;
  PreparedMetadata prepared_metadata_;
  TokenMap::Ptr token_map_;
  Timer timer_;
};

} // namespace cass

#endif
