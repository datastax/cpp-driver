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

#ifndef DATASTAX_INTERNAL_CLUSTER_HPP
#define DATASTAX_INTERNAL_CLUSTER_HPP

#include "config.hpp"
#include "control_connector.hpp"
#include "event_loop.hpp"
#include "external.hpp"
#include "metadata.hpp"
#include "monitor_reporting.hpp"
#include "prepare_host_handler.hpp"
#include "prepared.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace core {

class Cluster;

class LockedHostMap {
public:
  typedef HostMap::iterator iterator;
  typedef HostMap::const_iterator const_iterator;

  LockedHostMap(const HostMap& hosts);
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
  virtual ~TokenMapListener() {}

  /**
   * A callback that's called when the token map has changed. This happens as
   * a result of the token map being rebuilt which can happen if keyspace metadata
   * has changed or if node is added/removed from a cluster.
   *
   * @param token_map The updated token map.
   */
  virtual void on_token_map_updated(const TokenMap::Ptr& token_map) = 0;
};

/**
 * A listener that handles cluster events.
 */
class ClusterListener
    : public HostListener
    , public TokenMapListener {
public:
  typedef Vector<ClusterListener*> Vec;

  virtual ~ClusterListener() {}

  /**
   * A callback that's called when the control connection receives an up event.
   * It means that the host might be available to handle queries, but not
   * necessarily.
   *
   * @param host A host that may be available.
   */
  virtual void on_host_maybe_up(const Host::Ptr& host) {}

  /**
   * A callback that's called as the result of `Cluster::notify_host_up()`.
   * It's *always* called for a valid (not ignored) host that's ready to
   * receive queries. The ready state means the host has had any previously
   * prepared queries setup on the newly available server. If the host was
   * previously ready the callback is just called.
   *
   * @param host A host that's ready to receive queries.
   */
  virtual void on_host_ready(const Host::Ptr& host) {}

  /**
   * A callback that's called when the cluster connects or reconnects to a host.
   *
   * Note: This is mostly for testing.
   *
   * @param cluster The cluster object.
   */
  virtual void on_reconnect(Cluster* cluster) {}

  /**
   * A callback that's called when the cluster has closed.
   *
   * @param cluster The cluster object.
   */
  virtual void on_close(Cluster* cluster) = 0;
};

/**
 * A class for recording host and token map events so they can be replayed.
 */
struct ClusterEvent {
  typedef Vector<ClusterEvent> Vec;
  enum Type {
    HOST_UP,
    HOST_DOWN,
    HOST_ADD,
    HOST_REMOVE,
    HOST_MAYBE_UP,
    HOST_READY,
    TOKEN_MAP_UPDATE
  };

  ClusterEvent(Type type, const Host::Ptr& host)
      : type(type)
      , host(host) {}

  ClusterEvent(const TokenMap::Ptr& token_map)
      : type(TOKEN_MAP_UPDATE)
      , token_map(token_map) {}

  static void process_event(const ClusterEvent& event, ClusterListener* listener);
  static void process_events(const Vec& events, ClusterListener* listener);

  Type type;
  Host::Ptr host;
  TokenMap::Ptr token_map;
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
  LoadBalancingPolicy::Vec load_balancing_policies;

  /**
   * The port to use for the contact points. This setting is spread to
   * the other hosts using the contact point hosts.
   */
  int port;

  /**
   * Reconnection policy to use when attempting to reconnect the control connection.
   */
  ReconnectionPolicy::Ptr reconnection_policy;

  /**
   * If true then cached prepared statements are prepared when a host is brought
   * up or is added.
   */
  bool prepare_on_up_or_add_host;

  /**
   * Max number of requests to be written out to the socket per write system call.
   */
  unsigned max_prepares_per_flush;

  /**
   * If true then events are disabled on startup. Events can be explicitly
   * started by calling `Cluster::start_events()`.
   */
  bool disable_events_on_startup;

  /**
   * A factory for creating cluster metadata resolvers. A cluster metadata resolver is used to
   * determine contact points and retrieve other metadata required to connect the
   * cluster.
   */
  ClusterMetadataResolverFactory::Ptr cluster_metadata_resolver_factory;
};

/**
 * A cluster connection. This wraps and maintains a control connection to a
 * cluster. If a host in the cluster fails then it re-establishes a new control
 * connection to a different host. A cluster will never close without an
 * explicit call to close because it repeatedly tries to re-establish its
 * connection even if no hosts are available.
 */
class Cluster
    : public RefCounted<Cluster>
    , public ControlConnectionListener {
public:
  typedef SharedRefPtr<Cluster> Ptr;

  /**
   * Constructor. Don't use directly.
   *
   * @param connection The current control connection.
   * @param listener A listener to handle cluster events.
   * @param event_loop The event loop.
   * @param connected_host The currently connected host.
   * @param hosts Available hosts for the cluster (based on load balancing
   * policies).
   * @param schema Current schema metadata.
   * @param load_balancing_policy The default load balancing policy to use for
   * determining the next control connection host.
   * @param load_balancing_policies
   * @param local_dc The local datacenter determined by the metadata service for initializing the
   * load balancing policies.
   * @param supported_options Supported options discovered during control connection.
   * @param settings The control connection settings to use for reconnecting the
   * control connection.
   */
  Cluster(const ControlConnection::Ptr& connection, ClusterListener* listener,
          EventLoop* event_loop, const Host::Ptr& connected_host, const HostMap& hosts,
          const ControlConnectionSchema& schema,
          const LoadBalancingPolicy::Ptr& load_balancing_policy,
          const LoadBalancingPolicy::Vec& load_balancing_policies, const String& local_dc,
          const StringMultimap& supported_options, const ClusterSettings& settings);

  /**
   * Set the listener that will handle events for the cluster
   * (*NOT* thread-safe).
   *
   * @param listener The cluster listener.
   */
  void set_listener(ClusterListener* listener = NULL);

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
  void notify_host_up(const Address& address);

  /**
   * Notify that a node has been determined to be down via an external source.
   * DOWN events from the control connection are ignored so it is up to other
   * sources to determine a host is unavailable (thread-safe).
   *
   * @param address That address of the host that is now unavailable.
   */
  void notify_host_down(const Address& address);

  /**
   * Start host and token map events. Events that occurred during startup will be
   * replayed (thread-safe).
   */
  void start_events();

  /**
   * Start the client monitor events (thread-safe).
   *
   * @param client_id Client ID associated with the session.
   * @param session_id Session ID associated with the session.
   * @param config The config object.
   */
  void start_monitor_reporting(const String& client_id, const String& session_id,
                               const Config& config);

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
  Host::Ptr find_host(const Address& address) const;

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
  void prepared(const String& id, const PreparedMetadata::Entry::Ptr& entry);

  /**
   * Get available hosts (determined by host distance). This filters out ignored
   * hosts (*NOT* thread-safe).
   *
   * @return A mapping of available hosts.
   */
  HostMap available_hosts() const;

public:
  ProtocolVersion protocol_version() const { return connection_->protocol_version(); }
  const Host::Ptr& connected_host() const { return connected_host_; }
  const TokenMap::Ptr& token_map() const { return token_map_; }
  const String& local_dc() const { return local_dc_; }
  const VersionNumber& dse_server_version() const { return connection_->dse_server_version(); }
  const StringMultimap& supported_options() const { return supported_options_; }

private:
  friend class ClusterRunClose;
  friend class ClusterNotifyUp;
  friend class ClusterNotifyDown;
  friend class ClusterStartEvents;
  friend class ClusterStartClientMonitor;

private:
  void update_hosts(const HostMap& hosts);
  void update_schema(const ControlConnectionSchema& schema);
  void update_token_map(const HostMap& hosts, const String& partitioner,
                        const ControlConnectionSchema& schema);

  bool is_host_ignored(const Host::Ptr& host) const;

  void schedule_reconnect();

  void on_schedule_reconnect(Timer* timer);
  void handle_schedule_reconnect();

  void on_reconnect(ControlConnector* connector);

private:
  void internal_close();
  void handle_close();

  void internal_notify_host_up(const Address& address);
  void notify_host_up_after_prepare(const Host::Ptr& host);

  void internal_notify_host_down(const Address& address);

  void internal_start_events();
  void internal_start_monitor_reporting(const String& client_id, const String& session_id,
                                        const Config& config);

  void on_monitor_reporting(Timer* timer);

  void notify_host_add(const Host::Ptr& host);
  void notify_host_add_after_prepare(const Host::Ptr& host);

  void notify_host_remove(const Address& address);

private:
  void notify_or_record(const ClusterEvent& event);

private:
  bool prepare_host(const Host::Ptr& host, const PrepareHostHandler::Callback& callback);

  void on_prepare_host_add(const PrepareHostHandler* handler);
  void on_prepare_host_up(const PrepareHostHandler* handler);

private:
  // Control connection listener methods

  virtual void on_update_schema(SchemaType type, const ResultResponse::Ptr& result,
                                const String& keyspace_name, const String& target_name);

  virtual void on_drop_schema(SchemaType type, const String& keyspace_name,
                              const String& target_name);

  virtual void on_up(const Address& address);
  virtual void on_down(const Address& address);

  virtual void on_add(const Host::Ptr& host);
  virtual void on_remove(const Address& address);

  virtual void on_close(ControlConnection* connection);

private:
  ControlConnection::Ptr connection_;
  ControlConnector::Ptr reconnector_;
  ClusterListener* listener_;
  EventLoop* const event_loop_;
  const LoadBalancingPolicy::Ptr load_balancing_policy_;
  LoadBalancingPolicy::Vec load_balancing_policies_;
  const ClusterSettings settings_;
  ScopedPtr<QueryPlan> query_plan_;
  bool is_closing_;
  Host::Ptr connected_host_;
  LockedHostMap hosts_;
  Metadata metadata_;
  PreparedMetadata prepared_metadata_;
  TokenMap::Ptr token_map_;
  String local_dc_;
  StringMultimap supported_options_;
  Timer timer_;
  bool is_recording_events_;
  ClusterEvent::Vec recorded_events_;
  ScopedPtr<MonitorReporting> monitor_reporting_;
  Timer monitor_reporting_timer_;
  ScopedPtr<ReconnectionSchedule> reconnection_schedule_;
};

}}} // namespace datastax::internal::core

#endif
