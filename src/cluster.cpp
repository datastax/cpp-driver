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
#include "resolver.hpp"
#include "round_robin_policy.hpp"
#include "speculative_execution.hpp"
#include "utils.hpp"

namespace cass {

/**
 * A task for initiating the cluster close process.
 */
class RunCloseCluster : public Task {
public:
  RunCloseCluster(const Cluster::Ptr& cluster)
    : cluster_(cluster) { }

  void run(EventLoop* event_loop) {
    cluster_->internal_close();
  }

private:
  Cluster::Ptr cluster_;
};

/**
 * A task for marking a node as UP.
 */
class NotifyUpCluster : public Task {
public:
  NotifyUpCluster(const Cluster::Ptr& cluster, const Address& address)
    : cluster_(cluster)
    , address_(address) { }

  void run(EventLoop* event_loop) {
    cluster_->internal_notify_up(address_);
  }

private:
  Cluster::Ptr cluster_;
  Address address_;
};

/**
 * A task for marking a node as DOWN.
 */
class NotifyDownCluster : public Task {
public:
  NotifyDownCluster(const Cluster::Ptr& cluster, const Address& address)
    : cluster_(cluster)
    , address_(address) { }

  void run(EventLoop* event_loop) {
    cluster_->internal_notify_down(address_);
  }

private:
  Cluster::Ptr cluster_;
  Address address_;
};

/**
 * A no operation cluster listener. This is used when a listener is not set.
 */
class NopClusterListener : public ClusterListener {
public:
  virtual void on_connect(Cluster* cluster) { }

  virtual void on_up(const Host::Ptr& host) { }
  virtual void on_down(const Host::Ptr& host) { }

  virtual void on_add(const Host::Ptr& host) { }
  virtual void on_remove(const Host::Ptr& host) { }

  virtual void on_update_token_map(const TokenMap::Ptr& token_map) { }

  virtual void on_close(Cluster* cluster) { }
};

static NopClusterListener nop_cluster_listener__;

LockedHostMap::LockedHostMap(const HostMap& hosts)
  : hosts_(hosts) {
  uv_mutex_init(&mutex_);
}

LockedHostMap::~LockedHostMap() {
  uv_mutex_destroy(&mutex_);
}

LockedHostMap::const_iterator LockedHostMap::find(const Address& address) const {
  return hosts_.find(address);
}

Host::Ptr LockedHostMap::get(const Address& address) const {
  ScopedMutex l(&mutex_);
  const_iterator it = find(address);
  if (it == end()) return Host::Ptr();
  return it->second;
}

void LockedHostMap::erase(const Address& address) {
  ScopedMutex l(&mutex_);
  hosts_.erase(address);
}

Host::Ptr& LockedHostMap::operator[](const Address& address) {
  ScopedMutex l(&mutex_);
  return hosts_[address];
}

LockedHostMap&LockedHostMap::operator=(const HostMap& hosts) {
  ScopedMutex l(&mutex_);
  hosts_ = hosts;
  return *this;
}

ClusterSettings::ClusterSettings()
  : load_balancing_policy(Memory::allocate<RoundRobinPolicy>())
  , port(CASS_DEFAULT_PORT)
  , reconnect_timeout_ms(CASS_DEFAULT_RECONNECT_WAIT_TIME_MS)
  , prepare_on_up_or_add_host(CASS_DEFAULT_PREPARE_ON_UP_OR_ADD_HOST)
  , max_prepares_per_flush(CASS_DEFAULT_MAX_PREPARES_PER_FLUSH) {
  load_balancing_policies.push_back(load_balancing_policy);
}

ClusterSettings::ClusterSettings(const Config& config)
  : control_connection_settings(config)
  , load_balancing_policy(config.load_balancing_policy())
  , load_balancing_policies(config.load_balancing_policies())
  , port(config.port())
  , reconnect_timeout_ms(config.reconnect_wait_time_ms())
  , prepare_on_up_or_add_host(config.prepare_on_up_or_add_host())
  , max_prepares_per_flush(CASS_DEFAULT_MAX_PREPARES_PER_FLUSH) { }

Cluster::Cluster(const ControlConnection::Ptr& connection,
                 ClusterListener* listener,
                 EventLoop* event_loop,
                 const Host::Ptr& connected_host,
                 const HostMap& hosts,
                 const ControlConnectionSchema& schema,
                 const LoadBalancingPolicy::Ptr& load_balancing_policy,
                 const LoadBalancingPolicy::Vec& load_balancing_policies,
                 const ClusterSettings& settings)
  : connection_(connection)
  , listener_(listener ? listener : &nop_cluster_listener__)
  , event_loop_(event_loop)
  , load_balancing_policy_(load_balancing_policy)
  , load_balancing_policies_(load_balancing_policies)
  , settings_(settings)
  , is_closing_(false)
  , connected_host_(connected_host)
  , hosts_(hosts) {
  inc_ref();
  connection_->set_listener(this);

  query_plan_.reset(load_balancing_policy_->new_query_plan("", NULL, NULL));

  update_schema(schema);
  update_token_map(hosts,
                   connected_host_->partitioner(),
                   schema);

  listener_->on_reconnect(this);
}

void Cluster::close() {
  event_loop_->add(Memory::allocate<RunCloseCluster>(Ptr(this)));
}

void Cluster::notify_up(const Address& address) {
  event_loop_->add(Memory::allocate<NotifyUpCluster>(Ptr(this), address));
}

void Cluster::notify_down(const Address& address) {
  event_loop_->add(Memory::allocate<NotifyDownCluster>(Ptr(this), address));
}

Metadata::SchemaSnapshot Cluster::schema_snapshot() {
  return metadata_.schema_snapshot();
}

Host::Ptr Cluster::host(const Address& address) const {
  return hosts_.get(address);
}

PreparedMetadata::Entry::Ptr Cluster::prepared(const String& id) const {
  return prepared_metadata_.get(id);
}

void Cluster::prepared(const String& id,
                       const PreparedMetadata::Entry::Ptr& entry) {
  prepared_metadata_.set(id, entry);
}

HostMap Cluster::available_hosts() const {
  HostMap available;
  for (HostMap::const_iterator it = hosts_.begin(),
       end = hosts_.end(); it != end; ++it) {
    if (!is_host_ignored(it->second)) {
      available[it->first] = it->second;
    }
  }
  return available;
}

void Cluster::update_hosts(const HostMap& hosts) {
  // Update the hosts and properly notify the listener
  HostMap existing(hosts_);

  for (HostMap::const_iterator it = hosts.begin(),
       end = hosts.end(); it != end; ++it) {
    HostMap::iterator find_it = existing.find(it->first);
    if (find_it != existing.end()) {
      existing.erase(find_it); // Already exists mark as visited
    } else {
      notify_add(it->second); // A new host has been added
    }
  }

  // Any hosts that existed before, but aren't in the new hosts
  // need to be marked as removed.
  for (HostMap::const_iterator it = existing.begin(),
       end = existing.end(); it != end; ++it) {
    notify_remove(it->first);
  }
}

void Cluster::update_schema(const ControlConnectionSchema& schema) {
  metadata_.clear_and_update_back(connection_->server_version());

  if (schema.keyspaces) {
    metadata_.update_keyspaces(schema.keyspaces.get(), false);
  }

  if (schema.tables) {
    metadata_.update_tables(schema.tables.get());
  }

  if (schema.views) {
    metadata_.update_views(schema.views.get());
  }

  if (schema.columns) {
    metadata_.update_columns(schema.columns.get());
  }

  if (schema.indexes) {
    metadata_.update_indexes(schema.indexes.get());
  }

  if (schema.user_types) {
    metadata_.update_user_types(schema.user_types.get());
  }

  if (schema.functions) {
    metadata_.update_functions(schema.functions.get());
  }

  if (schema.aggregates) {
    metadata_.update_aggregates(schema.aggregates.get());
  }

  if (schema.virtual_keyspaces) {
    metadata_.update_keyspaces(schema.virtual_keyspaces.get(), true);
  }

  if (schema.virtual_tables) {
    metadata_.update_tables(schema.virtual_tables.get());
  }

  if (schema.virtual_columns) {
    metadata_.update_columns(schema.virtual_columns.get());
  }

  metadata_.swap_to_back_and_update_front();
}

void Cluster::update_token_map(const HostMap& hosts,
                               const String& partitioner,
                               const ControlConnectionSchema& schema) {
  if (settings_.control_connection_settings.token_aware_routing && schema.keyspaces) {
    // Create a new token map and populate it
    token_map_ = TokenMap::from_partitioner(partitioner);
    if (!token_map_) {
      return; // Partition is not supported
    }
    token_map_->add_keyspaces(connection_->server_version(), schema.keyspaces.get());
    for (HostMap::const_iterator it = hosts.begin(),
         end = hosts.end(); it != end; ++it) {
      token_map_->add_host(it->second);
    }
    token_map_->build();
  }
}

// All hosts from the cluster are included in the host map and in the load
// balancing policies (LBP) so that LBPs return the correct host distance (esp.
// important for DC-aware). This method prevents connection pools from being
// created to ignored hosts.
bool Cluster::is_host_ignored(const Host::Ptr& host) const {
  return cass::is_host_ignored(load_balancing_policies_, host);
}

void Cluster::schedule_reconnect() {
  if (settings_.reconnect_timeout_ms > 0) {
    timer_.start(connection_->loop(), settings_.reconnect_timeout_ms,
                 bind_callback(&Cluster::on_schedule_reconnect, this));
  } else {
    handle_schedule_reconnect();
  }
}

void Cluster::on_schedule_reconnect(Timer* timer) {
  handle_schedule_reconnect();
}

void Cluster::handle_schedule_reconnect() {
  const Host::Ptr& host = query_plan_->compute_next();
  if (host) {
    reconnector_.reset(Memory::allocate<ControlConnector>(host->address(),
                                                          connection_->protocol_version(),
                                                          bind_callback(&Cluster::on_reconnect, this)));
    reconnector_
        ->with_settings(settings_.control_connection_settings)
        ->connect(connection_->loop());
  } else {
    // No more hosts, refresh the query plan and schedule a re-connection
    LOG_TRACE("Control connection query plan has no more hosts. "
              "Reset query plan and schedule reconnect");
    query_plan_.reset(load_balancing_policy_->new_query_plan("", NULL, NULL));
    schedule_reconnect();
  }
}

void Cluster::on_reconnect(ControlConnector* connector) {
  reconnector_.reset();
  if (is_closing_) {
    handle_close();
    return;
  }

  if (connector->is_ok()) {
    connection_ = connector->release_connection();
    connection_->set_listener(this);

    // Incrementally update the hosts  (notifying the listener)
    update_hosts(connector->hosts());

    // Get the newly connected host
    connected_host_ = hosts_[connection_->address()];
    assert(connected_host_ && "Connected host not found in hosts map");

    update_schema(connector->schema());
    update_token_map(connector->hosts(),
                     connected_host_->partitioner(),
                     connector->schema());

    // Notify the listener that we've built a new token map
    if (token_map_) {
      listener_->on_update_token_map(token_map_);
    }

    LOG_INFO("Control connection connected to %s",
             connected_host_->address_string().c_str());

    listener_->on_reconnect(this);
  } else if (!connector->is_canceled()) {
    LOG_ERROR("Unable to reestablish a control connection to host %s because of the following error: %s",
              connector->address().to_string().c_str(),
              connector->error_message().c_str());
    schedule_reconnect();
  }
}

void Cluster::internal_close() {
  is_closing_ = true;
  if (timer_.is_running()) {
    timer_.stop();
    handle_close();
  } else if (reconnector_) {
    reconnector_->cancel();
  } else if (connection_)  {
    connection_->close();
  }
}

void Cluster::handle_close() {
  for (LoadBalancingPolicy::Vec::const_iterator it = load_balancing_policies_.begin(),
       end = load_balancing_policies_.end(); it != end; ++it) {
    (*it)->close_handles();
  }
  connection_.reset();
  listener_->on_close(this);
  dec_ref();
}

void Cluster::internal_notify_up(const Address& address, const Host::Ptr& refreshed) {
  LockedHostMap::const_iterator it = hosts_.find(address);

  if (it == hosts_.end()) {
    LOG_WARN("Attempting to mark host %s that we don't have as UP",
             address.to_string().c_str());
    return;
  }

  Host::Ptr host(it->second);

  if (refreshed){
    if (token_map_) {
      token_map_ = token_map_->copy();
      token_map_->update_host_and_build(refreshed);
      listener_->on_update_token_map(token_map_);
    }
    hosts_[address] = host = refreshed;
  }

  if (host->is_up()) { // Check the state of the previously existing host.
    // Already marked up so don't repeat duplicate notifications.
    return;
  }

  host->set_up();
  for (LoadBalancingPolicy::Vec::const_iterator it = load_balancing_policies_.begin(),
       end = load_balancing_policies_.end(); it != end; ++it) {
    (*it)->on_up(host);
  }

  if (is_host_ignored(host)) {
    return; // Ignore host
  }

  if (!prepare_host(host,
                    bind_callback(&Cluster::on_prepare_host_up, this))) {
    notify_up_after_prepare(host);
  }
}

void Cluster::notify_up_after_prepare(const Host::Ptr& host) {
  listener_->on_up(host);
}

void Cluster::internal_notify_down(const Address& address) {
  LockedHostMap::const_iterator it = hosts_.find(address);

  if (it == hosts_.end()) {
    LOG_WARN("Attempting to mark host %s that we don't have as DOWN",
             address.to_string().c_str());
    return;
  }

  Host::Ptr host(it->second);

  if (host->is_down()) {
    // Already marked down so don't repeat duplicate notifications.
    return;
  }

  host->set_down();
  for (LoadBalancingPolicy::Vec::const_iterator it = load_balancing_policies_.begin(),
       end = load_balancing_policies_.end(); it != end; ++it) {
    (*it)->on_down(host);
  }

  listener_->on_down(host);
}

void Cluster::notify_add(const Host::Ptr& host) {
  LockedHostMap::const_iterator host_it = hosts_.find(host->address());

  if (host_it != hosts_.end()) {
    LOG_WARN("Attempting to add host %s that we already have",
             host->address_string().c_str());
    // If an entry already exists then notify that the node has been removed
    // then re-add it.
    for (LoadBalancingPolicy::Vec::const_iterator it = load_balancing_policies_.begin(),
         end = load_balancing_policies_.end(); it != end; ++it) {
      (*it)->on_remove(host_it->second);
    }
    listener_->on_remove(host_it->second);
  }

  hosts_[host->address()] = host;
  for (LoadBalancingPolicy::Vec::const_iterator it = load_balancing_policies_.begin(),
       end = load_balancing_policies_.end(); it != end; ++it) {
    (*it)->on_add(host);
  }

  if (is_host_ignored(host)) {
    return; // Ignore host
  }

  if (!prepare_host(host,
                    bind_callback(&Cluster::on_prepare_host_add, this))) {
    notify_add_after_prepare(host);
  }
}

void Cluster::notify_add_after_prepare(const Host::Ptr& host) {
  if (token_map_) {
    token_map_ = token_map_->copy();
    token_map_->update_host_and_build(host);
    listener_->on_update_token_map(token_map_);
  }
  listener_->on_add(host);
}

void Cluster::notify_remove(const Address& address) {
  LockedHostMap::const_iterator it = hosts_.find(address);

  if (it == hosts_.end()) {
    LOG_WARN("Attempting removing host %s that we don't have",
             address.to_string().c_str());
    return;
  }

  Host::Ptr host(it->second);

  if (token_map_) {
    token_map_ = token_map_->copy();
    token_map_->remove_host_and_build(host);
    listener_->on_update_token_map(token_map_);
  }

  hosts_.erase(host->address());
  for (LoadBalancingPolicy::Vec::const_iterator it = load_balancing_policies_.begin(),
       end = load_balancing_policies_.end(); it != end; ++it) {
    (*it)->on_remove(host);
  }
  listener_->on_remove(host);
}

bool Cluster::prepare_host(const Host::Ptr& host,
                           const PrepareHostHandler::Callback& callback) {
  if (settings_.prepare_on_up_or_add_host) {
    PrepareHostHandler::Ptr prepare_host_handler(
          Memory::allocate<PrepareHostHandler>(host,
                                               prepared_metadata_.copy(),
                                               callback,
                                               connection_->protocol_version(),
                                               settings_.max_prepares_per_flush));

    prepare_host_handler->prepare(connection_->loop(),
                                  settings_.control_connection_settings.connection_settings);
    return true;
  }
  return false;
}

void Cluster::on_prepare_host_add(const PrepareHostHandler* handler) {
  notify_add_after_prepare(handler->host());
}

void Cluster::on_prepare_host_up(const PrepareHostHandler* handler) {
  notify_up_after_prepare(handler->host());
}

void Cluster::on_update_schema(SchemaType type,
                               const ResultResponse::Ptr& result,
                               const String& keyspace_name,
                               const String& target_name) {
  switch (type) {
    case KEYSPACE:
      // Virtual keyspaces are not updated (always false)
      metadata_.update_keyspaces(result.get(), false);
      if (token_map_) {
        token_map_ = token_map_->copy();
        token_map_->update_keyspaces_and_build(connection_->server_version(), result.get());
        listener_->on_update_token_map(token_map_);
      }
      break;
    case TABLE:
      metadata_.update_tables(result.get());
      break;
    case VIEW:
      metadata_.update_views(result.get());
      break;
    case COLUMN:
      metadata_.update_columns(result.get());
      break;
    case INDEX:
      metadata_.update_indexes(result.get());
      break;
    case USER_TYPE:
      metadata_.update_user_types(result.get());
      break;
    case FUNCTION:
      metadata_.update_functions(result.get());
      break;
    case AGGREGATE:
      metadata_.update_aggregates(result.get());
      break;
  }
}

void Cluster::on_drop_schema(SchemaType type,
                             const String& keyspace_name,
                             const String& target_name) {
  switch (type) {
    case KEYSPACE:
      metadata_.drop_keyspace(keyspace_name);
      if (token_map_) {
        token_map_ = token_map_->copy();
        token_map_->drop_keyspace(keyspace_name);
        listener_->on_update_token_map(token_map_);
      }
      break;
    case TABLE:
      metadata_.drop_table_or_view(keyspace_name, target_name);
      break;
    case VIEW:
      metadata_.drop_table_or_view(keyspace_name, target_name);
      break;
    case USER_TYPE:
      metadata_.drop_user_type(keyspace_name, target_name);
      break;
    case FUNCTION:
      metadata_.drop_function(keyspace_name, target_name);
      break;
    case AGGREGATE:
      metadata_.drop_aggregate(keyspace_name, target_name);
      break;
    default:
      break;
  }
}

void Cluster::on_up(const Address& address, const Host::Ptr& refreshed) {
  internal_notify_up(address, refreshed);
}

void Cluster::on_down(const Address& address) {
  // Ignore on down events
}

void Cluster::on_add(const Host::Ptr& host) {
  notify_add(host);
}

void Cluster::on_remove(const Address& address) {
  notify_remove(address);
}

void Cluster::on_close(ControlConnection* connection) {
  if (!is_closing_) {
    LOG_WARN("Lost control connection to host %s",
             connection_->address_string().c_str());
    schedule_reconnect();
  } else {
    handle_close();
  }
}

} // namespace cass
