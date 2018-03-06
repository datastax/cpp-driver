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

#ifndef __CASS_CONTROL_CONNECTION_HPP_INCLUDED__
#define __CASS_CONTROL_CONNECTION_HPP_INCLUDED__

#include "address.hpp"
#include "config.hpp"
#include "connection.hpp"
#include "request_callback.hpp"
#include "host.hpp"
#include "load_balancing.hpp"
#include "macros.hpp"
#include "response.hpp"
#include "scoped_ptr.hpp"
#include "token_map.hpp"

#include <stdint.h>

namespace cass {

class Connector;
class ChainedControlRequestCallback;
class ControlRequestCallback;
class EventResponse;
class Request;
class Row;
class Session;
class Timer;
class Value;

class ControlConnection : public ConnectionListener {
public:
  static bool determine_address_for_peer_host(const Address& connected_address,
                                              const Value* peer_value,
                                              const Value* rpc_value,
                                              Address* output);

  enum State {
    CONTROL_STATE_NEW,
    CONTROL_STATE_READY,
    CONTROL_STATE_CLOSED
  };

  ControlConnection();
  virtual ~ControlConnection() {}

  int protocol_version() const {
    return protocol_version_;
  }

  const VersionNumber& cassandra_version() const {
    return cassandra_version_;
  }

  const Host::Ptr& connected_host() const;

  void clear();

  void connect(Session* session);
  void close();

  void on_up(const Address& address);
  void on_down(const Address& address);

private:
  friend class ControlRequestCallback;
  friend class ChainedControlRequestCallback;

  enum UpdateHostType {
    ADD_HOST,
    UPDATE_HOST_AND_BUILD
  };

  void schedule_reconnect(uint64_t ms = 0);
  void reconnect(bool retry_current_host);

  // Connection listener methods
  virtual void on_close(Connection* connection);
  virtual void on_event(const EventResponse* response);

  static void on_connect(Connector* connector);
  void handle_connect(Connector* connector);

  static void on_reconnect(Timer* timer);

  bool handle_query_invalid_response(const Response* response);
  void handle_query_failure(CassError code, const String& message);
  void handle_query_timeout();

  void query_meta_hosts();
  static void on_query_hosts(ChainedControlRequestCallback* callback);

  void query_meta_schema();
  static void on_query_meta_schema(ChainedControlRequestCallback* callback);

  void refresh_node_info(Host::Ptr host,
                         bool is_new_node,
                         bool query_tokens = false);
  static void on_refresh_node_info(ControlRequestCallback* callback);
  static void on_refresh_node_info_all(ControlRequestCallback* callback);

  void update_node_info(Host::Ptr host, const Row* row, UpdateHostType type);

  void refresh_keyspace(const StringRef& keyspace_name);
  static void on_refresh_keyspace(ControlRequestCallback* callback);

  void refresh_table_or_view(const StringRef& keyspace_name,
                     const StringRef& table_name);
  static void on_refresh_table_or_view(ChainedControlRequestCallback* callback);

  void refresh_type(const StringRef& keyspace_name,
                    const StringRef& type_name);
  static void on_refresh_type(ControlRequestCallback* callback);

  void refresh_function(const StringRef& keyspace_name,
                        const StringRef& function_name,
                        const StringRefVec& arg_types,
                        bool is_aggregate);
  static void on_refresh_function(ControlRequestCallback* callback);

private:
  State state_;
  Session* session_;
  Connection* connection_;
  int event_types_;
  Timer reconnect_timer_;
  ScopedPtr<QueryPlan> query_plan_;
  Host::Ptr current_host_;
  int protocol_version_;
  VersionNumber cassandra_version_;
  String last_connection_error_;
  bool use_schema_;
  bool token_aware_routing_;

private:
  DISALLOW_COPY_AND_ASSIGN(ControlConnection);
};

} // namespace cass

#endif
