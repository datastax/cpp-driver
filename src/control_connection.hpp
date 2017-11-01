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

class EventResponse;
class Request;
class Row;
class Session;
class Timer;
class Value;

class ControlConnection : public Connection::Listener {
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
  template<class T>
  class ControlMultipleRequestCallback : public MultipleRequestCallback {
  public:
    typedef void (*ResponseCallback)(ControlConnection*, const T&, const MultipleRequestCallback::ResponseMap&);

    ControlMultipleRequestCallback(ControlConnection* control_connection,
                                  ResponseCallback response_callback,
                                  const T& data)
        : MultipleRequestCallback(control_connection->connection_)
        , control_connection_(control_connection)
        , response_callback_(response_callback)
        , data_(data) {}

    void execute_query(const std::string& index, const std::string& query);

    virtual void on_set(const MultipleRequestCallback::ResponseMap& responses);

    virtual void on_error(CassError code, const std::string& message) {
      control_connection_->handle_query_failure(code, message);
    }

    virtual void on_timeout() {
      control_connection_->handle_query_timeout();
    }

  private:
    ControlConnection* control_connection_;
    ResponseCallback response_callback_;
    T data_;
  };

  struct RefreshTableData {
    RefreshTableData(const std::string& keyspace_name,
                     const std::string& table_name)
      : keyspace_name(keyspace_name)
      , table_or_view_name(table_name) {}
    std::string keyspace_name;
    std::string table_or_view_name;
  };

  struct UnusedData {};

  template<class T>
  class ControlCallback : public SimpleRequestCallback {
  public:
    typedef void (*ResponseCallback)(ControlConnection*, const T&, Response*);

    ControlCallback(const Request::ConstPtr& request,
                    ControlConnection* control_connection,
                    ResponseCallback response_callback,
                    const T& data)
      : SimpleRequestCallback(request)
      , control_connection_(control_connection)
      , response_callback_(response_callback)
      , data_(data) { }

  private:
    virtual void on_internal_set(ResponseMessage* response) {
      Response* response_body = response->response_body().get();
      if (control_connection_->handle_query_invalid_response(response_body)) {
        return;
      }
      response_callback_(control_connection_, data_, response_body);
    }

    virtual void on_internal_error(CassError code, const std::string& message) {
      control_connection_->handle_query_failure(code, message);
    }

    virtual void on_internal_timeout() {
      control_connection_->handle_query_timeout();
    }

  private:
    ControlConnection* control_connection_;
    ResponseCallback response_callback_;
    T data_;
  };

  struct RefreshNodeData {
    RefreshNodeData(const Host::Ptr& host,
                    bool is_new_node)
      : host(host)
      , is_new_node(is_new_node) {}
    Host::Ptr host;
    bool is_new_node;
  };

  struct RefreshFunctionData {
    typedef std::vector<std::string> StringVec;

    RefreshFunctionData(StringRef keyspace,
                        StringRef function,
                        const StringRefVec& arg_types,
                        bool is_aggregate)
      : keyspace(keyspace.to_string())
      , function(function.to_string())
      , arg_types(to_strings(arg_types))
      , is_aggregate(is_aggregate) { }

    std::string keyspace;
    std::string function;
    StringVec arg_types;
    bool is_aggregate;
  };

  enum UpdateHostType {
    ADD_HOST,
    UPDATE_HOST_AND_BUILD
  };

  void schedule_reconnect(uint64_t ms = 0);
  void reconnect(bool retry_current_host);

  // Connection listener methods
  virtual void on_ready(Connection* connection);
  virtual void on_close(Connection* connection);
  virtual void on_event(EventResponse* response);

  static void on_reconnect(Timer* timer);

  bool handle_query_invalid_response(Response* response);
  void handle_query_failure(CassError code, const std::string& message);
  void handle_query_timeout();

  void query_meta_hosts();
  static void on_query_hosts(ControlConnection* control_connection,
                             const UnusedData& data,
                             const MultipleRequestCallback::ResponseMap& responses);

  void query_meta_schema();
  static void on_query_meta_schema(ControlConnection* control_connection,
                                const UnusedData& data,
                                const MultipleRequestCallback::ResponseMap& responses);

  void refresh_node_info(Host::Ptr host,
                         bool is_new_node,
                         bool query_tokens = false);
  static void on_refresh_node_info(ControlConnection* control_connection,
                                   const RefreshNodeData& data,
                                   Response* response);
  static void on_refresh_node_info_all(ControlConnection* control_connection,
                                       const RefreshNodeData& data,
                                       Response* response);

  void update_node_info(Host::Ptr host, const Row* row, UpdateHostType type);

  void refresh_keyspace(const StringRef& keyspace_name);
  static void on_refresh_keyspace(ControlConnection* control_connection, const std::string& keyspace_name, Response* response);

  void refresh_table_or_view(const StringRef& keyspace_name,
                     const StringRef& table_name);
  static void on_refresh_table_or_view(ControlConnection* control_connection,
                               const RefreshTableData& data,
                               const MultipleRequestCallback::ResponseMap& responses);

  void refresh_type(const StringRef& keyspace_name,
                    const StringRef& type_name);
  static void on_refresh_type(ControlConnection* control_connection,
                              const std::pair<std::string, std::string>& keyspace_and_type_names,
                              Response* response);

  void refresh_function(const StringRef& keyspace_name,
                        const StringRef& function_name,
                        const StringRefVec& arg_types,
                        bool is_aggregate);
  static void on_refresh_function(ControlConnection* control_connection,
                                  const RefreshFunctionData& data,
                                  Response* response);

private:
  State state_;
  Session* session_;
  Connection* connection_;
  Timer reconnect_timer_;
  ScopedPtr<QueryPlan> query_plan_;
  Host::Ptr current_host_;
  int protocol_version_;
  VersionNumber cassandra_version_;
  std::string last_connection_error_;
  bool use_schema_;
  bool token_aware_routing_;

private:
  DISALLOW_COPY_AND_ASSIGN(ControlConnection);
};

} // namespace cass

#endif
