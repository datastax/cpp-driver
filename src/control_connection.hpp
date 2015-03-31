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

#ifndef __CASS_CONTROL_CONNECTION_HPP_INCLUDED__
#define __CASS_CONTROL_CONNECTION_HPP_INCLUDED__

#include "address.hpp"
#include "connection.hpp"
#include "token_map.hpp"
#include "handler.hpp"
#include "host.hpp"
#include "load_balancing.hpp"
#include "macros.hpp"
#include "multiple_request_handler.hpp"
#include "response.hpp"
#include "scoped_ptr.hpp"

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

  const SharedRefPtr<Host> connected_host() const;

  void clear();

  void connect(Session* session);
  void close();

  void on_up(const Address& address);
  void on_down(const Address& address);

private:
  template<class T>
  class ControlMultipleRequestHandler : public MultipleRequestHandler {
  public:
    typedef void (*ResponseCallback)(ControlConnection*, const T&, const MultipleRequestHandler::ResponseVec&);

    ControlMultipleRequestHandler(ControlConnection* control_connection,
                                  ResponseCallback response_callback,
                                  const T& data)
        : MultipleRequestHandler(control_connection->connection_)
        , control_connection_(control_connection)
        , response_callback_(response_callback)
        , data_(data) {}

    virtual void on_set(const MultipleRequestHandler::ResponseVec& responses);

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
      , table_name(table_name) {}
    std::string keyspace_name;
    std::string table_name;
  };

  struct QueryMetadataAllData {};

  template<class T>
  class ControlHandler : public Handler {
  public:
    typedef void (*ResponseCallback)(ControlConnection*, const T&, Response*);

    ControlHandler(Request* request,
                   ControlConnection* control_connection,
                   ResponseCallback response_callback,
                   const T& data)
      : request_(request)
      , control_connection_(control_connection)
      , response_callback_(response_callback)
      , data_(data) {}

    const Request* request() const {
      return request_.get();
    }

    virtual void on_set(ResponseMessage* response) {
      Response* response_body = response->response_body().get();
      if (control_connection_->handle_query_invalid_response(response_body)) {
        return;
      }
      response_callback_(control_connection_, data_, response_body);
    }

    virtual void on_error(CassError code, const std::string& message) {
      control_connection_->handle_query_failure(code, message);
    }

    virtual void on_timeout() {
      control_connection_->handle_query_timeout();
    }

  private:
    ScopedRefPtr<Request> request_;
    ControlConnection* control_connection_;
    ResponseCallback response_callback_;
    T data_;
  };

  struct RefreshNodeData {
    RefreshNodeData(const SharedRefPtr<Host>& host,
                    bool is_new_node)
      : host(host)
      , is_new_node(is_new_node) {}
    SharedRefPtr<Host> host;
    bool is_new_node;
  };

  void schedule_reconnect(uint64_t ms = 0);
  void reconnect(bool retry_current_host);

  // Connection listener methods
  virtual void on_ready(Connection* connection);
  virtual void on_close(Connection* connection);
  virtual void on_availability_change(Connection* connection) {}
  virtual void on_event(EventResponse* response);

  //TODO: possibly reorder callback functions to pair with initiator
  static void on_query_meta_all(ControlConnection* control_connection,
                                const QueryMetadataAllData& data,
                                const MultipleRequestHandler::ResponseVec& responses);
  static void on_refresh_node_info(ControlConnection* control_connection,
                                   const RefreshNodeData& data,
                                   Response* response);
  static void on_refresh_node_info_all(ControlConnection* control_connection,
                                       const RefreshNodeData& data,
                                       Response* response);
  void on_local_query(ResponseMessage* response);
  void on_peer_query(ResponseMessage* response);
  static void on_reconnect(Timer* timer);

  bool handle_query_invalid_response(Response* response);
  void handle_query_failure(CassError code, const std::string& message);
  void handle_query_timeout();

  void query_meta_all();
  void refresh_node_info(SharedRefPtr<Host> host,
                         bool is_new_node,
                         bool query_tokens = false);
  void update_node_info(SharedRefPtr<Host> host, const Row* row);

  void refresh_keyspace(const StringRef& keyspace_name);
  static void on_refresh_keyspace(ControlConnection* control_connection, const std::string& keyspace_name, Response* response);

  void refresh_table(const StringRef& keyspace_name,
                     const StringRef& table_name);
  static void on_refresh_table(ControlConnection* control_connection,
                               const RefreshTableData& data,
                               const MultipleRequestHandler::ResponseVec& responses);

private:
  State state_;
  Session* session_;
  Connection* connection_;
  Timer* reconnect_timer_;
  ScopedPtr<QueryPlan> query_plan_;
  Address current_host_address_;
  int protocol_version_;
  std::string last_connection_error_;
  bool query_tokens_;

  static Address bind_any_ipv4_;
  static Address bind_any_ipv6_;

private:
  DISALLOW_COPY_AND_ASSIGN(ControlConnection);
};

} // namespace cass

#endif
