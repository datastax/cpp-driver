/*
  Copyright 2014 DataStax

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
#include "handler.hpp"
#include "host.hpp"
#include "load_balancing.hpp"
#include "macros.hpp"
#include "multiple_request_handler.hpp"
#include "response.hpp"
#include "scoped_ptr.hpp"

namespace cass {

class EventResponse;
class Logger;
class Request;
class Row;
class Session;
class Timer;
class Value;

class ControlConnection {
public:
  static bool determine_address_for_peer_host(Logger* logger,
                                              const Address& connected_address,
                                              const Value* peer_value,
                                              const Value* rpc_value,
                                              Address* output);

  enum ControlState {
    CONTROL_STATE_NEW,
    CONTROL_STATE_READY,
    CONTROL_STATE_CLOSED
  };

  ControlConnection()
    : session_(NULL)
    , state_(CONTROL_STATE_NEW)
    , connection_(NULL)
    , reconnect_timer_(NULL) {}


  int protocol_version() const {
    return protocol_version_;
  }

  void connect(Session* session);
  void close();

  void on_up(const Address& address);
  void on_down(const Address& address, bool is_critical_failure);

private:
  class ControlMultipleRequestHandler : public MultipleRequestHandler {
  public:
    typedef boost::function1<void, const MultipleRequestHandler::ResponseVec&> ResponseCallback;

    ControlMultipleRequestHandler(ControlConnection* control_connection,
                                  ResponseCallback response_callback)
        : MultipleRequestHandler(control_connection->connection_)
        , control_connection_(control_connection)
        , response_callback_(response_callback) {}

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
  };

  template<class T>
  class ControlHandler : public Handler {
  public:
    typedef boost::function2<void, T, Response*> ResponseCallback;

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
      if(control_connection_->handle_query_invalid_response(response_body)) {
        return;
      }
      response_callback_(data_, response_body);
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

  typedef boost::function1<void, SharedRefPtr<Host> > RefreshNodeCallback;

  struct RefreshNodeData {
    RefreshNodeData(const SharedRefPtr<Host>& host,
                    RefreshNodeCallback callback)
      : host(host)
      , callback(callback) {}
    SharedRefPtr<Host> host;
    RefreshNodeCallback callback;
  };

  void schedule_reconnect(uint64_t ms = 0);
  void reconnect(bool retry_current_host);

  void on_connection_ready(Connection* connection);
  //TODO: possibly reorder callback functions to pair with initiator
  void on_query_meta_all(const MultipleRequestHandler::ResponseVec& responses);
  void on_refresh_node_info(RefreshNodeData data, Response* response);
  void on_refresh_node_info_all(RefreshNodeData data, Response* response);
  void on_local_query(ResponseMessage* response);
  void on_peer_query(ResponseMessage* response);
  void on_connection_closed(Connection* connection);
  void on_connection_event(EventResponse* response);
  void on_reconnect(Timer* timer);

  bool handle_query_invalid_response(Response* response);
  void handle_query_failure(CassError code, const std::string& message);
  void handle_query_timeout();

  void query_meta_all();
  void refresh_node_info(SharedRefPtr<Host> host, RefreshNodeCallback callback);
  void update_node_info(SharedRefPtr<Host> host, const Row* row);

private:
  Session* session_;
  ControlState state_;
  Connection* connection_;
  ScopedPtr<QueryPlan> query_plan_;
  Address current_host_address_;
  int protocol_version_;
  Timer* reconnect_timer_;
  Logger* logger_;
  std::string last_connection_error_;

  static Address bind_any_ipv4_;
  static Address bind_any_ipv6_;

private:
  DISALLOW_COPY_AND_ASSIGN(ControlConnection);
};

} // namespace cass

#endif
