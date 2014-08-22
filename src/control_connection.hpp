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

#include "connection.hpp"
#include "handler.hpp"
#include "load_balancing.hpp"
#include "macros.hpp"
#include "scoped_ptr.hpp"

#ifndef __CASS_CONTROL_CONNECTION_HPP_INCLUDED__
#define __CASS_CONTROL_CONNECTION_HPP_INCLUDED__

namespace cass {

class EventResponse;
class Request;
class ResponseMessage;
class Session;
class Timer;

class ControlConnection {
public:
  enum ControlState {
    CONTROL_STATE_NEW,
    CONTROL_STATE_CONNECTED,
    CONTROL_STATE_CLOSED
  };

  ControlConnection()
    : session_(NULL)
    , state_(CONTROL_STATE_NEW)
    , connection_(NULL)
    , reconnect_timer_(NULL) {}

  void set_session(Session* session) {
    session_ = session;
  }

  void connect();
  void close();

private:
  class ControlHandler : public Handler {
  public:
    typedef boost::function1<void, ResponseMessage*> ResponseCallback;

    ControlHandler(ControlConnection* control_connection,
                   Request* request,
                   ResponseCallback response_callback)
        : control_connection_(control_connection)
        , request_(request)
        , response_callback_(response_callback) {}

    const Request* request() const {
      return request_.get();
    }

    virtual void on_set(ResponseMessage* response);
    virtual void on_error(CassError code, const std::string& message);
    virtual void on_timeout();

  private:
    ControlConnection* control_connection_;
    ScopedRefPtr<Request> request_;
    ResponseCallback response_callback_;
  };

  void schedule_reconnect(uint64_t ms = 0);
  void reconnect();

  void on_connection_ready(Connection* connection);
  void on_connection_closed(Connection* connection);
  void on_connection_event(EventResponse* response);
  void on_reconnect(Timer* timer);

private:
  Session* session_;
  ControlState state_;
  Connection* connection_;
  ScopedPtr<QueryPlan> query_plan_;
  Timer* reconnect_timer_;

private:
  DISALLOW_COPY_AND_ASSIGN(ControlConnection);
};

} // namespace cass

#endif
