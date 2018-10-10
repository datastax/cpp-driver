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

#include "connection.hpp"

#include "event_response.hpp"
#include "options_request.hpp"
#include "request.hpp"
#include "result_response.hpp"

namespace cass {

/**
 * A request callback that handles heartbeats.
 */
class HeartbeatCallback : public SimpleRequestCallback {
public:
  HeartbeatCallback(Connection* connection);

private:
  virtual void on_internal_set(ResponseMessage* response);
  virtual void on_internal_error(CassError code, const String& message);
  virtual void on_internal_timeout();

private:
  Connection* connection_;
};

HeartbeatCallback::HeartbeatCallback(Connection* connection)
  : SimpleRequestCallback(Request::ConstPtr(Memory::allocate<OptionsRequest>()))
  , connection_(connection) { }

void HeartbeatCallback::on_internal_set(ResponseMessage* response) {
  LOG_TRACE("Heartbeat completed on host %s",
            connection_->socket_->address_string().c_str());
  connection_->heartbeat_outstanding_ = false;
}

void HeartbeatCallback::on_internal_error(CassError code, const String& message) {
  LOG_WARN("An error occurred on host %s during a heartbeat request: %s",
           connection_->socket_->address_string().c_str(),
           message.c_str());
  connection_->heartbeat_outstanding_ = false;
}

void HeartbeatCallback::on_internal_timeout() {
  LOG_WARN("Heartbeat request timed out on host %s",
           connection_->socket_->address_string().c_str());
  connection_->heartbeat_outstanding_ = false;
}

/**
 * A no operation connection listener. This is used if a listener is not set.
 */
class NopConnectionListener : public ConnectionListener {
  virtual void on_close(Connection* connection) { }
};

static NopConnectionListener nop_listener__;

void ConnectionHandler::on_read(Socket* socket, ssize_t nread, const uv_buf_t* buf) {
  connection_->on_read(buf->base, nread);
  free_buffer(buf);
}

void ConnectionHandler::on_write(Socket* socket, int status, SocketRequest* request) {
  connection_->on_write(status, static_cast<RequestCallback*>(request));
}

void ConnectionHandler::on_close() {
  connection_->on_close();
}

void SslConnectionHandler::on_ssl_read(Socket* socket, char* buf, size_t size) {
  connection_->on_read(buf, size);
}

void SslConnectionHandler::on_write(Socket* socket, int status, SocketRequest* request) {
  connection_->on_write(status, static_cast<RequestCallback*>(request));
}

void SslConnectionHandler::on_close() {
  connection_->on_close();
}

void RecordingConnectionListener::process_events(const EventResponse::Vec& events,
                                                 ConnectionListener* listener) {
  for (EventResponse::Vec::const_iterator it = events.begin(),
       end = events.end(); it != end; ++it) {
    listener->on_event(*it);
  }
}

Connection::Connection(const Socket::Ptr& socket,
                       int protocol_version,
                       unsigned int idle_timeout_secs,
                       unsigned int heartbeat_interval_secs)
  : socket_(socket)
  , inflight_request_count_(0)
  , response_(Memory::allocate<ResponseMessage>())
  , listener_(&nop_listener__)
  , protocol_version_(protocol_version)
  , idle_timeout_secs_(idle_timeout_secs)
  , heartbeat_interval_secs_(heartbeat_interval_secs)
  , heartbeat_outstanding_(false) {
  inc_ref(); // For the event loop
}

int32_t Connection::write(const RequestCallback::Ptr& callback) {
  int stream = stream_manager_.acquire(callback);
  if (stream < 0) {
    return Request::REQUEST_ERROR_NO_AVAILABLE_STREAM_IDS;
  }

  callback->notify_write(this, stream);

  int32_t request_size = socket_->write(callback.get());

  if (request_size < 0) {
    stream_manager_.release(stream);

    switch (request_size) {
      case SocketRequest::SOCKET_REQUEST_ERROR_CLOSED:
        callback->on_error(CASS_ERROR_LIB_WRITE_ERROR,
                           "Unable to write to close socket");
        break;

      case SocketRequest::SOCKET_REQUEST_ERROR_NO_HANDLER:
        callback->on_error(CASS_ERROR_LIB_WRITE_ERROR,
                           "Socket is not properly configured with a handler");
        break;

      case Request::REQUEST_ERROR_BATCH_WITH_NAMED_VALUES:
      case Request::REQUEST_ERROR_PARAMETER_UNSET:
        // Already handled with a specific error.
        break;

      case Request::REQUEST_ERROR_UNSUPPORTED_PROTOCOL:
        callback->on_error(CASS_ERROR_LIB_MESSAGE_ENCODE,
                           "Operation unsupported by this protocol version");
        break;

      default:
        callback->on_error(CASS_ERROR_LIB_WRITE_ERROR,
                           "Unspecified write error occurred");
        break;
    }

    return request_size;
  }

  // Add to the inflight count after we've cleared all posssible errors.
  inflight_request_count_.fetch_add(1);

  LOG_TRACE("Sending message type %s with stream %d on host %s",
            opcode_to_string(callback->request()->opcode()).c_str(),
            stream,
            socket_->address_string().c_str());

  callback->set_state(RequestCallback::REQUEST_STATE_WRITING);

  return request_size;
}

int32_t Connection::write_and_flush(const RequestCallback::Ptr& callback) {
  int32_t result = write(callback);
  if (result > 0) {
    flush();
  }
  return result;
}

size_t Connection::flush() {
  return socket_->flush();
}

void Connection::close() {
  socket_->close();
}

void Connection::defunct() {
  socket_->defunct();
}

void Connection::set_listener(ConnectionListener* listener) {
  listener_ = listener ? listener : &nop_listener__;
}

void Connection::start_heartbeats() {
  restart_heartbeat_timer();
  restart_terminate_timer();
}

void Connection::maybe_set_keyspace(ResponseMessage* response) {
  if (response->opcode() == CQL_OPCODE_RESULT) {
    ResultResponse* result =
        static_cast<ResultResponse*>(response->response_body().get());
    if (result->kind() == CASS_RESULT_KIND_SET_KEYSPACE) {
      keyspace_ = result->keyspace().to_string();
    }
  }
}

void Connection::on_write(int status, RequestCallback* request) {
  listener_->on_write();

  // A successful write means that a heartbeat doesn't need to be sent so the
  // timer can be reset.
  if (status == 0) {
    restart_heartbeat_timer();
  }

  // Keep alive after releasing from the stream manager.
  RequestCallback::Ptr callback(request);

  switch (callback->state()) {
    case RequestCallback::REQUEST_STATE_WRITING:
      if (status == 0) {
        callback->set_state(RequestCallback::REQUEST_STATE_READING);
        pending_reads_.add_to_back(request);
      } else {
        stream_manager_.release(callback->stream());
        inflight_request_count_.fetch_sub(1);
        callback->set_state(RequestCallback::REQUEST_STATE_FINISHED);
        callback->on_error(CASS_ERROR_LIB_WRITE_ERROR,
                           "Unable to write to socket");
      }
      break;

    case RequestCallback::REQUEST_STATE_READ_BEFORE_WRITE:
      stream_manager_.release(callback->stream());
      inflight_request_count_.fetch_sub(1);
      // The read callback happened before the write callback
      // returned. This is now responsible for finishing the request.
      callback->set_state(RequestCallback::REQUEST_STATE_FINISHED);
      // Use the response saved in the read callback
      maybe_set_keyspace(callback->read_before_write_response());
      callback->on_set(callback->read_before_write_response());
      break;

    default:
      assert(false && "Invalid request state after write finished");
      break;
  }
}

void Connection::on_read(const char* buf, size_t size) {
  listener_->on_read();

  const char* pos = buf;
  size_t remaining = size;

  // A successful read means the connection is still responsive
  restart_terminate_timer();

  while (remaining != 0 && !socket_->is_closing()) {
    ssize_t consumed = response_->decode(pos, remaining);
    if (consumed <= 0) {
      LOG_ERROR("Error decoding/consuming message");
      defunct();
      continue;
    }

    if (response_->is_body_ready()) {
      ScopedPtr<ResponseMessage> response(response_.release());
      response_.reset(Memory::allocate<ResponseMessage>());

      LOG_TRACE("Consumed message type %s with stream %d, input %u, remaining %u on host %s",
                opcode_to_string(response->opcode()).c_str(),
                static_cast<int>(response->stream()),
                static_cast<unsigned int>(size),
                static_cast<unsigned int>(remaining),
                socket_->address_string().c_str());

      if (response->stream() < 0) {
        if (response->opcode() == CQL_OPCODE_EVENT) {
          listener_->on_event(response->response_body());
        } else {
          LOG_ERROR("Invalid response opcode for event stream: %s",
                    opcode_to_string(response->opcode()).c_str());
          defunct();
          continue;
        }
      } else {
        RequestCallback::Ptr callback;

        if (stream_manager_.get(response->stream(), callback)) {
          switch (callback->state()) {
            case RequestCallback::REQUEST_STATE_READING:
              pending_reads_.remove(callback.get());
              stream_manager_.release(callback->stream());
              inflight_request_count_.fetch_sub(1);
              callback->set_state(RequestCallback::REQUEST_STATE_FINISHED);
              maybe_set_keyspace(response.get());
              callback->on_set(response.get());
              break;

            case RequestCallback::REQUEST_STATE_WRITING:
              // There are cases when the read callback will happen
              // before the write callback. If this happens we have
              // to allow the write callback to finish the request.
              callback->set_state(RequestCallback::REQUEST_STATE_READ_BEFORE_WRITE);
              // Save the response for the write callback
              callback->set_read_before_write_response(response.release()); // Transfer ownership
              break;

            default:
              assert(false && "Invalid request state after receiving response");
              break;
          }
        } else {
          LOG_ERROR("Invalid stream ID %d", response->stream());
          defunct();
          continue;
        }
      }
    }
    remaining -= consumed;
    pos += consumed;
  }
}

void Connection::on_close() {
  heartbeat_timer_.stop();
  terminate_timer_.stop();
  while (!pending_reads_.is_empty()) {
    pending_reads_.pop_front()->on_close();
  }
  listener_->on_close(this);
  dec_ref();
}

void Connection::restart_heartbeat_timer() {
  if (!is_closing() && heartbeat_interval_secs_ > 0) {
    heartbeat_timer_.start(socket_->loop(), 1000 * heartbeat_interval_secs_,
                           bind_callback(&Connection::on_heartbeat, this));
  }
}

void Connection::on_heartbeat(Timer* timer) {
  if (!heartbeat_outstanding_ && !socket_->is_closing()) {
    if (!write_and_flush(RequestCallback::Ptr(Memory::allocate<HeartbeatCallback>(this)))) {
      // Recycling only this connection with a timeout error. This is unlikely and
      // it means the connection ran out of stream IDs as a result of requests
      // that never returned and as a result timed out.
      LOG_ERROR("No streams IDs available for heartbeat request. "
                "Terminating connection...");
      defunct();
      return;
    }
    heartbeat_outstanding_ = true;
  }

  restart_heartbeat_timer();
}

void Connection::restart_terminate_timer() {
  // The terminate timer shouldn't be started without having heartbeats enabled,
  // otherwise connections would be terminated in periods of request inactivity.
  if (!is_closing() && heartbeat_interval_secs_ > 0 &&
      idle_timeout_secs_ > 0) {
    terminate_timer_.start(socket_->loop(), 1000 * idle_timeout_secs_,
                           bind_callback(&Connection::on_terminate, this));
  }
}

void Connection::on_terminate(Timer* timer) {
  LOG_ERROR("Failed to send a heartbeat within connection idle interval. "
            "Terminating connection...");
  defunct();
}

} // namespace cass
