/*
  Copyright (c) 2014 DataStax

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

#include "cassandra.hpp"
#include "session.hpp"

extern "C" {

CassSession* cass_session_new() {
  return CassSession::to(new cass::Session());
}

void cass_session_free(CassSession* session) {
  delete session->from();
}

CassError cass_session_setopt(CassSession* session,
                              CassOption option,
                              const void* data, size_t data_length) {
  return session->config().option(option, data, data_length);
}

CassError cass_session_getopt( const CassSession* session,
                               CassOption option,
                               void** data, size_t* data_length) {
  return CASS_OK;
}

CassFuture* cass_session_connect(CassSession* session) {
  return CassFuture::to(session->connect(""));
}

CassFuture* cass_session_connect_keyspace(CassSession* session,
                                          const char* keyspace) {
  return CassFuture::to(session->connect(keyspace));
}

CassFuture* cass_session_shutdown(CassSession* session) {
  // TODO(mpenick): Make sure this handles shutdown during the middle of startup
  return CassFuture::to(session->shutdown());
}


CassFuture* cass_session_prepare(CassSession* session,
                                 CassString statement) {
  return CassFuture::to(session->prepare(statement.data, statement.length));
}

void cass_session_execute(CassSession* session,
                          CassStatement* statement,
                          CassFuture** output) {
  cass::Future* future = session->execute(statement->from());
  if(output == nullptr) {
    future->release();
  } else {
    *output = CassFuture::to(future);
  }
}

void cass_session_execute_batch(CassSession* session,
                                CassBatch* batch,
                                CassFuture** output) {
  cass::Future* future = session->execute(batch->from());
  if(output == nullptr) {
    future->release();
  } else {
    *output = CassFuture::to(future);
  }
}

} // extern "C"

namespace cass {

Session::Session()
  : state_(SESSION_STATE_NEW)
  , ssl_context_(nullptr)
  , connect_future_(nullptr)
  , shutdown_future_(nullptr)
  , load_balancing_policy_(new RoundRobinPolicy())
  , pending_resolve_count_(0)
  , pending_connections_count_(0)
  , pending_workers_count_(0)
  , current_io_worker_(0) { }

int Session::init() {
  int rc = EventThread::init(config_.queue_size_event());
  if(rc != 0) return rc;
  request_queue_.reset(new AsyncQueue<MPMCQueue<RequestHandler*>>(config_.queue_size_io()));
  rc = request_queue_->init(loop(), this, &Session::on_execute);
  if(rc != 0) return rc;
  for (size_t i = 0; i < config_.thread_count_io(); ++i) {
    IOWorkerPtr io_worker(new IOWorker(this, logger_.get(), config_));
    int rc = io_worker->init();
    if(rc != 0) return rc;
    io_workers_.push_back(io_worker);
  }
  return rc;
}

void Session::join() {
  for(auto io_worker : io_workers_) {
    io_worker->join();
  }
  logger_->join();
  EventThread::join();
}

bool Session::notify_connect_async(const Host& host) {
  SessionEvent event;
  event.type = SessionEvent::NOTIFY_CONNECTED;
  event.host = host;
  return send_event_async(event);
}

bool Session::notify_shutdown_async() {
  SessionEvent event;
  event.type = SessionEvent::NOTIFY_SHUTDOWN;
  return send_event_async(event);
}

Future* Session::connect(const std::string& ks) {
  connect_future_ = new SessionFuture();
  connect_future_->session = this;

  SessionState expected_state = SESSION_STATE_NEW;
  if (state_.compare_exchange_strong(
        expected_state,
        SESSION_STATE_CONNECTING)) {

    logger_.reset(new Logger(config_));
    logger_->init();

    init();

    keyspace_ = ks;
    run();

    connect_async();
  } else {
    connect_future_->set_error(CASS_ERROR_LIB_ALREADY_CONNECTED, "Connect has already been called");
  }
  return connect_future_;
}


bool Session::connect_async() {
  SessionEvent event;
  event.type = SessionEvent::CONNECT;
  return send_event_async(event);
}

void Session::init_pools() {
  int num_pools =  hosts_.size() * io_workers_.size();
  pending_connections_count_ = num_pools * config_.core_connections_per_host();
  for(auto& host : hosts_) {
    for(auto io_worker : io_workers_) {
      io_worker->add_pool_async(host);
    }
  }
}

void Session::close() {
  EventThread::close();
  request_queue_->close();
}

Future*Session::shutdown() {
  shutdown_future_ = new ShutdownSessionFuture();
  shutdown_future_->session = this;

  SessionState expected_state_ready = SESSION_STATE_READY;
  SessionState expected_state_connecting = SESSION_STATE_CONNECTING;
  if (state_.compare_exchange_strong(expected_state_ready, SESSION_STATE_DISCONNECTING) ||
      state_.compare_exchange_strong(expected_state_connecting, SESSION_STATE_DISCONNECTING)) {
    pending_workers_count_ = io_workers_.size();
    for(auto io_worker : io_workers_) {
      io_worker->shutdown_async();
    }
  } else {
    shutdown_future_->set_error(CASS_ERROR_LIB_NOT_CONNECTED, "The session is not connected");
  }

  return shutdown_future_;
}

void Session::on_run() {
  if(config_.log_level() != CASS_LOG_DISABLED) {
    logger_->run();
  }
  for(auto io_worker : io_workers_) {
    io_worker->run();
  }
}

void Session::on_event(const SessionEvent& event) {
  if(event.type == SessionEvent::CONNECT) {
    int port = config_.port();
    for(std::string seed : config_.contact_points()) {
      Address address;
      if(Address::from_string(seed, port, &address)) {
        Host host(address);
        hosts_.insert(host);
      } else {
        pending_resolve_count_++;
        Resolver::resolve(loop(), seed, port, this, on_resolve);
      }
    }
    if(pending_resolve_count_ == 0) {
      init_pools();
    }
  } else if(event.type == SessionEvent::NOTIFY_CONNECTED) {
    if(--pending_connections_count_ == 0) {
      logger_->debug("Session is connected");
      load_balancing_policy_->init(hosts_);
      state_ = SESSION_STATE_READY;
      connect_future_->set_result();
      connect_future_ = nullptr;
    }
    logger_->debug("Session pending connection count %d", pending_connections_count_);
  } else if(event.type == SessionEvent::NOTIFY_SHUTDOWN) {
    if(--pending_workers_count_ == 0) {
      shutdown_future_->set_result();
      shutdown_future_ = nullptr;
      state_ = SESSION_STATE_DISCONNECTED;
      logger_->shutdown_async();
      close();
    }
  }
}

void Session::on_resolve(Resolver* resolver) {
  Session* session = reinterpret_cast<Session*>(resolver->data());
  if(resolver->is_success()) {
    Host host(resolver->address());
    session->hosts_.insert(host);
    if(--session->pending_resolve_count_ == 0) {
      session->init_pools();
    }
  } else {
    session->logger_->error("Unable to resolve %s:%d\n", resolver->host().c_str(), resolver->port());
  }
}

SSLSession* Session::ssl_session_new() {
  if (ssl_context_) {
    return ssl_context_->session_new();
  }
  return NULL;
}

Future* Session::prepare(const char* statement, size_t length) {
  Message* request = new Message();
  request->opcode = CQL_OPCODE_PREPARE;
  PrepareRequest* prepare = new PrepareRequest();
  prepare->prepare_string(statement, length);
  request->body.reset(prepare);
  RequestHandler* request_handler
      = new RequestHandler(request);
  request_handler->future()->statement.assign(statement, length);
  execute(request_handler);
  return request_handler->future();
}

Future* Session::execute(MessageBody* statement) {
  Message* request = new Message();
  request->opcode = statement->opcode();
  request->body.reset(statement);
  RequestHandler* request_handler = new RequestHandler(request);
  execute(request_handler);
  return request_handler->future();
}

void Session::on_execute(uv_async_t* data, int status) {
  Session* session = reinterpret_cast<Session*>(data->data);

  RequestHandler* request_handler = nullptr;
  while (session->request_queue_->dequeue(request_handler)) {
    session->load_balancing_policy_->new_query_plan(&request_handler->hosts);

    size_t start = session->current_io_worker_;
    size_t remaining = session->io_workers_.size();
    while(remaining != 0) {
      // TODO(mpenick): Make this something better than RR
      auto io_worker = session->io_workers_[start];
      if(io_worker->execute(request_handler)) {
        session->current_io_worker_ = (start + 1) % session->io_workers_.size();
        break;
      }
      start++;
      remaining--;
    }

    if(remaining == 0) {
      request_handler->on_error(CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD, "All workers are busy");
    }
  }
}

} // namespace cass
