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

void cass_session_free(CassSession* session) {
  delete session->from();
}

CassFuture* cass_session_close(CassSession* session) {
  // TODO(mpenick): Make sure this handles close during the middle of a connect
  cass::SessionCloseFuture* close_future = new cass::SessionCloseFuture();
  session->close_async(close_future);
  return CassFuture::to(close_future);
}

CassFuture* cass_session_prepare(CassSession* session, CassString statement) {
  return CassFuture::to(session->prepare(statement.data, statement.length));
}

CassFuture* cass_session_execute(CassSession* session,
                                 CassStatement* statement) {
  return CassFuture::to(session->execute(statement->from()));
}

CassFuture* cass_session_execute_batch(CassSession* session, CassBatch* batch) {
  return CassFuture::to(session->execute(batch->from()));
}

} // extern "C"

namespace cass {

Session::Session(const Config& config)
    : ssl_context_(nullptr)
    , connect_future_(nullptr)
    , close_future_(nullptr)
    , config_(config)
    , load_balancing_policy_(new RoundRobinPolicy())
    , pending_resolve_count_(0)
    , pending_pool_count_(0)
    , pending_workers_count_(0)
    , current_io_worker_(0)
    , is_closing_(false) {
}

Session::~Session() {
  if (connect_future_ != nullptr) {
    connect_future_->release();
  }
}

int Session::init() {
  int rc = EventThread::init(config_.queue_size_event());
  if (rc != 0) return rc;
  request_queue_.reset(
      new AsyncQueue<MPMCQueue<RequestHandler*>>(config_.queue_size_io()));
  rc = request_queue_->init(loop(), this, &Session::on_execute);
  if (rc != 0) return rc;
  for (size_t i = 0; i < config_.thread_count_io(); ++i) {
    IOWorkerPtr io_worker(new IOWorker(this, logger_.get(), config_));
    int rc = io_worker->init();
    if (rc != 0) return rc;
    io_workers_.push_back(io_worker);
  }
  return rc;
}

bool Session::notify_ready_async() {
  SessionEvent event;
  event.type = SessionEvent::NOTIFY_READY;
  return send_event_async(event);
}

bool Session::notify_closed_async() {
  SessionEvent event;
  event.type = SessionEvent::NOTIFY_CLOSED;
  return send_event_async(event);
}

bool Session::connect_async(const std::string& keyspace, Future* future) {
  logger_.reset(new Logger(config_));
  if (logger_->init() != 0 || init() != 0) {
    return false;
  }

  SessionEvent event;
  event.type = SessionEvent::CONNECT;
  if (!send_event_async(event)) {
    return false;
  }

  if (!keyspace.empty()) {
    set_keyspace(keyspace);
  }

  connect_future_ = future;
  connect_future_->retain();

  run();

  return true;
}

void Session::close_async(Future* future) {
  close_future_ = future;
  close_future_->retain();

  is_closing_ = true;
  while (!request_queue_->enqueue(nullptr)) {
    // Keep trying
  }
}

void Session::init_pools() {
  pending_pool_count_ = hosts_.size() * io_workers_.size();
  for (auto& host : hosts_) {
    for (auto io_worker : io_workers_) {
      io_worker->add_pool_async(host);
    }
  }
}

void Session::close_handles() {
  EventThread::close_handles();
  request_queue_->close_handles();
}

void Session::on_run() {
  if (config_.log_level() != CASS_LOG_DISABLED) {
    logger_->run();
  }
  for (auto io_worker : io_workers_) {
    io_worker->run();
  }
}

void Session::on_after_run() {
  for (auto io_worker : io_workers_) {
    io_worker->join();
  }
  logger_->join();

  Future* close_future = close_future_;

  // Let the session thread handle the final cleanup of the session. At this
  // point we know that no other code is using this object.
  delete this;

  if (close_future) {
    close_future->set();
  }
}

void Session::on_event(const SessionEvent& event) {
  if (event.type == SessionEvent::CONNECT) {
    int port = config_.port();
    for (std::string seed : config_.contact_points()) {
      Address address;
      if (Address::from_string(seed, port, &address)) {
        Host host(address);
        hosts_.insert(host);
      } else {
        pending_resolve_count_++;
        Resolver::resolve(loop(), seed, port, this, on_resolve);
      }
    }
    if (pending_resolve_count_ == 0) {
      if (hosts_.empty()) {
        connect_future_->set_error(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE,
                                   "No hosts provided or no hosts resolved");
        connect_future_ = nullptr;
      } else {
        init_pools();
      }
    }
  } else if (event.type == SessionEvent::NOTIFY_READY) {
    if (--pending_pool_count_ == 0) {
      logger_->debug("Session is connected");
      load_balancing_policy_->init(hosts_);
      connect_future_->set();
      connect_future_ = nullptr;
    }
    logger_->debug("Session pending pool count %d", pending_pool_count_);
  } else if (event.type == SessionEvent::NOTIFY_CLOSED) {
    if (--pending_workers_count_ == 0) {
      logger_->close_async();
      close_handles();
    }
  }
}

void Session::on_resolve(Resolver* resolver) {
  Session* session = reinterpret_cast<Session*>(resolver->data());
  if (resolver->is_success()) {
    Host host(resolver->address());
    session->hosts_.insert(host);
    if (--session->pending_resolve_count_ == 0) {
      session->init_pools();
    }
  } else {
    session->logger_->error("Unable to resolve %s:%d\n",
                            resolver->host().c_str(), resolver->port());
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
  RequestHandler* request_handler = new RequestHandler(request);
  ResponseFuture* future = request_handler->future();
  future->statement.assign(statement, length);
  execute(request_handler);
  return future;
}

Future* Session::execute(MessageBody* statement) {
  Message* request = new Message();
  request->opcode = statement->opcode();
  request->body.reset(statement);
  RequestHandler* request_handler = new RequestHandler(request);
  Future* future = request_handler->future();
  execute(request_handler);
  return future;
}

void Session::on_execute(uv_async_t* data, int status) {
  Session* session = reinterpret_cast<Session*>(data->data);

  RequestHandler* request_handler = nullptr;
  while (session->request_queue_->dequeue(request_handler)) {
    if (request_handler != nullptr) {
      request_handler->keyspace = session->keyspace();
      session->load_balancing_policy_->new_query_plan(&request_handler->hosts);

      size_t start = session->current_io_worker_;
      size_t remaining = session->io_workers_.size();
      const size_t size = session->io_workers_.size();
      while (remaining != 0) {
        // TODO(mpenick): Make this something better than RR
        auto io_worker = session->io_workers_[start % size];
        if (io_worker->execute(request_handler)) {
          session->current_io_worker_ = (start + 1) % size;
          break;
        }
        start++;
        remaining--;
      }

      if (remaining == 0) {
        request_handler->on_error(CASS_ERROR_LIB_NO_AVAILABLE_IO_THREAD,
                                  "All workers are busy");
      }
    }
  }

  if (session->is_closing_) {
    session->pending_workers_count_ = session->io_workers_.size();
    for (auto io_worker : session->io_workers_) {
      io_worker->close_async();
    }
  }
}

} // namespace cass
