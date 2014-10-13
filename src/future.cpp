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

#include "future.hpp"

#include "request_handler.hpp"
#include "scoped_ptr.hpp"
#include "types.hpp"

extern "C" {

void cass_future_free(CassFuture* future) {
  // Futures can be deleted without being waited on
  // because they'll be cleaned up by the notifying thread
  future->dec_ref();
}

CassError cass_future_set_callback(CassFuture* future,
                                   CassFutureCallback callback,
                                   void* data) {
  if (!future->set_callback(callback, data)) {
    return CASS_ERROR_LIB_CALLBACK_ALREADY_SET;
  }
  return CASS_OK;
}

cass_bool_t cass_future_ready(CassFuture* future) {
  return static_cast<cass_bool_t>(future->ready());
}

void cass_future_wait(CassFuture* future) {
  future->wait();
}

cass_bool_t cass_future_wait_timed(CassFuture* future, cass_duration_t wait_us) {
  return static_cast<cass_bool_t>(future->wait_for(wait_us));
}

CassSession* cass_future_get_session(CassFuture* future) {
  if (future->type() != cass::CASS_FUTURE_TYPE_SESSION_CONNECT) {
    return NULL;
  }
  cass::SessionConnectFuture* connect_future =
      static_cast<cass::SessionConnectFuture*>(future->from());
  if (connect_future->is_error()) {
    return NULL;
  }
  return CassSession::to(connect_future->release_result());
}

const CassResult* cass_future_get_result(CassFuture* future) {
  if (future->type() != cass::CASS_FUTURE_TYPE_RESPONSE) {
    return NULL;
  }
  cass::ResponseFuture* response_future =
      static_cast<cass::ResponseFuture*>(future->from());
  if (response_future->is_error()) {
    return NULL;
  }

  cass::ResultResponse* result
      = static_cast<cass::ResultResponse*>(response_future->release_result());

  if (result != NULL) {
    result->decode_first_row();
  }

  return CassResult::to(result);
}

const CassPrepared* cass_future_get_prepared(CassFuture* future) {
  if (future->type() != cass::CASS_FUTURE_TYPE_RESPONSE) {
    return NULL;
  }
  cass::ResponseFuture* response_future =
      static_cast<cass::ResponseFuture*>(future->from());
  if (response_future->is_error()) {
    return NULL;
  }
  cass::ScopedPtr<cass::ResultResponse> result(
      static_cast<cass::ResultResponse*>(response_future->release_result()));
  if (result && result->kind() == CASS_RESULT_KIND_PREPARED) {
    cass::Prepared* prepared =
        new cass::Prepared(result.release(), response_future->statement);
    prepared->inc_ref();
    return CassPrepared::to(prepared);
  }
  return NULL;
}

CassError cass_future_error_code(CassFuture* future) {
  const cass::Future::Error* error = future->get_error();
  if (error != NULL) {
    return error->code;
  } else {
    return CASS_OK;
  }
}

CassString cass_future_error_message(CassFuture* future) {
  CassString str;
  const cass::Future::Error* error = future->get_error();
  if (error != NULL) {
    const std::string& message = error->message;
    str.data = message.data();
    str.length = message.size();
  } else {
    str.data = "";
    str.length = 0;
  }
  return str;
}

} // extern "C"

namespace cass {

bool Future::set_callback(Future::Callback callback, void* data) {
  ScopedMutex lock(&mutex_);
  if (callback_) {
    return false; // Callback is already set
  }
  callback_ = callback;
  data_ = data;
  if (is_set_) {
    // Run the callback if the future is already set
    lock.unlock();
    if (loop_ == NULL) {
      callback(CassFuture::to(this), data);
    } else if(callback_) {
      run_callback_on_work_thread();
    }
  }
  return true;
}

void Future::internal_set(ScopedMutex& lock) {
  is_set_ = true;
  uv_cond_broadcast(&cond_);
  if (callback_) {
    if (loop_ == NULL) {
      Callback callback = callback_;
      void* data = data_;
      lock.unlock();
      callback(CassFuture::to(this), data);
    } else {
      run_callback_on_work_thread();
    }
  }
}

void Future::run_callback_on_work_thread() {
  inc_ref(); // Keep the future alive for the callback
  work_.data = this;
  uv_queue_work(loop_, &work_, on_work, on_after_work);
}

void Future::on_work(uv_work_t* work) {
  Future* future = static_cast<Future*>(work->data);

  ScopedMutex lock(&future->mutex_);
  Callback callback = future->callback_;
  void* data = future->data_;
  lock.unlock();

  callback(CassFuture::to(future), data);
}

void Future::on_after_work(uv_work_t* work, int status) {
  Future* future = static_cast<Future*>(work->data);
  future->dec_ref();
}

} // namespace cass


