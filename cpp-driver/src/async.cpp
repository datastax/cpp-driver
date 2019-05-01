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

#include "async.hpp"

using namespace datastax::internal::core;

Async::Async()
    : handle_(NULL) {}

Async::~Async() { close_handle(); }

int Async::start(uv_loop_t* loop, const Async::Callback& callback) {
  if (handle_ == NULL) {
    handle_ = new AllocatedT<uv_async_t>();
    handle_->data = this;
    int rc = uv_async_init(loop, handle_, on_async);
    if (rc != 0) return rc;
  }
  callback_ = callback;
  return 0;
}

void Async::send() {
  if (handle_ == NULL) return;
  uv_async_send(handle_);
}

void Async::close_handle() {
  if (handle_ == NULL) return;
  uv_close(reinterpret_cast<uv_handle_t*>(handle_), on_close);
  handle_ = NULL;
}

bool Async::is_running() const {
  if (handle_ == NULL) return false;
  return uv_is_active(reinterpret_cast<uv_handle_t*>(handle_)) != 0;
}

void Async::on_async(uv_async_t* handle) {
  Async* async = static_cast<Async*>(handle->data);
  async->callback_(async);
}

void Async::on_close(uv_handle_t* handle) {
  delete reinterpret_cast<AllocatedT<uv_async_t>*>(handle);
}
