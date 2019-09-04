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

#ifndef DATASTAX_INTERNAL_NAME_RESOLVER_HPP
#define DATASTAX_INTERNAL_NAME_RESOLVER_HPP

#include "address.hpp"
#include "callback.hpp"
#include "ref_counted.hpp"
#include "string.hpp"
#include "timer.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace core {

class NameResolver : public RefCounted<NameResolver> {
public:
  typedef SharedRefPtr<NameResolver> Ptr;

  typedef internal::Callback<void, NameResolver*> Callback;

  enum Status {
    NEW,
    RESOLVING,
    FAILED_BAD_PARAM,
    FAILED_UNABLE_TO_RESOLVE,
    FAILED_TIMED_OUT,
    CANCELED,
    SUCCESS
  };

  NameResolver(const Address& address, const Callback& callback)
      : address_(address)
      , status_(NEW)
      , uv_status_(-1)
      , callback_(callback) {
    req_.data = this;
  }

  uv_loop_t* loop() { return req_.loop; }

  bool is_success() { return status_ == SUCCESS; }
  bool is_canceled() { return status_ == CANCELED; }
  bool is_timed_out() { return status_ == FAILED_TIMED_OUT; }
  Status status() { return status_; }
  int uv_status() { return uv_status_; }

  const Address& address() const { return address_; }
  const String& hostname() const { return hostname_; }
  const String& service() const { return service_; }

  void resolve(uv_loop_t* loop, uint64_t timeout, int flags = 0) {
    status_ = RESOLVING;

    inc_ref(); // For the event loop

    if (timeout > 0) {
      timer_.start(loop, timeout, bind_callback(&NameResolver::on_timeout, this));
    }

    Address::SocketStorage storage;
    int rc = uv_getnameinfo(loop, &req_, on_resolve, address_.to_sockaddr(&storage), flags);

    if (rc != 0) {
      status_ = FAILED_BAD_PARAM;
      timer_.stop();
      uv_status_ = rc;
      callback_(this);
      dec_ref();
    }
  }

  void cancel() {
    if (status_ == RESOLVING) {
      uv_cancel(reinterpret_cast<uv_req_t*>(&req_));
      timer_.stop();
      status_ = CANCELED;
    }
  }

private:
  static void on_resolve(uv_getnameinfo_t* req, int status, const char* hostname,
                         const char* service) {
    NameResolver* resolver = static_cast<NameResolver*>(req->data);

    if (resolver->status_ == RESOLVING) { // A timeout may have happened
      resolver->timer_.stop();

      if (status != 0) {
        resolver->status_ = FAILED_UNABLE_TO_RESOLVE;
      } else {
        if (hostname != NULL) {
          resolver->hostname_ = hostname;
        }
        if (service != NULL) {
          resolver->service_ = service;
        }
        resolver->status_ = SUCCESS;
      }
    }

    resolver->uv_status_ = status;
    resolver->callback_(resolver);
    resolver->dec_ref();
  }

  void on_timeout(Timer* timer) {
    status_ = FAILED_TIMED_OUT;
    uv_cancel(reinterpret_cast<uv_req_t*>(&req_));
  }

private:
  uv_getnameinfo_t req_;
  Timer timer_;
  Address address_;
  Status status_;
  int uv_status_;
  String hostname_;
  String service_;
  Callback callback_;
};

}}} // namespace datastax::internal::core

#endif
