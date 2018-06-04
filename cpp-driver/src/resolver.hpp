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

#ifndef __CASS_RESOLVER_HPP_INCLUDED__
#define __CASS_RESOLVER_HPP_INCLUDED__

#include "address.hpp"
#include "ref_counted.hpp"
#include "string.hpp"
#include "timer.hpp"
#include "vector.hpp"

#include <uv.h>

#include <functional>

namespace cass {

class Resolver : public RefCounted<Resolver> {
public:
  typedef SharedRefPtr<Resolver> Ptr;
  typedef Vector<Ptr> Vec;
  typedef void (*Callback)(Resolver*);

  enum Status {
    NEW,
    RESOLVING,
    FAILED_BAD_PARAM,
    FAILED_UNSUPPORTED_ADDRESS_FAMILY,
    FAILED_UNABLE_TO_RESOLVE,
    FAILED_TIMED_OUT,
    CANCELED,
    SUCCESS
  };

  Resolver(const String& hostname, int port, void* data, Callback cb)
      : hostname_(hostname)
      , port_(port)
      , status_(NEW)
      , data_(data)
      , callback_(cb) {
    req_.data = this;
  }

  ~Resolver() { }

  const String& hostname() { return hostname_; }
  int port() { return port_; }

  bool is_canceled() { return status_ == CANCELED; }
  bool is_success() { return status_ == SUCCESS; }
  bool is_timed_out() { return status_ == FAILED_TIMED_OUT; }
  Status status() { return status_; }
  int uv_status() { return uv_status_; }

  const AddressVec& addresses() const { return addresses_; }
  void* data() { return data_; }

  void resolve(uv_loop_t* loop, uint64_t timeout, struct addrinfo* hints = NULL) {
    status_ = RESOLVING;

    inc_ref(); // For the event loop

    if (timeout > 0) {
      timer_.start(loop, timeout, this, on_timeout);
    }

    OStringStream ss;
    ss << port_;
    int rc = uv_getaddrinfo(loop, &req_, on_resolve, hostname_.c_str(),
                            ss.str().c_str(), hints);

    if (rc != 0) {
      status_ = FAILED_BAD_PARAM;
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
  static void on_resolve(uv_getaddrinfo_t* req, int status,
                         struct addrinfo* res) {
    Resolver* resolver = static_cast<Resolver*>(req->data);

    if (resolver->status_ == RESOLVING) { // A timeout may have happened
      resolver->timer_.stop();

      if (status != 0) {
        resolver->status_ = FAILED_UNABLE_TO_RESOLVE;
      } else if (!resolver->init_addresses(res)) {
        resolver->status_ = FAILED_UNSUPPORTED_ADDRESS_FAMILY;
      } else {
        resolver->status_ = SUCCESS;
      }
    }

    resolver->uv_status_ = status;
    resolver->callback_(resolver);
    resolver->dec_ref();
    uv_freeaddrinfo(res);
  }

  static void on_timeout(Timer* timer) {
    Resolver* resolver = static_cast<Resolver*>(timer->data());
    resolver->status_ = FAILED_TIMED_OUT;
    uv_cancel(reinterpret_cast<uv_req_t*>(&resolver->req_));
  }

private:
  bool init_addresses(struct addrinfo* res) {
    bool status = false;
    do {
      Address address;
      if (address.init(res->ai_addr)) {
        addresses_.push_back(address);
        status = true;
      }
      res = res->ai_next;
    } while(res);
    return status;
  }

private:
  uv_getaddrinfo_t req_;
  Timer timer_;
  String hostname_;
  int port_;
  Status status_;
  int uv_status_;
  AddressVec addresses_;
  void* data_;
  Callback callback_;

private:
  DISALLOW_COPY_AND_ASSIGN(Resolver);
};

class MultiResolver : public RefCounted<MultiResolver> {
public:
  typedef SharedRefPtr<MultiResolver> Ptr;
  typedef void (*Callback)(MultiResolver* resolver);

  MultiResolver(void* data,
                Callback callback)
    : remaining_(0)
    , data_(data)
    , callback_(callback) { }

  const Resolver::Vec& resolvers() { return resolvers_; }
  void* data() { return data_; }

  void resolve(uv_loop_t* loop,
               const String& host, int port,
               uint64_t timeout, struct addrinfo* hints = NULL) {
    inc_ref();
    Resolver::Ptr resolver(Memory::allocate<Resolver>(host, port, this, on_resolve));
    resolver->resolve(loop, timeout, hints);
    resolvers_.push_back(resolver);
    remaining_++;
  }

  void cancel() {
    for (Resolver::Vec::iterator it = resolvers_.begin(),
         end = resolvers_.end(); it != end; ++it) {
      (*it)->cancel();
    }
  }

private:
  static void on_resolve(Resolver* resolver) {
    MultiResolver* multi_resolver = static_cast<MultiResolver*>(resolver->data());
    multi_resolver->handle_resolve();
  }

  void handle_resolve() {
    remaining_--;
    if (remaining_ <= 0 && callback_) {
      callback_(this);
    }
    dec_ref();
  }

private:
  Resolver::Vec resolvers_;
  int remaining_;
  void* data_;
  Callback callback_;

private:
  DISALLOW_COPY_AND_ASSIGN(MultiResolver);
};

} // namespace cass

#endif
