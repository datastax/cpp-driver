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

#include <uv.h>

#include <functional>

namespace cass {

template <class T>
class Resolver {
  friend class Memory;

public:
  typedef void (*Callback)(Resolver*);

  enum Status {
    RESOLVING,
    FAILED_BAD_PARAM,
    FAILED_UNSUPPORTED_ADDRESS_FAMILY,
    FAILED_UNABLE_TO_RESOLVE,
    FAILED_TIMED_OUT,
    SUCCESS
  };

  const String& hostname() { return hostname_; }
  int port() { return port_; }
  bool is_success() { return status_ == SUCCESS; }
  bool is_timed_out() { return status_ == FAILED_TIMED_OUT; }
  Status status() { return status_; }
  const AddressVec& addresses() const { return addresses_; }
  T& data() { return data_; }

  static void resolve(uv_loop_t* loop, const String& hostname, int port,
                      const T& data, Callback cb, uint64_t timeout,
                      struct addrinfo* hints = NULL) {
    Resolver* resolver = Memory::allocate<Resolver>(hostname, port, data, cb);

    OStringStream ss;
    ss << port;

    if (timeout > 0) {
      resolver->timer_.start(loop, timeout, resolver, on_timeout);
    }

    int rc = uv_getaddrinfo(loop, &resolver->req_, on_resolve, hostname.c_str(),
                            ss.str().c_str(), hints);

    if (rc != 0) {
      resolver->status_ = FAILED_BAD_PARAM;
      resolver->cb_(resolver);
      Memory::deallocate(resolver);
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

    resolver->cb_(resolver);

    Memory::deallocate(resolver);
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
  Resolver(const String& hostname, int port, const T& data, Callback cb)
      : hostname_(hostname)
      , port_(port)
      , status_(RESOLVING)
      , data_(data)
      , cb_(cb) {
    req_.data = this;
  }

  ~Resolver() {}

  uv_getaddrinfo_t req_;
  Timer timer_;
  String hostname_;
  int port_;
  Status status_;
  AddressVec addresses_;
  T data_;
  Callback cb_;
};

template <class T>
class MultiResolver : public RefCounted<MultiResolver<T> > {
public:
  typedef SharedRefPtr<MultiResolver<T> > Ptr;

  typedef cass::Resolver<MultiResolver<T>*> Resolver;
  typedef void (*ResolveCallback)(Resolver* resolver);

  typedef void (*FinishedCallback)(MultiResolver<T>* resolver);

  MultiResolver(const T& data,
                ResolveCallback resolve_cb,
                FinishedCallback finished_cb)
    : data_(data)
    , resolve_cb_(resolve_cb)
    , finished_cb_(finished_cb) { }

  ~MultiResolver() {
    if (finished_cb_) finished_cb_(this);
  }

  T& data() { return data_; }

  void resolve(uv_loop_t* loop,
               const String& host, int port,
               uint64_t timeout, struct addrinfo* hints = NULL) {
    this->inc_ref();
    cass::Resolver<MultiResolver<T>*>::resolve(loop, host, port, this,
                                               on_resolve, timeout, hints);
  }

private:
  static void on_resolve(Resolver* resolver) {
    MultiResolver<T>* multi_resolver = resolver->data();
    if (multi_resolver->resolve_cb_) multi_resolver->resolve_cb_(resolver);
    multi_resolver->dec_ref();
  }

private:
  T data_;
  ResolveCallback resolve_cb_;
  FinishedCallback finished_cb_;

  DISALLOW_COPY_AND_ASSIGN(MultiResolver);
};

} // namespace cass

#endif
