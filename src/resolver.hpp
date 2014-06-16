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

#ifndef __CASS_RESOLVER_HPP_INCLUDED__
#define __CASS_RESOLVER_HPP_INCLUDED__

#include <uv.h>

#include <functional>
#include <string>

#include "address.hpp"

namespace cass {

class Resolver {
public:
  typedef std::function<void(Resolver*)> Callback;

  enum Status {
    RESOLVING,
    FAILED_BAD_PARAM,
    FAILED_UNSUPPORTED_ADDRESS_FAMILY,
    FAILED_UNABLE_TO_RESOLVE,
    SUCCESS
  };

  const std::string& host() { return host_; }
  int port() { return port_; }
  bool is_success() { return status_ == SUCCESS; }
  Status status() { return status_; }
  Address& address() { return address_; }
  void* data() { return data_; }

  static void resolve(uv_loop_t* loop, const std::string& host, int port,
                      void* data, Callback cb,
                      struct addrinfo* hints = nullptr) {
    Resolver* resolver = new Resolver(host, port, data, cb);

    int rc = uv_getaddrinfo(loop, &resolver->req_, on_resolve, host.c_str(),
                            std::to_string(port).c_str(), hints);

    if (rc != 0) {
      resolver->status_ = FAILED_BAD_PARAM;
      resolver->cb_(resolver);
      delete resolver;
    }
  }

private:
  static void on_resolve(uv_getaddrinfo_t* req, int status,
                         struct addrinfo* res) {
    Resolver* resolver = static_cast<Resolver*>(req->data);

    if (status != 0) {
      resolver->status_ = FAILED_UNABLE_TO_RESOLVE;
    } else if (!resolver->address_.init(res->ai_addr)) {
      resolver->status_ = FAILED_UNSUPPORTED_ADDRESS_FAMILY;
    } else {
      resolver->status_ = SUCCESS;
    }

    resolver->cb_(resolver);

    delete resolver;
    uv_freeaddrinfo(res);
  }

private:
  Resolver(const std::string& host, int port, void* data, Callback cb)
      : host_(host)
      , port_(port)
      , status_(RESOLVING)
      , data_(data)
      , cb_(cb) {
    req_.data = this;
  }

  ~Resolver() {}

  uv_getaddrinfo_t req_;
  std::string host_;
  int port_;
  Status status_;
  Address address_;
  void* data_;
  Callback cb_;
};

} // namespace cass

#endif
