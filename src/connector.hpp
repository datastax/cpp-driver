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

#ifndef __CASS_CONNECTOR_HPP_INCLUDED__
#define __CASS_CONNECTOR_HPP_INCLUDED__

#include <uv.h>

#include "address.hpp"

namespace cass {

class Connector {
public:
  typedef void (*Callback)(Connector*);

  const Address& address() { return address_; }
  int status() { return status_; }
  void* data() { return data_; }

  static void connect(uv_tcp_t* handle, const Address& address, void* data,
                      Callback cb) {
    Connector* connector = new Connector(address, data, cb);

    int rc = 0;

#if UV_VERSION_MAJOR == 0
    if (address.family() == AF_INET) {
      rc = uv_tcp_connect(&connector->req_, handle, *address.addr_in(),
                          on_connect);
    } else {
      rc = uv_tcp_connect6(&connector->req_, handle, *address.addr_in6(),
                           on_connect);
    }
#else
    rc = uv_tcp_connect(&connector->req_, handle, address.addr(),
                        on_connect);
#endif

    if (rc != 0) {
      connector->status_ = -1;
      connector->cb_(connector);
      delete connector;
    }
  }

private:
  static void on_connect(uv_connect_t* req, int status) {
    Connector* connector = static_cast<Connector*>(req->data);
    connector->status_ = status;
    connector->cb_(connector);
    delete connector;
  }

private:
  Connector(const Address& address, void* data, Callback cb)
      : address_(address)
      , data_(data)
      , cb_(cb)
      , status_(-1) {
    req_.data = this;
  }

  ~Connector() {}

  uv_connect_t req_;
  Address address_;
  void* data_;
  Callback cb_;
  int status_;
};
}
#endif
