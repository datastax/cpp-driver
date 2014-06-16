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

#ifndef __CASS_CONNECTER_HPP_INCLUDED__
#define __CASS_CONNECTER_HPP_INCLUDED__

#include <uv.h>

#include "address.hpp"

namespace cass {

class Connecter {
public:
  typedef std::function<void(Connecter*)> Callback;

  enum Status { CONNECTING, FAILED, SUCCESS };

  const Address& address() { return address_; }
  Status status() { return status_; }
  void* data() { return data_; }

  static void connect(uv_tcp_t* handle, const Address& address, void* data,
                      Callback cb) {
    Connecter* connecter = new Connecter(address, data, cb);

    int rc = 0;
    if (address.family() == AF_INET) {
      rc = uv_tcp_connect(&connecter->req_, handle, *address.addr_in(),
                          on_connect);
    } else {
      rc = uv_tcp_connect6(&connecter->req_, handle, *address.addr_in6(),
                           on_connect);
    }

    if (rc != 0) {
      connecter->status_ = FAILED;
      connecter->cb_(connecter);
      delete connecter;
    }
  }

private:
  static void on_connect(uv_connect_t* req, int status) {
    Connecter* connecter = static_cast<Connecter*>(req->data);
    if (status != 0) {
      connecter->status_ = FAILED;
    } else {
      connecter->status_ = SUCCESS;
    }
    connecter->cb_(connecter);
    delete connecter;
  }

private:
  Connecter(const Address& address, void* data, Callback cb)
      : address_(address)
      , data_(data)
      , cb_(cb)
      , status_(CONNECTING) {
    req_.data = this;
  }

  ~Connecter() {}

  uv_connect_t req_;
  Address address_;
  void* data_;
  Callback cb_;
  Status status_;
};
}
#endif
