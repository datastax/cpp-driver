/*
  Copyright (c) 2014-2017 DataStax

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
#include "callback.hpp"
#include "memory.hpp"
#include "ref_counted.hpp"

namespace cass {

/**
 * A wrapper for uv_connect that handles connecting a TCP connection.
 */
class TcpConnector : public RefCounted<TcpConnector> {
public:
  typedef SharedRefPtr<TcpConnector> Ptr;

  typedef cass::Callback<void, TcpConnector*> Callback;

  enum Status {
    NEW,
    CONNECTING,
    FAILED_BAD_PARAM,
    FAILED_TO_CONNECT,
    CANCELED,
    SUCCESS
  };

  /**
   * Constructor
   *
   * @param address The address to connect to.
   */
  TcpConnector(const Address& address)
      : address_(address)
      , status_(NEW)
      , uv_status_(-1) {
    req_.data = this;
  }

  /**
   * Connect the given TCP handle.
   *
   * @param handle The handle to connect.
   * @param callback A callback that's called when the handle is connected or
   * an error occurs.
   */
  void connect(uv_tcp_t* handle, const Callback& callback) {
    int rc = 0;

    inc_ref(); // For the event loop

    callback_ = callback;
    status_ = CONNECTING;

    rc = uv_tcp_connect(&req_, handle, static_cast<const Address>(address_).addr(), on_connect);

    if (rc != 0) {
      status_ = FAILED_BAD_PARAM;
      uv_status_ = rc;
      callback_(this);
      dec_ref();
    }
  }

  /**
   * Cancel the connection process.
   */
  void cancel() {
    if (status_ == CONNECTING) {
      uv_cancel(reinterpret_cast<uv_req_t*>(&req_));
      status_ = CANCELED;
    }
  }

public:
  uv_loop_t* loop() { return req_.handle->loop;  }

  bool is_success() { return status_ == SUCCESS; }
  bool is_canceled() { return status_ == CANCELED; }
  Status status() { return status_; }
  int uv_status() { return uv_status_; }

  const Address& address() { return address_; }


private:
  static void on_connect(uv_connect_t* req, int status) {
    TcpConnector* connector = static_cast<TcpConnector*>(req->data);
    if (connector->status_ == CONNECTING) {
      if (status == 0) {
        connector->status_ = SUCCESS;
      } else {
        connector->status_ = FAILED_TO_CONNECT;
      }
    }

    connector->uv_status_ = status;
    connector->callback_(connector);
    connector->dec_ref();
  }

private:
  uv_connect_t req_;
  Address address_;
  Callback callback_;
  Status status_;
  int uv_status_;
};

} // namespace cass

#endif
