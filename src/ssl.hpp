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

#ifndef __CASS_SSL_HPP_INCLUDED__
#define __CASS_SSL_HPP_INCLUDED__

#include "address.hpp"
#include "cassandra.h"
#include "ref_counted.hpp"

#include "third_party/rb/ring_buffer.hpp"

#include <string>
#include <uv.h>

namespace cass {

// TODO(mpenick):
// 1) SSL Error handling
//    a) No SSL
//    b) Bad handshake (no support for cipher etc.)
//    c) Bad read/write
// done 2) Load certificate chain
// done 3) Load private key
// done 4) Cert chain verification
// done 5) Disable verification
// done 6) Initialize SSL library

template <class T>
class SslSessionBase {
public:
  SslSessionBase(const Address& address,
                 const std::string& hostname,
                 int flags)
    : addr_(address)
    , hostname_(hostname)
    , verify_flags_(flags)
    , error_code_(CASS_OK) {}

  bool has_error() const {
    return error_code() != CASS_OK;
  }

  CassError error_code() const {
    return error_code_;
  }

  std::string error_message() const {
    return error_message_;
  }

  bool is_handshake_done() const {
    return static_cast<const T*>(this)->is_handshake_done_impl();
  }

  void do_handshake() {
    static_cast<T*>(this)->do_handshake_impl();
  }

  void verify() {
    static_cast<T*>(this)->verify_impl();
  }

  int encrypt(const char* data, size_t data_size) {
    return static_cast<T*>(this)->encrypt_impl(data, data_size);
  }

  int decrypt(char* data, size_t data_size) {
    return static_cast<T*>(this)->decrypt_impl(data, data_size);
  }

  rb::RingBuffer& incoming() { return incoming_; }

  rb::RingBuffer& outgoing() { return outgoing_; }

protected:
  Address addr_;
  std::string hostname_;
  int verify_flags_;
  rb::RingBuffer incoming_;
  rb::RingBuffer outgoing_;
  CassError error_code_;
  std::string error_message_;
};

template <class S, class T>
class SslContextBase : public RefCounted<SslContextBase<S, T> > {
public:
  SslContextBase()
    : verify_flags_(CASS_SSL_VERIFY_PEER_CERT | CASS_SSL_VERIFY_PEER_IDENTITY) {}

  static SslContextBase<S, T>* create();

  static void init();

  SslSessionBase<T>* create_session(const Address& address,
                                    const std::string& hostname) {
    return static_cast<S*>(this)->create_session_impl(address, hostname);
  }

  CassError add_trusted_cert(CassString cert) {
    return static_cast<S*>(this)->add_trusted_cert_impl(cert);
  }

  void set_verify_flags(int flags) {
    verify_flags_ = flags;
  }

  CassError set_cert(CassString cert) {
    return static_cast<S*>(this)->set_cert_impl(cert);
  }

  CassError set_private_key(CassString key, const char* password) {
    return static_cast<S*>(this)->set_private_key_impl(key, password);
  }

protected:
  int verify_flags_;
};

} // namespace cass

#include "ssl_openssl_impl.hpp"

#endif
