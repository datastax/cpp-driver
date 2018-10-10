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

#ifndef __CASS_SSL_HPP_INCLUDED__
#define __CASS_SSL_HPP_INCLUDED__

#include "address.hpp"
#include "cassandra.h"
#include "cassconfig.hpp"
#include "external.hpp"
#include "ref_counted.hpp"
#include "ring_buffer.hpp"
#include "string.hpp"

#include <uv.h>

namespace cass {

class SslSession {
public:
  SslSession(const Address& address,
             const String& hostname,
             int flags)
    : address_(address)
    , hostname_(hostname)
    , verify_flags_(flags)
    , error_code_(CASS_OK) {}

  virtual ~SslSession() {}

  bool has_error() const {
    return error_code() != CASS_OK;
  }

  CassError error_code() const {
    return error_code_;
  }

  String error_message() const {
    return error_message_;
  }

  virtual bool is_handshake_done() const = 0;
  virtual void do_handshake() = 0;
  virtual void verify() = 0;

  virtual int encrypt(const char* data, size_t data_size) = 0;
  virtual int decrypt(char* data, size_t data_size) = 0;

  rb::RingBuffer& incoming() { return incoming_; }
  rb::RingBuffer& outgoing() { return outgoing_; }

protected:
  Address address_;
  String hostname_;
  int verify_flags_;
  rb::RingBuffer incoming_;
  rb::RingBuffer outgoing_;
  CassError error_code_;
  String error_message_;
};

class SslContext : public RefCounted<SslContext> {
public:
  typedef SharedRefPtr<SslContext> Ptr;

  SslContext()
    : verify_flags_(CASS_SSL_VERIFY_PEER_CERT) {}

  virtual ~SslContext() {}

  void set_verify_flags(int flags) {
    verify_flags_ = flags;
  }

  virtual SslSession* create_session(const Address& address, const String& hostname) = 0;
  virtual CassError add_trusted_cert(const char* cert, size_t cert_length) = 0;
  virtual CassError set_cert(const char* cert, size_t cert_length) = 0;
  virtual CassError set_private_key(const char* key,
                                    size_t key_length,
                                    const char* password,
                                    size_t password_length) = 0;

protected:
  int verify_flags_;
};

template <class T>
class SslContextFactoryBase {
public:
  static SslContext::Ptr create();
  static void init_once();
  static void thread_cleanup();

  static void init(); // Tests only
  static void cleanup(); // Tests only

private:
  static uv_once_t ssl_init_guard;
};

template<class T>
SslContext::Ptr SslContextFactoryBase<T>::create() {
  return T::create();
}

template<class T>
void SslContextFactoryBase<T>::init_once() {
  uv_once(&ssl_init_guard, T::init);
}

template<class T>
void SslContextFactoryBase<T>::thread_cleanup() {
  T::internal_thread_cleanup();
}

template<class T>
void SslContextFactoryBase<T>::init() {
  T::internal_init();
}

template<class T>
void SslContextFactoryBase<T>::cleanup() {
  T::internal_cleanup();
}

} // namespace cass

#ifdef HAVE_OPENSSL
#include "ssl/ssl_openssl_impl.hpp"
#else
#include "ssl/ssl_no_impl.hpp"
#endif

EXTERNAL_TYPE(cass::SslContext, CassSsl)

#endif
