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

#ifndef DATASTAX_INTERNAL_SSL_OPENSSL_IMPL_HPP
#define DATASTAX_INTERNAL_SSL_OPENSSL_IMPL_HPP

#include "ssl/ring_buffer_bio.hpp"

#include <assert.h>
#include <openssl/bio.h>
#include <openssl/ssl.h>

namespace datastax { namespace internal { namespace core {

class OpenSslSession : public SslSession {
public:
  OpenSslSession(const Address& address, const String& hostname, const String& sni_server_name,
                 int flags, SSL_CTX* ssl_ctx);
  ~OpenSslSession();

  virtual bool is_handshake_done() const { return SSL_is_init_finished(ssl_) != 0; }

  virtual void do_handshake();
  virtual void verify();

  virtual int encrypt(const char* buf, size_t size);
  virtual int decrypt(char* buf, size_t size);

private:
  void check_error(int rc);

  SSL* ssl_;
  rb::RingBufferState incoming_state_;
  rb::RingBufferState outgoing_state_;
  BIO* incoming_bio_;
  BIO* outgoing_bio_;
};

class OpenSslContext : public SslContext {
public:
  OpenSslContext();

  ~OpenSslContext();

  virtual SslSession* create_session(const Address& address, const String& hostname,
                                     const String& sni_server_name);
  virtual CassError add_trusted_cert(const char* cert, size_t cert_length);
  virtual CassError set_cert(const char* cert, size_t cert_length);
  virtual CassError set_private_key(const char* key, size_t key_length, const char* password,
                                    size_t password_length);
  virtual CassError set_min_protocol_version(CassSslTlsVersion min_version);

private:
  SSL_CTX* ssl_ctx_;
  X509_STORE* trusted_store_;
};

class OpenSslContextFactory : public SslContextFactoryBase<OpenSslContextFactory> {
public:
  static SslContext::Ptr create();
  static void internal_init();
  static void internal_thread_cleanup();
  static void internal_cleanup();
};

typedef SslContextFactoryBase<OpenSslContextFactory> SslContextFactory;

}}} // namespace datastax::internal::core

#endif
