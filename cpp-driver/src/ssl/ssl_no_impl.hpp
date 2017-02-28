/*
  Copyright (c) 2014-2016 DataStax

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

#ifndef __CASS_SSL_NO_IMPL_HPP_INCLUDED__
#define __CASS_SSL_NO_IMPL_HPP_INCLUDED__

namespace cass {

class NoSslSession : public SslSession {
public:
  NoSslSession(const Host::ConstPtr& host);

  virtual bool is_handshake_done() const { return false; }
  virtual void do_handshake() {}
  virtual void verify() {}

  virtual int encrypt(const char* buf, size_t size) { return -1; }
  virtual int decrypt(char* buf, size_t size) { return -1; }
};

class NoSslContext : public SslContext {
public:

  virtual SslSession* create_session(const Host::ConstPtr& host);

  virtual CassError add_trusted_cert(const char* cert, size_t cert_length);
  virtual CassError set_cert(const char* cert, size_t cert_length);
  virtual CassError set_private_key(const char* key,
                                    size_t key_length,
                                    const char* password,
                                    size_t password_length);
};

class NoSslContextFactory : SslContextFactoryBase<NoSslContextFactory> {
public:
  static SslContext::Ptr create();
  static void init() {}
};

typedef SslContextFactoryBase<NoSslContextFactory> SslContextFactory;

} // namespace cass

#endif

