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

#ifndef __CASS_SSL_NO_IMPL_HPP_INCLUDED__
#define __CASS_SSL_NO_IMPL_HPP_INCLUDED__

namespace cass {

class NoSessionImpl : public SslSessionBase<NoSessionImpl> {
public:
  NoSessionImpl(const Address& address,
                const std::string& hostname);

  bool is_handshake_done_impl() const { return false; }

  void do_handshake_impl() {}

  void verify_impl() {}

  int encrypt_impl(const char* buf, size_t size) { return -1; }

  int decrypt_impl(char* buf, size_t size) { return -1; }
};

class NoContextImpl: public SslContextBase<NoContextImpl, NoSessionImpl> {
public:
  static NoContextImpl* create();

  static void init() {}

  NoSessionImpl* create_session_impl(const Address& address,
                                     const std::string& hostname);

  CassError add_trusted_cert_impl(CassString cert);

  CassError set_cert_impl(CassString cert);

  CassError set_private_key_impl(CassString key, const char* password);
};

typedef SslSessionBase<NoSessionImpl> SslSession;
typedef SslContextBase<NoContextImpl, NoSessionImpl> SslContext;

} // namespace cass

#endif

