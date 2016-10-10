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

#include "ssl.hpp"

#include "cassandra.h"
#include "external.hpp"

#include <uv.h>

extern "C" {

CassSsl* cass_ssl_new() {
  cass::SslContextFactory::init();
  return cass_ssl_new_no_lib_init();
}

CassSsl* cass_ssl_new_no_lib_init() {
  cass::SslContext::Ptr ssl_context(cass::SslContextFactory::create());
  ssl_context->inc_ref();
  return CassSsl::to(ssl_context.get());
}

void cass_ssl_free(CassSsl* ssl) {
  ssl->dec_ref();
}

CassError cass_ssl_add_trusted_cert(CassSsl* ssl, const char* cert) {
  return cass_ssl_add_trusted_cert_n(ssl, cert, strlen(cert));
}

CassError cass_ssl_add_trusted_cert_n(CassSsl* ssl, const char* cert, size_t cert_length) {
  return ssl->add_trusted_cert(cert, cert_length);
}

void cass_ssl_set_verify_flags(CassSsl* ssl, int flags) {
  ssl->set_verify_flags(flags);
}

CassError cass_ssl_set_cert(CassSsl* ssl, const char* cert) {
  return cass_ssl_set_cert_n(ssl, cert, strlen(cert));
}

CassError cass_ssl_set_cert_n(CassSsl* ssl,
                              const char* cert,
                              size_t cert_length) {
  return ssl->set_cert(cert, cert_length);
}

CassError cass_ssl_set_private_key(CassSsl* ssl,
                                   const char* key,
                                   const char* password) {
  return cass_ssl_set_private_key_n(ssl,
                                    key, strlen(key),
                                    password, strlen(password));
}

CassError cass_ssl_set_private_key_n(CassSsl* ssl,
                                     const char* key,
                                     size_t key_length,
                                     const char* password,
                                     size_t password_length) {
  return ssl->set_private_key(key,
                              key_length,
                              password,
                              password_length);
}

} // extern "C"

namespace cass {

static uv_once_t ssl_init_guard = UV_ONCE_INIT;

template<class T>
SslContext::Ptr SslContextFactoryBase<T>::create() {
  return T::create();
}

template<class T>
void SslContextFactoryBase<T>::init() {
  uv_once(&ssl_init_guard, T::init);
}


} // namespace cass
