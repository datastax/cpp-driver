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

#include "ssl.hpp"

namespace cass {

NoSslSession::NoSslSession(const Host::ConstPtr& host)
  : SslSession(host, CASS_SSL_VERIFY_NONE) {
  error_code_ = CASS_ERROR_LIB_NOT_IMPLEMENTED;
  error_message_ = "SSL support not built into driver";
}

SslSession* cass::NoSslContext::create_session(const Host::ConstPtr& host) {
  return new NoSslSession(host);
}

CassError cass::NoSslContext::add_trusted_cert(const char* cert,
                                               size_t cert_length) {
  return CASS_ERROR_LIB_NOT_IMPLEMENTED;
}

CassError cass::NoSslContext::set_cert(const char* cert,
                                       size_t cert_length) {
  return CASS_ERROR_LIB_NOT_IMPLEMENTED;
}

CassError cass::NoSslContext::set_private_key(const char* key,
                                              size_t key_length,
                                              const char* password,
                                              size_t password_length) {
  return CASS_ERROR_LIB_NOT_IMPLEMENTED;
}

SslContext::Ptr NoSslContextFactory::create() {
  return SslContext::Ptr(new NoSslContext());
}


} // namespace cass
