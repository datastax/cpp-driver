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

#include "ssl.hpp"

namespace cass {

NoSessionImpl::NoSessionImpl(const Address& address,
                             const std::string& hostname)
  : SslSessionBase<NoSessionImpl>(address, hostname, CASS_SSL_VERIFY_NONE) {
  error_code_ = CASS_ERROR_LIB_NOT_IMPLEMENTED;
  error_message_ = "SSL support not built into driver";
}

NoContextImpl*NoContextImpl::create() {
  return new NoContextImpl();
}

NoSessionImpl*cass::NoContextImpl::create_session_impl(const Address& address,
                                                       const std::string& hostname) {
  return new NoSessionImpl(address, hostname);
}

CassError cass::NoContextImpl::add_trusted_cert_impl(CassString cert) {
  return CASS_ERROR_LIB_NOT_IMPLEMENTED;
}

CassError cass::NoContextImpl::set_cert_impl(CassString cert) {
  return CASS_ERROR_LIB_NOT_IMPLEMENTED;
}

CassError cass::NoContextImpl::set_private_key_impl(CassString key, const char* password) {
  return CASS_ERROR_LIB_NOT_IMPLEMENTED;
}

} // namespace cass
