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

#include "dse.h"

#include "dse_auth.hpp"
#include "logger.hpp"

#define DSE_AUTHENTICATOR "com.datastax.bdp.cassandra.auth.DseAuthenticator"

#define PLAINTEXT_AUTH_MECHANISM "PLAIN"
#define PLAINTEXT_AUTH_SERVER_INITIAL_CHALLENGE "PLAIN-START"

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::enterprise;

extern "C" {

CassError
dse_gssapi_authenticator_set_lock_callbacks(DseGssapiAuthenticatorLockCallback lock_callback,
                                            DseGssapiAuthenticatorUnlockCallback unlock_callback,
                                            void* data) {
#ifdef HAVE_KERBEROS
  return DseGssapiAuthenticator::set_lock_callbacks(lock_callback, unlock_callback, data);
#else
  return CASS_ERROR_LIB_NOT_IMPLEMENTED;
#endif
}

} // extern "C"

bool DsePlainTextAuthenticator::initial_response(String* response) {
  if (class_name_ == DSE_AUTHENTICATOR) {
    response->assign(PLAINTEXT_AUTH_MECHANISM);
    return true;
  } else {
    return evaluate_challenge(PLAINTEXT_AUTH_SERVER_INITIAL_CHALLENGE, response);
  }
}

bool DsePlainTextAuthenticator::evaluate_challenge(const String& token, String* response) {
  if (token != PLAINTEXT_AUTH_SERVER_INITIAL_CHALLENGE) {
    LOG_ERROR("Invalid start token for DSE plaintext authenticator during challenge: '%s'",
              token.c_str());
    return false;
  }

  // Credentials are of the form "<authid>\0<username>\0<password>"
  response->append(authorization_id_);
  response->push_back('\0');
  response->append(username_);
  response->push_back('\0');
  response->append(password_);

  return true;
}

bool DsePlainTextAuthenticator::success(const String& token) {
  // no-op
  return true;
}
