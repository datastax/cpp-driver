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

#ifndef DATASTAX_ENTERPRISE_INTERNAL_AUTH_HPP
#define DATASTAX_ENTERPRISE_INTERNAL_AUTH_HPP

#include "allocated.hpp"
#include "auth.hpp"
#include "string.hpp"

namespace datastax { namespace internal { namespace enterprise {

class DsePlainTextAuthenticator : public core::Authenticator {
public:
  DsePlainTextAuthenticator(const String& class_name, const String& username,
                            const String& password, const String& authorization_id)
      : class_name_(class_name)
      , username_(username)
      , password_(password)
      , authorization_id_(authorization_id) {}

  virtual bool initial_response(String* response);
  virtual bool evaluate_challenge(const String& token, String* response);
  virtual bool success(const String& token);

private:
  String class_name_;
  String username_;
  String password_;
  String authorization_id_;
};

class DsePlainTextAuthProvider : public core::AuthProvider {
public:
  DsePlainTextAuthProvider(const String& username, const String& password,
                           const String& authorization_id)
      : AuthProvider(String("DsePlainTextAuthProvider") +
                     (authorization_id.empty() ? "" : " (Proxy)"))
      , username_(username)
      , password_(password)
      , authorization_id_(authorization_id) {}

  virtual core::Authenticator::Ptr new_authenticator(const core::Address& address,
                                                     const String& hostname,
                                                     const String& class_name) const {
    return core::Authenticator::Ptr(
        new DsePlainTextAuthenticator(class_name, username_, password_, authorization_id_));
  }

private:
  String username_;
  String password_;
  String authorization_id_;
};

}}} // namespace datastax::internal::enterprise

#ifdef HAVE_KERBEROS
#include "gssapi/dse_auth_gssapi.hpp"
#endif

#endif
