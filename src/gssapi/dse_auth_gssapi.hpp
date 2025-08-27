/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#ifndef DATASTAX_ENTERPRISE_INTERNAL_GSSAPI_KERBEROS_IMPL_HPP
#define DATASTAX_ENTERPRISE_INTERNAL_GSSAPI_KERBEROS_IMPL_HPP

#include "allocated.hpp"
#include "auth.hpp"
#include "dse.h"
#include "string.hpp"

namespace datastax { namespace internal { namespace enterprise {

class GssapiAuthenticatorImpl;

class DseGssapiAuthenticator : public core::Authenticator {
public:
  DseGssapiAuthenticator(const core::Address& address, const String& hostname,
                         const String& class_name, const String& service, const String& principal,
                         const String& authorization_id);

  virtual bool initial_response(String* response);
  virtual bool evaluate_challenge(const String& token, String* response);
  virtual bool success(const String& token);

public:
  static CassError set_lock_callbacks(DseGssapiAuthenticatorLockCallback lock_callback,
                                      DseGssapiAuthenticatorUnlockCallback unlock_callback,
                                      void* data);

  static void lock() { lock_callback_(data_); }
  static void unlock() { unlock_callback_(data_); }

private:
  static DseGssapiAuthenticatorLockCallback lock_callback_;
  static DseGssapiAuthenticatorUnlockCallback unlock_callback_;
  static void* data_;

private:
  core::Address address_;
  String hostname_;
  String class_name_;
  String service_;
  String principal_;
  String authorization_id_;
  ScopedPtr<GssapiAuthenticatorImpl> impl_;
};

class DseGssapiAuthProvider : public core::AuthProvider {
public:
  DseGssapiAuthProvider(const String& service, const String& principal,
                        const String& authorization_id)
      : AuthProvider(String("DseGssapiAuthProvider") + (authorization_id.empty() ? "" : " (Proxy)"))
      , service_(service)
      , principal_(principal)
      , authorization_id_(authorization_id) {}

  virtual core::Authenticator::Ptr new_authenticator(const core::Address& address,
                                                     const String& hostname,
                                                     const String& class_name) const {
    return core::Authenticator::Ptr(new DseGssapiAuthenticator(
        address, hostname, class_name, service_, principal_, authorization_id_));
  }

private:
  String service_;
  String principal_;
  String authorization_id_;
};

}}} // namespace datastax::internal::enterprise

#endif
