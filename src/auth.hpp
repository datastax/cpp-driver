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

#ifndef DATASTAX_INTERNAL_AUTH_HPP
#define DATASTAX_INTERNAL_AUTH_HPP

#include "buffer.hpp"
#include "external.hpp"
#include "host.hpp"
#include "macros.hpp"
#include "map.hpp"
#include "ref_counted.hpp"
#include "string.hpp"

namespace datastax { namespace internal { namespace core {

class Authenticator : public RefCounted<Authenticator> {
public:
  typedef SharedRefPtr<Authenticator> Ptr;

  Authenticator() {}
  virtual ~Authenticator() {}

  const String& error() { return error_; }
  void set_error(const String& error) { error_ = error; }

  virtual bool initial_response(String* response) = 0;
  virtual bool evaluate_challenge(const String& token, String* response) = 0;
  virtual bool success(const String& token) = 0;

protected:
  String error_;

private:
  DISALLOW_COPY_AND_ASSIGN(Authenticator);
};

class PlainTextAuthenticator : public Authenticator {
public:
  PlainTextAuthenticator(const String& username, const String& password)
      : username_(username)
      , password_(password) {}

  virtual bool initial_response(String* response);
  virtual bool evaluate_challenge(const String& token, String* response);
  virtual bool success(const String& token);

private:
  const String& username_;
  const String& password_;
};

class AuthProvider : public RefCounted<AuthProvider> {
public:
  typedef SharedRefPtr<AuthProvider> Ptr;

  AuthProvider(const String& name = "")
      : RefCounted<AuthProvider>()
      , name_(name) {}

  virtual ~AuthProvider() {}

  virtual Authenticator::Ptr new_authenticator(const Address& address, const String& hostname,
                                               const String& class_name) const {
    return Authenticator::Ptr();
  }

  const String& name() const { return name_; }

private:
  DISALLOW_COPY_AND_ASSIGN(AuthProvider);

private:
  const String name_;
};

class ExternalAuthenticator : public Authenticator {
public:
  ExternalAuthenticator(const Address& address, const String& hostname, const String& class_name,
                        const CassAuthenticatorCallbacks* callbacks, void* data);

  ~ExternalAuthenticator();

  const Address& address() const { return address_; }

  const String& hostname() const { return hostname_; }
  const String& class_name() const { return class_name_; }

  String* response() { return response_; }

  void* exchange_data() const { return exchange_data_; }
  void set_exchange_data(void* exchange_data) { exchange_data_ = exchange_data; }

  virtual bool initial_response(String* response);
  virtual bool evaluate_challenge(const String& token, String* response);
  virtual bool success(const String& token);

private:
  const Address address_;
  const String hostname_;
  const String class_name_;
  String* response_;
  const CassAuthenticatorCallbacks* callbacks_;
  void* data_;
  void* exchange_data_;
};

class ExternalAuthProvider : public AuthProvider {
public:
  ExternalAuthProvider(const CassAuthenticatorCallbacks* exchange_callbacks,
                       CassAuthenticatorDataCleanupCallback cleanup_callback, void* data)
      : AuthProvider("ExternalAuthProvider")
      , exchange_callbacks_(*exchange_callbacks)
      , cleanup_callback_(cleanup_callback)
      , data_(data) {}

  ~ExternalAuthProvider() {
    if (cleanup_callback_ != NULL) {
      cleanup_callback_(data_);
    }
  }

  virtual Authenticator::Ptr new_authenticator(const Address& address, const String& hostname,
                                               const String& class_name) const {
    return Authenticator::Ptr(
        new ExternalAuthenticator(address, hostname, class_name, &exchange_callbacks_, data_));
  }

private:
  const CassAuthenticatorCallbacks exchange_callbacks_;
  CassAuthenticatorDataCleanupCallback cleanup_callback_;
  void* data_;
};

class PlainTextAuthProvider : public AuthProvider {
public:
  PlainTextAuthProvider(const String& username, const String& password)
      : AuthProvider("PlainTextAuthProvider")
      , username_(username)
      , password_(password) {}

  virtual Authenticator::Ptr new_authenticator(const Address& address, const String& hostname,
                                               const String& class_name) const {
    return Authenticator::Ptr(new PlainTextAuthenticator(username_, password_));
  }

private:
  String username_;
  String password_;
};

}}} // namespace datastax::internal::core

EXTERNAL_TYPE(datastax::internal::core::ExternalAuthenticator, CassAuthenticator)

#endif
