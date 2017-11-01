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

#ifndef __CASS_AUTH_HPP_INCLUDED__
#define __CASS_AUTH_HPP_INCLUDED__

#include "buffer.hpp"
#include "external.hpp"
#include "host.hpp"
#include "macros.hpp"
#include "ref_counted.hpp"

#include <map>
#include <string>

namespace cass {

class V1Authenticator {
public:
  V1Authenticator() { }
  virtual ~V1Authenticator() { }

  typedef std::map<std::string, std::string> Credentials;
  virtual void get_credentials(Credentials* credentials) = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(V1Authenticator);
};

class Authenticator : public RefCounted<Authenticator> {
public:
  typedef SharedRefPtr<Authenticator> Ptr;

  Authenticator() { }
  virtual ~Authenticator() { }

  const std::string& error() { return error_; }
  void set_error(const std::string& error) { error_ = error; }

  virtual bool initial_response(std::string* response) = 0;
  virtual bool evaluate_challenge(const std::string& token, std::string* response) = 0;
  virtual bool success(const std::string& token) = 0;

protected:
  std::string error_;

private:
  DISALLOW_COPY_AND_ASSIGN(Authenticator);
};

class PlainTextAuthenticator : public V1Authenticator, public Authenticator {
public:
  PlainTextAuthenticator(const std::string& username,
                         const std::string& password)
    : username_(username)
    , password_(password) { }

  virtual void get_credentials(Credentials* credentials);

  virtual bool initial_response(std::string* response);
  virtual bool evaluate_challenge(const std::string& token, std::string* response);
  virtual bool success(const std::string& token);

private:
  const std::string& username_;
  const std::string& password_;
};

class AuthProvider : public RefCounted<AuthProvider> {
public:
  typedef SharedRefPtr<AuthProvider> Ptr;

  AuthProvider()
    : RefCounted<AuthProvider>() { }

  virtual ~AuthProvider() { }

  virtual V1Authenticator* new_authenticator_v1(const Host::ConstPtr& host,
                                                const std::string& class_name) const {
    return NULL;
  }

  virtual Authenticator::Ptr new_authenticator(const Host::ConstPtr& host,
                                               const std::string& class_name) const {
    return Authenticator::Ptr();
  }

private:
  DISALLOW_COPY_AND_ASSIGN(AuthProvider);
};

class ExternalAuthenticator : public Authenticator {
public:
  ExternalAuthenticator(const Host::ConstPtr& host, const std::string& class_name,
                        const CassAuthenticatorCallbacks* callbacks, void* data);

  ~ExternalAuthenticator();

  const Address& address() const { return address_; }

  const std::string& hostname() const { return hostname_; }
  const std::string& class_name() const { return class_name_; }

  std::string* response() { return response_; }

  void* exchange_data() const { return exchange_data_; }
  void set_exchange_data(void* exchange_data) { exchange_data_ = exchange_data; }

  virtual bool initial_response(std::string* response);
  virtual bool evaluate_challenge(const std::string& token, std::string* response);
  virtual bool success(const std::string& token);

private:
  const Address address_;
  const std::string hostname_;
  const std::string class_name_;
  std::string* response_;
  const CassAuthenticatorCallbacks* callbacks_;
  void* data_;
  void* exchange_data_;
};

class ExternalAuthProvider : public AuthProvider {
public:
  ExternalAuthProvider(const CassAuthenticatorCallbacks* exchange_callbacks,
                   CassAuthenticatorDataCleanupCallback cleanup_callback,
                   void* data)
    : exchange_callbacks_(*exchange_callbacks)
    , cleanup_callback_(cleanup_callback)
    , data_(data) { }

  ~ExternalAuthProvider() {
    if (cleanup_callback_ != NULL) {
      cleanup_callback_(data_);
    }
  }

  virtual Authenticator::Ptr new_authenticator(const Host::ConstPtr& host,
                                               const std::string& class_name) const {
    return Authenticator::Ptr(new ExternalAuthenticator(host, class_name, &exchange_callbacks_, data_));
  }

private:
  const CassAuthenticatorCallbacks exchange_callbacks_;
  CassAuthenticatorDataCleanupCallback cleanup_callback_;
  void* data_;
};

class PlainTextAuthProvider : public AuthProvider {
public:
  PlainTextAuthProvider(const std::string& username,
                        const std::string& password)
    : username_(username)
    , password_(password) { }

  virtual V1Authenticator* new_authenticator_v1(const Host::ConstPtr& host,
                                                const std::string& class_name) const {
    return new PlainTextAuthenticator(username_, password_);
  }

  virtual Authenticator::Ptr new_authenticator(const Host::ConstPtr& host,
                                               const std::string& class_name) const {
    return Authenticator::Ptr(new PlainTextAuthenticator(username_, password_));
  }

private:
  std::string username_;
  std::string password_;
};

} // namespace cass

EXTERNAL_TYPE(cass::ExternalAuthenticator, CassAuthenticator)

#endif
