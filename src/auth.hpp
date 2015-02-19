/*
  Copyright (c) 2014-2015 DataStax

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

#include "macros.hpp"
#include "address.hpp"
#include "buffer.hpp"
#include "ref_counted.hpp"

#include <map>
#include <string>

namespace cass {

class V1Authenticator {
public:
  V1Authenticator() {}
  virtual ~V1Authenticator() {}

  typedef std::map<std::string, std::string> Credentials;
  virtual void get_credentials(Credentials* credentials) = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(V1Authenticator);
};

class Authenticator {
public:
  Authenticator() {}
  virtual ~Authenticator() {}

  virtual std::string initial_response() = 0;
  // TODO(mpenick): Do these need to know the difference between a
  // NULL token and an empty token?
  virtual std::string evaluate_challenge(const std::string& challenge) = 0;
  virtual void on_authenticate_success(const std::string& token) = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(Authenticator);
};

class PlainTextAuthenticator : public V1Authenticator, public Authenticator {
public:
  PlainTextAuthenticator(const std::string& username,
                         const std::string& password)
      : username_(username)
      , password_(password) {}

  virtual void get_credentials(Credentials* credentials);

  virtual std::string initial_response();
  virtual std::string evaluate_challenge(const std::string& challenge);
  virtual void on_authenticate_success(const std::string& token);

private:
  const std::string& username_;
  const std::string& password_;
};

class AuthProvider : public RefCounted<AuthProvider> {
public:
  AuthProvider()
    : RefCounted<AuthProvider>() {}

  virtual ~AuthProvider() {}

  virtual V1Authenticator* new_authenticator_v1(Address host) const { return NULL; }
  virtual Authenticator* new_authenticator(Address host) const { return NULL; }

private:
  DISALLOW_COPY_AND_ASSIGN(AuthProvider);
};

class PlainTextAuthProvider : public AuthProvider {
public:
  PlainTextAuthProvider(const std::string& username,
                         const std::string& password)
      : username_(username)
      , password_(password) {}

  virtual V1Authenticator* new_authenticator_v1(Address host) const {
    return new PlainTextAuthenticator(username_, password_);
  }

  virtual Authenticator* new_authenticator(Address host) const {
    return new PlainTextAuthenticator(username_, password_);
  }

private:
  std::string username_;
  std::string password_;
};

} // namespace cass

#endif
