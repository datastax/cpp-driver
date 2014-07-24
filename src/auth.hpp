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

#ifndef __CASS_AUTH_HPP_INCLUDED__
#define __CASS_AUTH_HPP_INCLUDED__

#include "macros.hpp"
#include "address.hpp"
#include "buffer.hpp"

#include <map>
#include <string>

namespace cass {

class V1Autenticator {
public:
  virtual ~V1Autenticator() {}

  typedef std::map<std::string, std::string> Credentials;
  virtual void get_credentials(Credentials* credentials) = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(V1Autenticator);
};


class Authenticator {
public:
  virtual ~Authenticator() {}

  virtual std::string initial_response() = 0;
  virtual std::string evaluate_challenge(const std::string& challenge) = 0;
  virtual void on_authenticate_success(const std::string& token) = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(Authenticator);
};

class AuthProvider {
public:
  virtual V1Autenticator* new_authenticator_v1(Address host) { return NULL; }
  virtual Authenticator* new_authenticator(Address host) = 0;
};

} // namespace cass

#endif
