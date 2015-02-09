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

#ifndef __CASS_AUTH_RESPONSES_HPP_INCLUDED__
#define __CASS_AUTH_RESPONSES_HPP_INCLUDED__

#include "constants.hpp"
#include "response.hpp"

#include <string>

namespace cass {

class AuthenticateResponse : public Response {
public:
  AuthenticateResponse()
    : Response(CQL_OPCODE_AUTHENTICATE)
    , authenticator_(NULL)
    , authenticator_size_(0) {}

  bool decode(int version, char* buffer, size_t size);

  std::string authenticator() const {
    return std::string(authenticator_, authenticator_size_);
  }

private:
  char* authenticator_;
  size_t authenticator_size_;
};

class AuthChallengeResponse : public Response {
public:
  AuthChallengeResponse()
    : Response(CQL_OPCODE_AUTH_CHALLENGE)
    , token_(NULL)
    , token_size_(0) {}

  std::string token() const { return std::string(token_, token_size_); }

  bool decode(int version, char* buffer, size_t size);

private:
  char* token_;
  size_t token_size_;
};

class AuthSuccessResponse : public Response {
public:
  AuthSuccessResponse()
    : Response(CQL_OPCODE_AUTH_SUCCESS)
    , token_(NULL)
    , token_size_(0) {}

  std::string token() const { return std::string(token_, token_size_); }

  bool decode(int version, char* buffer, size_t size);

private:
  char* token_;
  size_t token_size_;
};

} // namespace cass

#endif
