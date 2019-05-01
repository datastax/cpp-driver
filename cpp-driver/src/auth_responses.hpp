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

#ifndef DATASTAX_INTERNAL_AUTH_RESPONSES_HPP
#define DATASTAX_INTERNAL_AUTH_RESPONSES_HPP

#include "constants.hpp"
#include "response.hpp"
#include "string.hpp"
#include "string_ref.hpp"

namespace datastax { namespace internal { namespace core {

class AuthenticateResponse : public Response {
public:
  AuthenticateResponse()
      : Response(CQL_OPCODE_AUTHENTICATE) {}

  const String& class_name() const { return class_name_; }

  virtual bool decode(Decoder& decoder);

private:
  String class_name_;
};

class AuthChallengeResponse : public Response {
public:
  AuthChallengeResponse()
      : Response(CQL_OPCODE_AUTH_CHALLENGE) {}

  const String& token() const { return token_; }

  virtual bool decode(Decoder& decoder);

private:
  String token_;
};

class AuthSuccessResponse : public Response {
public:
  AuthSuccessResponse()
      : Response(CQL_OPCODE_AUTH_SUCCESS) {}

  const String& token() const { return token_; }

  virtual bool decode(Decoder& decoder);

private:
  String token_;
};

}}} // namespace datastax::internal::core

#endif
