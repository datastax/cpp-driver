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

#ifndef DATASTAX_INTERNAL_AUTH_REQUESTS_HPP
#define DATASTAX_INTERNAL_AUTH_REQUESTS_HPP

#include "auth.hpp"
#include "constants.hpp"
#include "ref_counted.hpp"
#include "request.hpp"

namespace datastax { namespace internal { namespace core {

class AuthResponseRequest : public Request {
public:
  AuthResponseRequest(const String& token, const Authenticator::Ptr& auth)
      : Request(CQL_OPCODE_AUTH_RESPONSE)
      , token_(token)
      , auth_(auth) {}

  const Authenticator::Ptr& auth() const { return auth_; }

private:
  int encode(ProtocolVersion version, RequestCallback* callback, BufferVec* bufs) const;

private:
  String token_;
  Authenticator::Ptr auth_;
};

}}} // namespace datastax::internal::core

#endif
