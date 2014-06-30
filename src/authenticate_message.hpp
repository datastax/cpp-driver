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

#ifndef __CASS_AUTHENTICATE_HPP_INCLUDED__
#define __CASS_AUTHENTICATE_HPP_INCLUDED__

#include "message_body.hpp"
#include "serialization.hpp"

namespace cass {

struct AuthenticateMessage : public MessageBody {
  AuthenticateMessage()
      : MessageBody(CQL_OPCODE_AUTHENTICATE) {}

  bool consume(char* buffer, size_t size)
  {
    decode_string(buffer, &authenticator, size);
    return true;
  }

  char* GetAuthenticator() { return authenticator; }

private:
  char* authenticator;
};

} // namespace cass

#endif
