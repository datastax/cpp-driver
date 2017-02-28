/*
  Copyright (c) 2014-2016 DataStax

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

#include "auth_responses.hpp"

#include "serialization.hpp"

namespace cass {

bool AuthenticateResponse::decode(int version, char* buffer, size_t size) {
  StringRef class_name;
  decode_string(buffer, &class_name);
  class_name_ = class_name.to_string();
  return true;
}

bool cass::AuthChallengeResponse::decode(int version, char* buffer, size_t size) {
  if (version < 2) {
    return false;
  }
  StringRef token;
  decode_bytes(buffer, &token);
  token_ = token.to_string();
  return true;
}

bool cass::AuthSuccessResponse::decode(int version, char* buffer, size_t size) {
  if (version < 2) {
    return false;
  }
  StringRef token;
  decode_bytes(buffer, &token);
  token_ = token.to_string();
  return true;
}

} // namespace cass
