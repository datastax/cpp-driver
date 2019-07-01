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

#include "auth_responses.hpp"

#include "serialization.hpp"

using namespace datastax::internal::core;

bool AuthenticateResponse::decode(Decoder& decoder) {
  decoder.set_type("authentication");
  StringRef class_name;

  CHECK_RESULT(decoder.decode_string(&class_name));
  class_name_ = class_name.to_string();
  decoder.maybe_log_remaining();
  return true;
}

bool AuthChallengeResponse::decode(Decoder& decoder) {
  decoder.set_type("authentication challenge");
  StringRef token;

  CHECK_RESULT(decoder.decode_bytes(&token));
  token_ = token.to_string();
  decoder.maybe_log_remaining();
  return true;
}

bool AuthSuccessResponse::decode(Decoder& decoder) {
  decoder.set_type("authentication success");
  StringRef token;

  CHECK_RESULT(decoder.decode_bytes(&token));
  token_ = token.to_string();
  decoder.maybe_log_remaining();
  return true;
}
