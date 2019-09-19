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

#include "supported_response.hpp"

#include "logger.hpp"
#include "serialization.hpp"
#include "utils.hpp"

#include <algorithm>

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

bool SupportedResponse::decode(Decoder& decoder) {
  decoder.set_type("supported");
  StringMultimap supported_options;
  CHECK_RESULT(decoder.decode_string_multimap(supported_options));
  decoder.maybe_log_remaining();

  // Force keys to be uppercase
  for (StringMultimap::iterator it = supported_options.begin(), end = supported_options.end();
       it != end; ++it) {
    String key = it->first;
    std::transform(key.begin(), key.end(), key.begin(), toupper);
    supported_options_[key] = it->second;
  }

  return true;
}
