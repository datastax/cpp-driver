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

#ifndef __CASS_OPTIONS_REQUEST_HPP_INCLUDED__
#define __CASS_OPTIONS_REQUEST_HPP_INCLUDED__

#include "message_body.hpp"

namespace cass {

struct OptionsRequest : public MessageBody {
  OptionsRequest()
    : MessageBody(CQL_OPCODE_OPTIONS) {}

  bool
  consume(
      char*  buffer,
      size_t size) {
    (void) buffer;
    (void) size;
    return true;
  }

  bool
  prepare(
      size_t  reserved,
      char**  output,
      size_t& size) {
    return true;
  }
};

} // namespace cass

#endif
