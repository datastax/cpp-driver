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

#ifndef DATASTAX_INTERNAL_REGISTER_REQUEST_HPP
#define DATASTAX_INTERNAL_REGISTER_REQUEST_HPP

#include "constants.hpp"
#include "request.hpp"

namespace datastax { namespace internal { namespace core {

class RegisterRequest : public Request {
public:
  RegisterRequest(int event_types)
      : Request(CQL_OPCODE_REGISTER)
      , event_types_(event_types) {}

private:
  int encode(ProtocolVersion version, RequestCallback* callback, BufferVec* bufs) const;

  int event_types_;
};

}}} // namespace datastax::internal::core
#endif
