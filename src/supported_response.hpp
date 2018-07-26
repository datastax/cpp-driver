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

#ifndef __CASS_SUPPORTED_RESPONSE_HPP_INCLUDED__
#define __CASS_SUPPORTED_RESPONSE_HPP_INCLUDED__

#include "response.hpp"
#include "constants.hpp"
#include "string.hpp"
#include "vector.hpp"

namespace cass {

class SupportedResponse : public Response {
public:
  SupportedResponse()
      : Response(CQL_OPCODE_SUPPORTED) {}

  virtual bool decode(Decoder& decoder);

  const Vector<String> compression() { return compression_; }
  const Vector<String> cql_versions() { return cql_versions_; }
  const Vector<String> protocol_versions() { return protocol_versions_; }

private:
  Vector<String> compression_;
  Vector<String> cql_versions_;
  Vector<String> protocol_versions_;
};

} // namespace cass

#endif
