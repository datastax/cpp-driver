/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#ifndef DATASTAX_INTERNAL_SUPPORTED_RESPONSE_HPP
#define DATASTAX_INTERNAL_SUPPORTED_RESPONSE_HPP

#include "constants.hpp"
#include "response.hpp"
#include "string.hpp"
#include "vector.hpp"

namespace datastax { namespace internal { namespace core {

class SupportedResponse : public Response {
public:
  SupportedResponse()
      : Response(CQL_OPCODE_SUPPORTED) {}

  virtual bool decode(Decoder& decoder);

  /**
   * Get the supported options provided by the server.
   *
   * @return Supported options; all keys are normalized (uppercase).
   */
  const StringMultimap& supported_options() const { return supported_options_; }

private:
  StringMultimap supported_options_;
};

}}} // namespace datastax::internal::core

#endif
