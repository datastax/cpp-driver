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

#ifndef __CASS_REQUEST_HPP_INCLUDED__
#define __CASS_REQUEST_HPP_INCLUDED__

#include "buffer.hpp"
#include "macros.hpp"
#include "ref_counted.hpp"

#include "third_party/boost/boost/utility/string_ref.hpp"

namespace cass {

class RequestMessage;

class Request : public RefCounted<Request> {
public:
  enum {
    ENCODE_ERROR_UNSUPPORTED_PROTOCOL = -1
  };

  Request(uint8_t opcode)
      : opcode_(opcode) {}

  virtual ~Request() {}

  uint8_t opcode() const { return opcode_; }

  virtual int encode(int version, BufferVec* bufs) const = 0;

private:
  uint8_t opcode_;

private:
  DISALLOW_COPY_AND_ASSIGN(Request);
};

class RoutableRequest : public Request {
public:
  RoutableRequest(uint8_t opcode)
    : Request(opcode) {}

  RoutableRequest(uint8_t opcode, const std::string& keyspace)
    : Request(opcode)
    , keyspace_(keyspace){}

  virtual const BufferRefs& key_parts() const = 0;

  const std::string& keyspace() const { return keyspace_; }
  void set_keyspace(const std::string& keyspace) { keyspace_ = keyspace; }

private:
  std::string keyspace_;
};

} // namespace cass

#endif
