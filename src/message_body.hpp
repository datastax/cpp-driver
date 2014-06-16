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

#ifndef __CASS_BODY_HPP_INCLUDED__
#define __CASS_BODY_HPP_INCLUDED__

#include <memory>
#include <atomic>

#include "common.hpp"
#include "future.hpp"

namespace cass {

class MessageBody : public RefCounted<MessageBody> {
public:
  MessageBody(uint8_t opcode)
      : opcode_(opcode) {}

  virtual ~MessageBody() {}

  DISALLOW_COPY_AND_ASSIGN(MessageBody);

  uint8_t opcode() const { return opcode_; }
  char* buffer() { return buffer_.get(); }
  void set_buffer(char* buffer) { buffer_.reset(buffer); }

  virtual bool consume(char* buffer, size_t size) { return false; }

  virtual bool prepare(size_t reserved, char** output, size_t& size) {
    return false;
  }

private:
  uint8_t opcode_;
  std::unique_ptr<char[]> buffer_;
};

} // namespace cass

#endif
