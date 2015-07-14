/*
  Copyright (c) 2014-2015 DataStax

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
#include "constants.hpp"
#include "macros.hpp"
#include "ref_counted.hpp"
#include "retry_policy.hpp"
#include "string_ref.hpp"

#include <stdint.h>

namespace cass {

class Handler;
class RequestMessage;

class Request : public RefCounted<Request> {
public:
  enum {
    ENCODE_ERROR_UNSUPPORTED_PROTOCOL = -1,
    ENCODE_ERROR_BATCH_WITH_NAMED_VALUES = -2
  };

  typedef std::map<const void*, Buffer> EncodingCache;

  Request(uint8_t opcode)
      : opcode_(opcode)
      , consistency_(CASS_CONSISTENCY_ONE)
      , serial_consistency_(CASS_CONSISTENCY_ANY)
      , default_timestamp_(CASS_INT64_MIN) {}

  virtual ~Request() {}

  uint8_t opcode() const { return opcode_; }

  CassConsistency consistency() const { return consistency_; }

  void set_consistency(CassConsistency consistency) { consistency_ = consistency; }

  CassConsistency serial_consistency() const { return serial_consistency_; }

  void set_serial_consistency(CassConsistency serial_consistency) {
    serial_consistency_ = serial_consistency;
  }

  int64_t default_timestamp() const { return default_timestamp_; }

  void set_default_timestamp(int64_t timestamp) { default_timestamp_ = timestamp; }

  RetryPolicy* retry_policy() const {
    return retry_policy_.get();
  }

  void set_retry_policy(RetryPolicy* retry_policy) {
    retry_policy_.reset(retry_policy);
  }

  virtual int encode(int version, Handler* handler, BufferVec* bufs) const = 0;

private:
  uint8_t opcode_;
  CassConsistency consistency_;
  CassConsistency serial_consistency_;
  int64_t default_timestamp_;
  SharedRefPtr<RetryPolicy> retry_policy_;

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

  virtual bool get_routing_key(std::string* routing_key, EncodingCache* cache) const = 0;

  const std::string& keyspace() const { return keyspace_; }
  void set_keyspace(const std::string& keyspace) { keyspace_ = keyspace; }

private:
  std::string keyspace_;
};

} // namespace cass

#endif
