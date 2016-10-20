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

#ifndef __CASS_REQUEST_HPP_INCLUDED__
#define __CASS_REQUEST_HPP_INCLUDED__

#include "buffer.hpp"
#include "cassandra.h"
#include "constants.hpp"
#include "external.hpp"
#include "macros.hpp"
#include "ref_counted.hpp"
#include "retry_policy.hpp"
#include "string_ref.hpp"

#include <stdint.h>
#include <utility>
#include <map>

namespace cass {

class RequestCallback;
class RequestMessage;

class CustomPayload : public RefCounted<CustomPayload> {
public:
  typedef SharedRefPtr<const CustomPayload> ConstPtr;

  virtual ~CustomPayload() { }

  void set(const char* name, size_t name_length,
           const uint8_t* value, size_t value_size);

  void remove(const char* name, size_t name_length) {
    items_.erase(std::string(name, name_length));
  }

  int32_t encode(BufferVec* bufs) const;

private:
  typedef std::map<std::string, Buffer> ItemMap;
  ItemMap items_;
};

class Request : public RefCounted<Request> {
public:
  typedef SharedRefPtr<const Request> ConstPtr;

  enum {
    REQUEST_ERROR_UNSUPPORTED_PROTOCOL = -1,
    REQUEST_ERROR_BATCH_WITH_NAMED_VALUES = -2,
    REQUEST_ERROR_PARAMETER_UNSET = -3,
    REQUEST_ERROR_NO_AVAILABLE_STREAM_IDS = -4,
    REQUEST_ERROR_CANCELLED = -5
  };

  static const CassConsistency DEFAULT_CONSISTENCY = CASS_CONSISTENCY_LOCAL_ONE;

  typedef std::map<const void*, Buffer> EncodingCache;

  Request(uint8_t opcode)
      : opcode_(opcode)
      , consistency_(DEFAULT_CONSISTENCY)
      , serial_consistency_(CASS_CONSISTENCY_ANY)
      , timestamp_(CASS_INT64_MIN)
      , is_idempotent_(false)
      , record_attempted_addresses_(false)
      , request_timeout_ms_(CASS_UINT64_MAX) { } // Disabled (use the cluster-level timeout)

  virtual ~Request() { }

  uint8_t opcode() const { return opcode_; }

  CassConsistency consistency() const { return consistency_; }

  void set_consistency(CassConsistency consistency) { consistency_ = consistency; }

  CassConsistency serial_consistency() const { return serial_consistency_; }

  void set_serial_consistency(CassConsistency serial_consistency) {
    serial_consistency_ = serial_consistency;
  }

  int64_t timestamp() const { return timestamp_; }

  void set_timestamp(int64_t timestamp) { timestamp_ = timestamp; }

  bool is_idempotent() const { return is_idempotent_; }

  void set_is_idempotent(bool is_idempotent) { is_idempotent_ = is_idempotent; }

  bool record_attempted_addresses() const { return record_attempted_addresses_; }

  void set_record_attempted_addresses(bool record_attempted_addresses) {
    record_attempted_addresses_ = record_attempted_addresses;
  }

  uint64_t request_timeout_ms(uint64_t default_timeout_ms) const;

  void set_request_timeout_ms(uint64_t request_timeout_ms) {
    request_timeout_ms_ = request_timeout_ms;
  }

  RetryPolicy* retry_policy() const {
    return retry_policy_.get();
  }

  void set_retry_policy(RetryPolicy* retry_policy) {
    retry_policy_.reset(retry_policy);
  }

  const CustomPayload::ConstPtr& custom_payload() const {
    return custom_payload_;
  }

  void set_custom_payload(const CustomPayload* payload) {
    custom_payload_.reset(payload);
  }

  virtual int encode(int version, RequestCallback* callback, BufferVec* bufs) const = 0;

private:
  uint8_t opcode_;
  CassConsistency consistency_;
  CassConsistency serial_consistency_;
  int64_t timestamp_;
  bool is_idempotent_;
  bool record_attempted_addresses_;
  uint64_t request_timeout_ms_;
  RetryPolicy::Ptr retry_policy_;
  CustomPayload::ConstPtr custom_payload_;

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

EXTERNAL_TYPE(cass::CustomPayload, CassCustomPayload)


#endif
