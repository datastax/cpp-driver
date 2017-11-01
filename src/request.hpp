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

// A grouping of common request settings that can be easily inherited (copied).
// Important: If a member is added to this structure "cassandra.h" should also
// be updated to reflect the new inherited setting(s).
struct RequestSettings {
  RequestSettings()
    : consistency(CASS_CONSISTENCY_UNKNOWN)
    , serial_consistency(CASS_CONSISTENCY_UNKNOWN)
    , request_timeout_ms(CASS_UINT64_MAX)
    , is_idempotent(false) { }
  CassConsistency consistency;
  CassConsistency serial_consistency;
  uint64_t request_timeout_ms;
  RetryPolicy::Ptr retry_policy;
  bool is_idempotent;
  std::string keyspace;
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

  Request(uint8_t opcode)
      : opcode_(opcode)
      , timestamp_(CASS_INT64_MIN)
      , record_attempted_addresses_(false) { }

  virtual ~Request() { }

  uint8_t opcode() const { return opcode_; }

  const RequestSettings& settings() const { return settings_; }

  void set_settings(const RequestSettings& settings) { settings_ = settings; }

  CassConsistency consistency() const { return settings_.consistency; }

  void set_consistency(CassConsistency consistency) { settings_.consistency = consistency; }

  CassConsistency serial_consistency() const { return settings_.serial_consistency; }

  void set_serial_consistency(CassConsistency serial_consistency) {
    settings_.serial_consistency = serial_consistency;
  }

  uint64_t request_timeout_ms() const { return settings_.request_timeout_ms; }

  void set_request_timeout_ms(uint64_t request_timeout_ms) {
    settings_.request_timeout_ms = request_timeout_ms;
  }

  const RetryPolicy::Ptr& retry_policy() const {
    return settings_.retry_policy;
  }

  void set_retry_policy(RetryPolicy* retry_policy) {
    settings_.retry_policy.reset(retry_policy);
  }

  bool is_idempotent() const {
    // Prepare requests are idempotent and should be retried regardless of the
    // setting inherited from an existing statement.
    return opcode_ == CQL_OPCODE_PREPARE || settings_.is_idempotent;
  }

  void set_is_idempotent(bool is_idempotent) { settings_.is_idempotent = is_idempotent; }

  const std::string& keyspace() const { return settings_.keyspace; }

  void set_keyspace(const std::string& keyspace) { settings_.keyspace = keyspace; }

  int64_t timestamp() const { return timestamp_; }

  void set_timestamp(int64_t timestamp) { timestamp_ = timestamp; }

  bool record_attempted_addresses() const { return record_attempted_addresses_; }

  void set_record_attempted_addresses(bool record_attempted_addresses) {
    record_attempted_addresses_ = record_attempted_addresses;
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
  RequestSettings settings_;
  int64_t timestamp_;
  bool record_attempted_addresses_;
  CustomPayload::ConstPtr custom_payload_;

private:
  DISALLOW_COPY_AND_ASSIGN(Request);
};

class RoutableRequest : public Request {
public:
  RoutableRequest(uint8_t opcode)
    : Request(opcode) { }

  virtual bool get_routing_key(std::string* routing_key) const = 0;
};

} // namespace cass

EXTERNAL_TYPE(cass::CustomPayload, CassCustomPayload)


#endif
