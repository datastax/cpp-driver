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

#include "decoder.hpp"
#include "logger.hpp"
#include "value.hpp"

#define CHECK_REMAINING(SIZE, DETAIL)             \
  do {                                            \
    if (remaining_ < static_cast<size_t>(SIZE)) { \
      notify_error(DETAIL, SIZE);                 \
      return false;                               \
    }                                             \
  } while (0)

using namespace datastax::internal::core;

void Decoder::maybe_log_remaining() const {
  if (remaining_ > 0) {
    LOG_TRACE("Data remaining in %s response: %u", type_, static_cast<unsigned int>(remaining_));
  }
}

bool Decoder::decode_inet(Address* output) {
  CHECK_REMAINING(sizeof(uint8_t), "length of inet");

  uint8_t address_length = 0;
  input_ = internal::decode_byte(input_, address_length);
  remaining_ -= sizeof(uint8_t);
  if (address_length > CASS_INET_V6_LENGTH) {
    LOG_ERROR("Invalid inet address length of %d bytes", address_length);
    return false;
  }

  CHECK_REMAINING(address_length, "inet");
  uint8_t address[CASS_INET_V6_LENGTH];
  memcpy(address, input_, address_length);
  input_ += address_length;
  remaining_ -= address_length;

  CHECK_REMAINING(sizeof(int32_t), "port");
  int32_t port = 0;
  input_ = internal::decode_int32(input_, port);
  remaining_ -= sizeof(int32_t);

  *output = Address(address, address_length, port);
  return output->is_valid_and_resolved();
}

bool Decoder::decode_inet(CassInet* output) {
  CHECK_REMAINING(sizeof(uint8_t), "length of inet");

  input_ = internal::decode_byte(input_, output->address_length);
  remaining_ -= sizeof(uint8_t);
  if (output->address_length > CASS_INET_V6_LENGTH) {
    LOG_ERROR("Invalid inet address length of %d bytes", output->address_length);
    return false;
  }

  CHECK_REMAINING(output->address_length, "inet");
  memcpy(output->address, input_, output->address_length);
  input_ += output->address_length;
  remaining_ -= output->address_length;
  return true;
}

bool Decoder::as_inet(const int address_length, CassInet* output) const {
  output->address_length = static_cast<uint8_t>(address_length);
  if (output->address_length > CASS_INET_V6_LENGTH) {
    LOG_ERROR("Invalid inet address length of %d bytes", output->address_length);
    return false;
  }

  CHECK_REMAINING(output->address_length, "inet");
  memcpy(output->address, input_, output->address_length);
  return true;
}

bool Decoder::decode_write_type(CassWriteType& output) {
  StringRef write_type;
  output = CASS_WRITE_TYPE_UNKNOWN;
  if (!decode_string(&write_type)) return false;

  if (write_type == "SIMPLE") {
    output = CASS_WRITE_TYPE_SIMPLE;
  } else if (write_type == "BATCH") {
    output = CASS_WRITE_TYPE_BATCH;
  } else if (write_type == "UNLOGGED_BATCH") {
    output = CASS_WRITE_TYPE_UNLOGGED_BATCH;
  } else if (write_type == "COUNTER") {
    output = CASS_WRITE_TYPE_COUNTER;
  } else if (write_type == "BATCH_LOG") {
    output = CASS_WRITE_TYPE_BATCH_LOG;
  } else if (write_type == "CAS") {
    output = CASS_WRITE_TYPE_CAS;
  } else if (write_type == "VIEW") {
    output = CASS_WRITE_TYPE_VIEW;
  } else if (write_type == "CDC") {
    output = CASS_WRITE_TYPE_CDC;
  } else {
    LOG_WARN("Invalid write type %.*s", (int)write_type.size(), write_type.data());
    return false;
  }

  return true;
}

bool Decoder::decode_warnings(WarningVec& output) {
  if (remaining_ < sizeof(uint16_t)) {
    notify_error("count of warnings", sizeof(uint16_t));
    return false;
  }
  uint16_t count = 0;
  input_ = internal::decode_uint16(input_, count);
  remaining_ -= sizeof(uint16_t);

  for (uint16_t i = 0; i < count; ++i) {
    StringRef warning;

    if (!decode_string(&warning)) return false;
    LOG_WARN("Server-side warning: %.*s", (int)warning.size(), warning.data());
    output.push_back(warning);
  }

  return true;
}

Value Decoder::decode_value(const DataType::ConstPtr& data_type) {
  int32_t size = 0;
  if (!decode_int32(size)) return Value();

  if (size >= 0) {
    Decoder decoder(input_, size, protocol_version_);
    input_ += size;
    remaining_ -= size;

    int32_t count = 0;
    if (!data_type->is_collection()) {
      return Value(data_type, decoder);
    } else if (decoder.decode_int32(count)) {
      return Value(data_type, count, decoder);
    }
    return Value();
  }
  return Value(data_type);
}

bool Decoder::update_value(Value& value) {
  int32_t size = 0;
  if (decode_int32(size)) {
    if (size >= 0) {
      Decoder decoder(input_, size, protocol_version_);
      input_ += size;
      remaining_ -= size;
      return value.update(decoder);
    }
    Decoder decoder;
    return value.update(decoder);
  }
  return false;
}

void Decoder::notify_error(const char* detail, size_t bytes) const {
  if (strlen(type_) == 0) {
    LOG_ERROR("Expected at least %u byte%s to decode %s value", static_cast<unsigned int>(bytes),
              (bytes > 1 ? "s" : ""), detail);
  } else {
    LOG_ERROR("Expected at least %u byte%s to decode %s %s response",
              static_cast<unsigned int>(bytes), (bytes > 1 ? "s" : ""), detail, type_);
  }
}
