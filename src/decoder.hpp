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

#ifndef DATASTAX_INTERNAL_DECODER_HPP
#define DATASTAX_INTERNAL_DECODER_HPP

#include "constants.hpp"
#include "data_type.hpp"
#include "protocol.hpp"
#include "serialization.hpp"
#include "small_vector.hpp"

#define CHECK_REMAINING(SIZE, DETAIL)             \
  do {                                            \
    if (remaining_ < static_cast<size_t>(SIZE)) { \
      notify_error(DETAIL, SIZE);                 \
      return false;                               \
    }                                             \
  } while (0)

namespace datastax { namespace internal { namespace core {

typedef Map<String, Vector<String> > StringMultimap;

struct Failure {
  CassInet endpoint;
  uint16_t failurecode;
};
typedef Vector<Failure> FailureVec;

struct CustomPayloadItem {
  CustomPayloadItem(StringRef name, StringRef value)
      : name(name)
      , value(value) {}
  StringRef name;
  StringRef value;
};
typedef SmallVector<CustomPayloadItem, 8> CustomPayloadVec;

typedef SmallVector<StringRef, 8> WarningVec;

class Value; // Forward declaration

/**
 * Decoder class to validate server responses
 */
class Decoder {
  friend class Value;

public:
  Decoder()
      : input_(NULL)
      , length_(0)
      , remaining_(0)
      , type_("") {}

  Decoder(const char* input, size_t length,
          ProtocolVersion protocol_version = ProtocolVersion::highest_supported())
      : protocol_version_(protocol_version)
      , input_(input)
      , length_(length)
      , remaining_(length)
      , type_("") {}

  void maybe_log_remaining() const;

  inline String as_string() const { return String(input_, remaining_); }
  inline StringRef as_string_ref() const { return StringRef(input_, remaining_); }

  inline Vector<char> as_vector() const {
    Vector<char> buffer(remaining_ + 1);
    memcpy(&buffer[0], input_, remaining_);
    buffer[remaining_] = '\0';
    return buffer;
  }

  inline ProtocolVersion protocol_version() const { return protocol_version_; }

  inline void set_type(const char* type) { type_ = type; }

  inline bool decode_byte(uint8_t& output) {
    CHECK_REMAINING(sizeof(uint8_t), "byte");

    input_ = internal::decode_byte(input_, output);
    remaining_ -= sizeof(uint8_t);
    return true;
  }

  inline bool as_byte(uint8_t* output) const {
    CHECK_REMAINING(sizeof(uint8_t), "byte");

    internal::decode_byte(input_, *output);
    return true;
  }

  inline bool as_bool(bool* output) const {
    uint8_t value = 0;
    if (!as_byte(&value)) return false;
    *output = value != 0;
    return true;
  }

  inline bool decode_int8(int8_t& output) {
    CHECK_REMAINING(sizeof(int8_t), "signed byte");

    input_ = internal::decode_int8(input_, output);
    remaining_ -= sizeof(int8_t);
    return true;
  }

  inline bool as_int8(int8_t* output) const {
    CHECK_REMAINING(sizeof(int8_t), "signed byte");

    internal::decode_int8(input_, *output);
    return true;
  }

  inline bool decode_uint16(uint16_t& output) {
    CHECK_REMAINING(sizeof(uint16_t), "unsigned short");

    input_ = internal::decode_uint16(input_, output);
    remaining_ -= sizeof(uint16_t);
    return true;
  }

  inline bool decode_int16(int16_t& output) {
    CHECK_REMAINING(sizeof(int16_t), "short");

    input_ = internal::decode_int16(input_, output);
    remaining_ -= sizeof(int16_t);
    return true;
  }

  inline bool as_int16(int16_t* output) const {
    CHECK_REMAINING(sizeof(int16_t), "short");

    internal::decode_int16(input_, *output);
    return true;
  }

  inline bool decode_uint32(uint32_t& output) {
    CHECK_REMAINING(sizeof(uint32_t), "unsigned int");

    input_ = internal::decode_uint32(input_, output);
    remaining_ -= sizeof(uint32_t);
    return true;
  }

  inline bool as_uint32(uint32_t* output) const {
    CHECK_REMAINING(sizeof(uint32_t), "unsigned int");

    internal::decode_uint32(input_, *output);
    return true;
  }

  inline bool decode_int32(int32_t& output) {
    CHECK_REMAINING(sizeof(int32_t), "int");

    input_ = internal::decode_int32(input_, output);
    remaining_ -= sizeof(int32_t);
    return true;
  }

  inline bool as_int32(int32_t* output) const {
    CHECK_REMAINING(sizeof(int32_t), "int");

    internal::decode_int32(input_, *output);
    return true;
  }

  inline bool decode_int64(int64_t& output) {
    CHECK_REMAINING(sizeof(int64_t), "long");

    input_ = internal::decode_int64(input_, output);
    remaining_ -= sizeof(int64_t);
    return true;
  }

  inline bool as_int64(int64_t* output) const {
    CHECK_REMAINING(sizeof(int64_t), "long");

    internal::decode_int64(input_, *output);
    return true;
  }

  inline bool decode_float(float& output) {
    CHECK_REMAINING(sizeof(int32_t), "float");

    input_ = internal::decode_float(input_, output);
    remaining_ -= sizeof(int32_t);
    return true;
  }

  inline bool as_float(float* output) const {
    CHECK_REMAINING(sizeof(int32_t), "float");

    internal::decode_float(input_, *output);
    return true;
  }

  inline bool decode_double(double& output) {
    CHECK_REMAINING(sizeof(int64_t), "double");

    input_ = internal::decode_double(input_, output);
    remaining_ -= sizeof(int64_t);
    return true;
  }

  inline bool as_double(double* output) const {
    CHECK_REMAINING(sizeof(int64_t), "double");

    internal::decode_double(input_, *output);
    return true;
  }

  inline bool decode_string(const char** output, size_t& size) {
    CHECK_REMAINING(sizeof(uint16_t), "length of string");

    uint16_t string_size = 0;
    input_ = internal::decode_uint16(input_, string_size);
    remaining_ -= sizeof(uint16_t);

    CHECK_REMAINING(string_size, "string");
    *output = input_;
    size = string_size;
    input_ += string_size;
    remaining_ -= string_size;
    return true;
  }

  inline bool decode_string(StringRef* output) {
    const char* str = NULL;
    size_t string_size = 0;
    if (!decode_string(&str, string_size)) return false;

    *output = StringRef(str, string_size);
    return true;
  }

  inline bool decode_long_string(const char** output, size_t& size) {
    CHECK_REMAINING(sizeof(int32_t), "length of long string");

    int32_t string_size = 0;
    input_ = internal::decode_int32(input_, string_size);
    remaining_ -= sizeof(int32_t);

    CHECK_REMAINING(string_size, "string");
    *output = input_;
    size = string_size;
    input_ += string_size;
    remaining_ -= string_size;
    return true;
  }

  inline bool decode_bytes(const char** output, size_t& size) {
    CHECK_REMAINING(sizeof(int32_t), "length of bytes");

    int32_t bytes_size = 0;
    input_ = internal::decode_int32(input_, bytes_size);
    remaining_ -= sizeof(int32_t);

    if (bytes_size < 0) {
      *output = NULL;
      size = 0;
    } else {
      CHECK_REMAINING(bytes_size, "bytes");
      *output = input_;
      size = bytes_size;
      input_ += bytes_size;
      remaining_ -= bytes_size;
    }

    return true;
  }

  inline bool decode_bytes(StringRef* output) {
    const char* bytes = NULL;
    size_t bytes_size = 0;
    if (!decode_bytes(&bytes, bytes_size)) return false;

    *output = StringRef(bytes, bytes_size);
    return true;
  }

  bool decode_inet(Address* output);
  bool decode_inet(CassInet* output);
  bool as_inet(const int address_length, CassInet* output) const;

  inline bool as_inet(const int address_length, const int port, Address* output) const {
    CassInet inet;
    if (!as_inet(address_length, &inet)) return false;
    *output = Address(inet.address, inet.address_length, port);
    return output->is_valid_and_resolved();
  }

  inline bool decode_string_map(Map<String, String>& map) {
    CHECK_REMAINING(sizeof(uint16_t), "size of string map");

    uint16_t pairs = 0;
    input_ = internal::decode_uint16(input_, pairs);
    remaining_ -= sizeof(uint16_t);

    map.clear();
    for (int i = 0; i < pairs; ++i) {
      const char* key = 0;
      size_t key_size = 0;
      const char* value = 0;
      size_t value_size = 0;

      if (!decode_string(&key, key_size)) return false;
      if (!decode_string(&value, value_size)) return false;
      map.insert(std::make_pair(String(key, key_size), String(value, value_size)));
    }

    return true;
  }

  inline bool decode_stringlist(Vector<String>& output) {
    CHECK_REMAINING(sizeof(uint16_t), "count of stringlist");

    uint16_t count = 0;
    input_ = internal::decode_uint16(input_, count);
    remaining_ -= sizeof(uint16_t);

    output.clear();
    output.reserve(count);
    for (int i = 0; i < count; ++i) {
      const char* str = NULL;
      size_t string_size = 0;

      if (!decode_string(&str, string_size)) return false;
      output.push_back(String(str, string_size));
    }

    return true;
  }

  inline bool decode_stringlist(StringRefVec& output) {
    CHECK_REMAINING(sizeof(uint16_t), "count of stringlist");

    uint16_t count = 0;
    input_ = internal::decode_uint16(input_, count);
    remaining_ -= sizeof(uint16_t);

    output.clear();
    output.reserve(count);
    for (int i = 0; i < count; ++i) {
      StringRef string_ref;

      if (!decode_string(&string_ref)) return false;
      output.push_back(string_ref);
    }

    return true;
  }

  inline bool as_stringlist(StringRefVec& output) const {
    size_t total_read = sizeof(uint16_t);
    CHECK_REMAINING(total_read, "count of stringlist");
    uint16_t count = 0;
    const char* pos = internal::decode_uint16(input_, count);

    output.clear();
    output.reserve(count);
    for (int i = 0; i < count; ++i) {
      total_read += sizeof(uint16_t);
      CHECK_REMAINING(total_read, "length of string");
      uint16_t string_size = 0;
      pos = internal::decode_uint16(pos, string_size);

      total_read += string_size;
      CHECK_REMAINING(total_read, "string");
      output.push_back(StringRef(pos, string_size));
      pos += string_size;
    }

    return true;
  }

  inline bool decode_string_multimap(StringMultimap& output) {
    CHECK_REMAINING(sizeof(uint16_t), "pair(s) of string multimap");

    uint16_t pairs = 0;
    input_ = internal::decode_uint16(input_, pairs);
    remaining_ -= sizeof(uint16_t);

    output.clear();
    for (int i = 0; i < pairs; ++i) {
      const char* key = 0;
      size_t key_size = 0;
      Vector<String> value;

      if (!decode_string(&key, key_size)) return false;
      if (!decode_stringlist(value)) return false;
      output.insert(std::make_pair(String(key, key_size), value));
    }

    return true;
  }

  inline bool decode_option(uint16_t& type, const char** class_name, size_t& class_name_size) {
    CHECK_REMAINING(sizeof(uint16_t), "option type");

    input_ = internal::decode_uint16(input_, type);
    remaining_ -= sizeof(uint16_t);

    if (type == CASS_VALUE_TYPE_CUSTOM) {
      if (!decode_string(class_name, class_name_size)) return false;
    }

    return true;
  }

  inline bool decode_uuid(CassUuid* output) {
    size_t bytes = sizeof(uint8_t) * 16;
    CHECK_REMAINING(bytes, "UUID");

    input_ = internal::decode_uuid(input_, output);
    remaining_ -= bytes;
    return true;
  }

  inline bool as_uuid(CassUuid* output) const {
    CHECK_REMAINING(sizeof(uint8_t) * 16, "UUID");

    internal::decode_uuid(input_, output);
    return true;
  }

  inline bool decode_vint(uint64_t& output) {
    CHECK_REMAINING(sizeof(uint8_t), "vint extra bytes");

    uint8_t first_byte = *input_++;
    remaining_ -= sizeof(uint8_t);

    if (first_byte <= 127) {
      // If this is a multibyte vint, at least the MSB of the first byte
      // will be set. Since that's not the case, this is a one-byte value.
      output = first_byte;
    } else {
      // The number of consecutive most significant bits of the first-byte tell us how
      // many additional bytes are in this vint. Count them like this:
      // 1. Invert the firstByte so that all leading 1s become 0s.
      // 2. Count the number of leading zeros; num_leading_zeros assumes a 64-bit long.
      // 3. We care about leading 0s in the byte, not int, so subtract out the
      //    appropriate number of extra bits (56 for a 64-bit int).

      // We mask out high-order bits to prevent sign-extension as the value is placed in a 64-bit
      // arg to the num_leading_zeros function.
      int extra_bytes = internal::num_leading_zeros(~first_byte & 0xff) - 56;
      CHECK_REMAINING(extra_bytes, "vint value");

      // Build up the vint value one byte at a time from the data bytes.
      // The firstByte contains size as well as the most significant bits of
      // the value. Extract just the value.
      output = first_byte & (0xff >> extra_bytes);
      for (int i = 0; i < extra_bytes; ++i) {
        uint8_t vint_byte = *input_++;
        output <<= 8;
        output |= vint_byte & 0xff;
      }
      remaining_ -= extra_bytes;
    }

    return true;
  }

  inline bool as_decimal(const uint8_t** output, size_t* size, int32_t* scale) {
    CHECK_REMAINING(sizeof(int32_t), "decimal scale");

    const char* pos = internal::decode_int32(input_, *scale);

    if (remaining_ - sizeof(int32_t) <= 0) {
      notify_error("decimal value", remaining_ - sizeof(int32_t));
      return false;
    }
    *output = reinterpret_cast<const uint8_t*>(pos);
    *size = remaining_ - sizeof(int32_t);
    return true;
  }

  inline bool as_duration(int32_t* out_months, int32_t* out_days, int64_t* out_nanos) const {
    Decoder decoder = Decoder(input_, remaining_, protocol_version_);
    uint64_t decoded = 0;

    if (!decoder.decode_vint(decoded)) return false;
    *out_months = static_cast<int32_t>(internal::decode_zig_zag(decoded));

    if (!decoder.decode_vint(decoded)) return false;
    *out_days = static_cast<int32_t>(internal::decode_zig_zag(decoded));

    if (!decoder.decode_vint(decoded)) return false;
    *out_nanos = static_cast<int64_t>(internal::decode_zig_zag(decoded));

    return true;
  }

  inline bool decode_custom_payload(CustomPayloadVec& output) {
    if (remaining_ < sizeof(uint16_t)) {
      notify_error("count of custom payload", sizeof(uint16_t));
      return false;
    }
    uint16_t count = 0;
    input_ = internal::decode_uint16(input_, count);
    remaining_ -= sizeof(uint16_t);

    for (uint16_t i = 0; i < count; ++i) {
      StringRef name;
      StringRef value;

      if (!decode_string(&name)) return false;
      if (!decode_bytes(&value)) return false;
      output.push_back(CustomPayloadItem(name, value));
    }

    return true;
  }

  inline bool decode_failures(FailureVec& output, int32_t& output_size) {
    if (remaining_ < sizeof(int32_t)) {
      notify_error("count of failures", sizeof(int32_t));
      return false;
    }
    input_ = internal::decode_int32(input_, output_size);
    remaining_ -= sizeof(int32_t);

    // Format: <endpoint><failurecode>
    // where:
    // <endpoint> is a [inetaddr]
    // <failurecode> is a [short]
    if (protocol_version_ >= CASS_PROTOCOL_VERSION_V5) {
      output.reserve(output_size);
      for (int32_t i = 0; i < output_size; ++i) {
        Failure failure;

        if (!decode_inet(&failure.endpoint)) return false;
        if (!decode_uint16(failure.failurecode)) return false;
        output.push_back(failure);
      }
    }

    return true;
  }

  bool decode_write_type(CassWriteType& output);
  bool decode_warnings(WarningVec& output);

  Value decode_value(const DataType::ConstPtr& data_type);
  bool update_value(Value& value);

  bool is_null() const { return input_ == NULL; }

protected:
  // Testing only
  inline const char* buffer() const { return input_; }

  inline size_t remaining() const { return remaining_; }

private:
  ProtocolVersion protocol_version_;
  const char* input_;
  size_t length_;
  size_t remaining_;
  const char* type_;

  void notify_error(const char* detail, size_t bytes) const;
};

}}} // namespace datastax::internal::core

#undef CHECK_REMAINING

#endif // DATASTAX_INTERNAL_DECODER_HPP
