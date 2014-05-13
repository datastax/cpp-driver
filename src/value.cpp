#include "value.hpp"

#include <limits>

#include "serialization.hpp"

namespace cass {

inline char* encode_float(char* output, float value) {
  assert(std::numeric_limits<float>::is_iec559);
  assert(sizeof(float) == sizeof(int32_t));
  return encode_int(output, *reinterpret_cast<int32_t*>(&value));
}

inline char* encode_double(char* output, double value) {
  assert(std::numeric_limits<double>::is_iec559);
  assert(sizeof(double) == sizeof(int64_t));
  return encode_int64(output, *reinterpret_cast<int64_t*>(&value));
}

inline char* encode_decimal(char* output, const Decimal& value) {
  char* buffer = encode_int(output, value.scale);
  memcpy(buffer, value.varint->data(), value.varint->size());
  return buffer + value.varint->size();
}

inline char* encode_inet(char* output, const Inet& value) {
  memcpy(output, value.address, value.address_len);
  return output + value.address_len;
}

Value::Value(const Value& v)
  : type_(v.type_) {
  if(type_ == BYTES) {
    basic().bytes = new std::string(*v.basic().bytes);
  } else if(type_ == DECIMAL) {
    basic().decimal.scale = v.basic().decimal.scale;
    basic().decimal.varint = new std::string(*v.basic().decimal.varint);
  } else {
    data_ = v.data_;
  }
}

Value& Value::operator=(const Value& v) {
  type_ = v.type_;
  if(type_ == BYTES) {
    basic().bytes = new std::string(*v.basic().bytes);
  } else if(type_ == DECIMAL) {
    basic().decimal.scale = v.basic().decimal.scale;
    basic().decimal.varint = new std::string(*v.basic().decimal.varint);
  } else {
    data_ = v.data_;
  }
  return *this;
}

Value::Value(Value&& v)
    : type_(v.type_) {
    if(type_ == BYTES) {
      basic().bytes = v.basic().bytes;
      v.basic().bytes = nullptr;
    } else if(type_ == DECIMAL) {
      basic().decimal.scale = v.basic().decimal.scale;
      basic().decimal.varint = v.basic().decimal.varint;
      v.basic().decimal.varint = nullptr;
    } else {
      data_ = v.data_;
    }
}

Value& Value::operator=(Value&& v) {
  type_ = v.type_;
  if(type_ == BYTES) {
    basic().bytes = v.basic().bytes;
    v.basic().bytes = nullptr;
  } else if(type_ == DECIMAL) {
    basic().decimal.scale = v.basic().decimal.scale;
    basic().decimal.varint = v.basic().decimal.varint;
    v.basic().decimal.varint = nullptr;
  } else {
    data_ = v.data_;
  }
  return *this;
}

char* Value::encode(char* output) const {
  switch(type_) {
    case FLOAT: return encode_float(encode_int(output, FLOAT_SIZE), as_flt());
    case DOUBLE: return encode_double(encode_int(output, DOUBLE_SIZE), as_dbl());
    case BOOL: return encode_byte(encode_int(output, BOOL_SIZE), as_bln() ? 1 : 0);
    case INT32: return encode_int(encode_int(output, INT32_SIZE), as_i32());
    case INT64: return encode_int64(encode_int(output, INT64_SIZE), as_i64());
    case UUID: return encode_uuid(encode_int(output, UUID_SIZE), as_uuid());
    case INET: return encode_inet(encode_int(output, get_size()), as_inet());
    case BYTES: return encode_long_string(output, as_bytes()->data(), as_bytes()->size());
    case DECIMAL: return encode_decimal(encode_int(output, get_size()), as_decimal());
    case LIST: assert(false);
    case MAP: assert(false);
    case SET: assert(false);
    case UDT: assert(false);
    default: assert(false);
  }
}

} // namespace cass
