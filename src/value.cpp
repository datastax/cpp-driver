#include "value.hpp"

#include "serialization.hpp"

namespace cass {

inline char* encode_decimal(char* output, const Decimal& value) {
  char* buffer = encode_int(output, value.scale);
  memcpy(buffer, value.varint->data(), value.varint->size());
  return buffer + value.varint->size();
}

inline char* encode_inet(char* output, const Inet& value) {
  memcpy(output, value.address, value.address_len);
  return encode_int(output + value.address_len, value.port);
}

char*Value::encode(char* output) const {
  switch(type_) {
    case FLOAT: assert(false);
    case DOUBLE: assert(false);
    case BOOL: return encode_byte(encode_int(output, BOOL_SIZE), as_bln() ? 1 : 0);
    case INT32: return encode_int(encode_int(output, INT32_SIZE), as_i32());
    case INT64: return encode_int64(encode_int(output, INT64_SIZE), as_i64());
    case UUID: return encode_uuid(encode_int(output, get_size()), as_uuid());
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
