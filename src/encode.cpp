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

#include "encode.hpp"
#include "utils.hpp"

namespace cass {

static Buffer encode_internal(CassDuration value, bool with_length) {
  // Each duration attribute needs to be converted to zigzag form. Use an array to make it
  // easy to do the same encoding ops on all three values.
  uint64_t zigzag_values[3];

  // We need varint sizes for each attribute.
  size_t varint_sizes[3];

  zigzag_values[0] = cass::encode_zig_zag(value.months);
  zigzag_values[1] = cass::encode_zig_zag(value.days);
  zigzag_values[2] = cass::encode_zig_zag(value.nanos);

  // We also need the total size of all three varint's.
  size_t data_size = 0;
  for (int i = 0; i < 3; ++i) {
    varint_sizes[i] = cass::varint_size(zigzag_values[i]);
    data_size += varint_sizes[i];
  }

  // Allocate our data buffer and then start populating it. If we're including the length,
  // allocate extra space for that.
  Buffer buf(with_length ? sizeof(int32_t) + data_size : data_size);

  // Write the length if with_length is set; this means subsequent data is written afterwards
  // in buf. Use pos to set our next write position properly regardless of with_length. Since we
  // don't write sequentially to the buffer (we jump around a bit) the easiest thing to do is grab
  // a pointer to the underlying data buffer and manipulate it.
  size_t pos = with_length ? buf.encode_int32(0, data_size) : 0;
  char* cur_byte = buf.data() + pos;

  for (int i = 0; i < 3; ++i) {
    if (varint_sizes[i] == 1) {
      // This is just a one byte value; write it and move on.
      *cur_byte++ = zigzag_values[i];
      continue;
    }

    // Write the bytes of zigzag value to the data array with most significant byte
    // in first byte of buffer (cur_byte), and so on.
    for (int j = varint_sizes[i] - 1 ; j >= 0 ; --j) {
      *(cur_byte + j) = zigzag_values[i] & 0xff;
      zigzag_values[i] >>= 8;
    }

    // Now mix in the size of the varint into the first byte of the buffer,
    // setting "size-1" higher order bits.
    *cur_byte |= ~(0xff >> (varint_sizes[i] - 1));

    // Move cur_byte forward, ready for the next attribute.
    cur_byte += varint_sizes[i];
  }
  return buf;
}

Buffer encode(CassDuration value) {
  return encode_internal(value, false);
}

Buffer encode_with_length(CassDuration value) {
  return encode_internal(value, true);
}

} // namespace cass
