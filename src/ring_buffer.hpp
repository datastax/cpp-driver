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

// Based on the ring buffer implementation in
// NodeBIO (https://github.com/joyent/node/blob/master/src/node_crypto_bio.h).
// The following is the license for that implementation:

// Copyright Joyent, Inc. and other Node contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

#ifndef DATASTAX_INTERNAL_RING_BUFFER_HPP
#define DATASTAX_INTERNAL_RING_BUFFER_HPP

#include "allocated.hpp"
#include "small_vector.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace rb {

class RingBuffer {
private:
  // NOTE: Size is maximum TLS frame length, this is required if we want
  // to fit whole ClientHello into one Buffer of RingBuffer.
  static const size_t BUFFER_LENGTH = 16 * 1024 + 5;

  class Buffer : public Allocated {
  public:
    Buffer()
        : read_pos_(0)
        , write_pos_(0)
        , next_(NULL) {}

    size_t read_pos_;
    size_t write_pos_;
    Buffer* next_;
    char data_[BUFFER_LENGTH];
  };

public:
  struct Position {
    Position(Buffer* buf, size_t pos)
        : buf(buf)
        , pos(pos) {}
    Buffer* buf;
    size_t pos;
  };

  RingBuffer()
      : length_(0)
      , read_head_(&head_)
      , write_head_(&head_) {
    // Loop head
    head_.next_ = &head_;
  }

  ~RingBuffer();

  Position write_position() { return Position(write_head_, write_head_->write_pos_); }

  // Move read head to next buffer if needed
  void try_move_read_head();

  // Allocate new buffer for write if needed
  void try_allocate_for_write();

  // Read `len` bytes maximum into `out`, return actual number of read bytes
  size_t read(char* out, size_t size);

  // Memory optimization:
  // Deallocate children of write head's child if they're empty
  void free_empty();

  // Return pointers and sizes of multiple internal data chunks available for
  // reading from position
  template <size_t N>
  size_t peek_multiple(Position pos, SmallVector<uv_buf_t, N>* bufs);

  // Find first appearance of `delim` in buffer or `limit` if `delim`
  // wasn't found.
  size_t index_of(char delim, size_t limit);

  // Discard all available data
  void reset();

  // Put `len` bytes from `data` into buffer
  void write(const char* data, size_t size);

  // Return pointer to internal data and amount of
  // contiguous data available for future writes
  char* peek_writable(size_t* size);

  // Commit reserved data
  void commit(size_t size);

  // Return size of buffer in bytes
  size_t inline length() { return length_; }

private:
  size_t length_;
  Buffer head_;
  Buffer* read_head_;
  Buffer* write_head_;
};

template <size_t N>
size_t RingBuffer::peek_multiple(Position pos, SmallVector<uv_buf_t, N>* bufs) {
  Buffer* buf = pos.buf;
  size_t offset = pos.pos;
  size_t total = 0;

  while (true) {
    char* base = (buf->data_ + offset);
    size_t len = (buf->write_pos_ - offset);
    bufs->push_back(uv_buf_init(base, len));
    total += len;

    /* Don't get past write head */
    if (buf == write_head_)
      break;
    else
      buf = buf->next_;

    offset = buf->read_pos_;
  }

  return total;
}

}}} // namespace datastax::internal::rb

#endif
