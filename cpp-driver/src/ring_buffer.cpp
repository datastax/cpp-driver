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

#include "ring_buffer.hpp"

#include <assert.h>
#include <string.h>

using namespace datastax::internal::rb;

RingBuffer::~RingBuffer() {
  Buffer* current = head_.next_;
  while (current != &head_) {
    Buffer* next = current->next_;
    delete current;
    current = next;
  }

  read_head_ = NULL;
  write_head_ = NULL;
}

void RingBuffer::try_move_read_head() {
  // `read_pos_` and `write_pos_` means the position of the reader and writer
  // inside the buffer, respectively. When they're equal - its safe to reset
  // them, because both reader and writer will continue doing their stuff
  // from new (zero) positions.
  while (read_head_->read_pos_ != 0 && read_head_->read_pos_ == read_head_->write_pos_) {
    // Reset positions
    read_head_->read_pos_ = 0;
    read_head_->write_pos_ = 0;

    // Move read_head_ forward, just in case if there're still some data to
    // read in the next buffer.
    if (read_head_ != write_head_) read_head_ = read_head_->next_;
  }
}

size_t RingBuffer::read(char* out, size_t size) {
  size_t bytes_read = 0;
  size_t expected = length_ > size ? size : length_;
  size_t offset = 0;
  size_t left = size;

  while (bytes_read < expected) {
    assert(read_head_->read_pos_ <= read_head_->write_pos_);
    size_t avail = read_head_->write_pos_ - read_head_->read_pos_;
    if (avail > left) avail = left;

    // Copy data
    if (out != NULL) memcpy(out + offset, read_head_->data_ + read_head_->read_pos_, avail);
    read_head_->read_pos_ += avail;

    // Move pointers
    bytes_read += avail;
    offset += avail;
    left -= avail;

    try_move_read_head();
  }
  assert(expected == bytes_read);
  length_ -= bytes_read;

  // Free all empty buffers, but write_head's child
  free_empty();

  return bytes_read;
}

void RingBuffer::free_empty() {
  Buffer* child = write_head_->next_;
  if (child == write_head_ || child == read_head_) return;
  Buffer* cur = child->next_;
  if (cur == write_head_ || cur == read_head_) return;

  Buffer* prev = child;
  while (cur != read_head_) {
    // Skip embedded buffer, and continue deallocating again starting from it
    if (cur == &head_) {
      prev->next_ = cur;
      prev = cur;
      cur = head_.next_;
      continue;
    }
    assert(cur != write_head_);
    assert(cur->write_pos_ == cur->read_pos_);

    Buffer* next = cur->next_;
    delete cur;
    cur = next;
  }
  assert(prev == child || prev == &head_);
  prev->next_ = cur;
}

size_t RingBuffer::index_of(char delim, size_t limit) {
  size_t bytes_read = 0;
  size_t max = length_ > limit ? limit : length_;
  size_t left = limit;
  Buffer* current = read_head_;

  while (bytes_read < max) {
    assert(current->read_pos_ <= current->write_pos_);
    size_t avail = current->write_pos_ - current->read_pos_;
    if (avail > left) avail = left;

    // Walk through data
    char* tmp = current->data_ + current->read_pos_;
    size_t off = 0;
    while (off < avail && *tmp != delim) {
      off++;
      tmp++;
    }

    // Move pointers
    bytes_read += off;
    left -= off;

    // Found `delim`
    if (off != avail) {
      return bytes_read;
    }

    // Move to next buffer
    if (current->read_pos_ + avail == BUFFER_LENGTH) {
      current = current->next_;
    }
  }
  assert(max == bytes_read);

  return max;
}

void RingBuffer::write(const char* data, size_t size) {
  size_t offset = 0;
  size_t left = size;
  while (left > 0) {
    size_t to_write = left;
    assert(write_head_->write_pos_ <= BUFFER_LENGTH);
    size_t avail = BUFFER_LENGTH - write_head_->write_pos_;

    if (to_write > avail) to_write = avail;

    // Copy data
    memcpy(write_head_->data_ + write_head_->write_pos_, data + offset, to_write);

    // Move pointers
    left -= to_write;
    offset += to_write;
    length_ += to_write;
    write_head_->write_pos_ += to_write;
    assert(write_head_->write_pos_ <= BUFFER_LENGTH);

    // Go to next buffer if there still are some bytes to write
    if (left != 0) {
      assert(write_head_->write_pos_ == BUFFER_LENGTH);
      try_allocate_for_write();
      write_head_ = write_head_->next_;

      // Additionally, since we're moved to the next buffer, read head
      // may be moved as well.
      try_move_read_head();
    }
  }
  assert(left == 0);
}

char* RingBuffer::peek_writable(size_t* size) {
  size_t available = BUFFER_LENGTH - write_head_->write_pos_;
  if (*size != 0 && available > *size)
    available = *size;
  else
    *size = available;

  return write_head_->data_ + write_head_->write_pos_;
}

void RingBuffer::commit(size_t size) {
  write_head_->write_pos_ += size;
  length_ += size;
  assert(write_head_->write_pos_ <= BUFFER_LENGTH);

  // Allocate new buffer if write head is full,
  // and there're no other place to go
  try_allocate_for_write();
  if (write_head_->write_pos_ == BUFFER_LENGTH) {
    write_head_ = write_head_->next_;

    // Additionally, since we're moved to the next buffer, read head
    // may be moved as well.
    try_move_read_head();
  }
}

void RingBuffer::try_allocate_for_write() {
  // If write head is full, next buffer is either read head or not empty.
  if (write_head_->write_pos_ == BUFFER_LENGTH &&
      (write_head_->next_ == read_head_ || write_head_->next_->write_pos_ != 0)) {
    Buffer* next = new Buffer();
    next->next_ = write_head_->next_;
    write_head_->next_ = next;
  }
}

void RingBuffer::reset() {
  while (read_head_->read_pos_ != read_head_->write_pos_) {
    assert(read_head_->write_pos_ > read_head_->read_pos_);

    length_ -= read_head_->write_pos_ - read_head_->read_pos_;
    read_head_->write_pos_ = 0;
    read_head_->read_pos_ = 0;

    read_head_ = read_head_->next_;
  }
  write_head_ = read_head_;
  assert(length_ == 0);
}
