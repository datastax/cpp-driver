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

#include "ring_buffer_bio.hpp"

#include <string.h>

namespace cass {
namespace rb {

const BIO_METHOD RingBufferBio::method = {
  BIO_TYPE_MEM,
  "ring buffer",
  RingBufferBio::write,
  RingBufferBio::read,
  RingBufferBio::puts,
  RingBufferBio::gets,
  RingBufferBio::ctrl,
  RingBufferBio::create,
  RingBufferBio::destroy,
  NULL
};

BIO* RingBufferBio::create(RingBuffer* ring_buffer) {
  // The const_cast doesn't violate const correctness.  OpenSSL's usage of
  // BIO_METHOD is effectively const but BIO_new() takes a non-const argument.
  BIO* bio =  BIO_new(const_cast<BIO_METHOD*>(&method));
  bio->ptr = ring_buffer;
  return bio;
}

int RingBufferBio::create(BIO* bio) {
  // XXX Why am I doing it?!
  bio->shutdown = 1;
  bio->init = 1;
  bio->num = -1;

  return 1;
}

int RingBufferBio::destroy(BIO* bio) {
  if (bio == NULL)
    return 0;

  return 1;
}

int RingBufferBio::read(BIO* bio, char* out, int len) {
  int bytes;
  BIO_clear_retry_flags(bio);

  bytes = from_bio(bio)->read(out, len);

  if (bytes == 0) {
    bytes = bio->num;
    if (bytes != 0) {
      BIO_set_retry_read(bio);
    }
  }

  return bytes;
}

int RingBufferBio::write(BIO* bio, const char* data, int len) {
  BIO_clear_retry_flags(bio);

  from_bio(bio)->write(data, len);

  return len;
}

int RingBufferBio::puts(BIO* bio, const char* str) {
  return write(bio, str, strlen(str));
}

int RingBufferBio::gets(BIO* bio, char* out, int size) {
  RingBuffer* ring_buffer =  from_bio(bio);

  if (ring_buffer->length() == 0)
    return 0;

  int i = ring_buffer->index_of('\n', size);

  // Include '\n', if it's there.  If not, don't read off the end.
  if (i < size && i >= 0 && static_cast<size_t>(i) < ring_buffer->length())
    i++;

  // Shift `i` a bit to NULL-terminate string later
  if (size == i)
    i--;

  // Flush read data
  ring_buffer->read(out, i);

  out[i] = 0;

  return i;
}

long RingBufferBio::ctrl(BIO* bio, int cmd, long num, void* ptr) {
  RingBuffer* ring_buffer;
  long ret;

  ring_buffer = from_bio(bio);
  ret = 1;

  switch (cmd) {
    case BIO_CTRL_RESET:
      ring_buffer->reset();
      break;
    case BIO_CTRL_EOF:
      ret = ring_buffer->length() == 0;
      break;
    case BIO_C_SET_BUF_MEM_EOF_RETURN:
      bio->num = num;
      break;
    case BIO_CTRL_INFO:
      ret = ring_buffer->length();
      if (ptr != NULL)
        *reinterpret_cast<void**>(ptr) = NULL;
      break;
    case BIO_C_SET_BUF_MEM:
      assert(0 && "Can't use SET_BUF_MEM_PTR with RingBufferBio");
      abort();
      break;
    case BIO_C_GET_BUF_MEM_PTR:
      assert(0 && "Can't use GET_BUF_MEM_PTR with RingBufferBio");
      ret = 0;
      break;
    case BIO_CTRL_GET_CLOSE:
      ret = bio->shutdown;
      break;
    case BIO_CTRL_SET_CLOSE:
      bio->shutdown = num;
      break;
    case BIO_CTRL_WPENDING:
      ret = 0;
      break;
    case BIO_CTRL_PENDING:
      ret = ring_buffer->length();
      break;
    case BIO_CTRL_DUP:
    case BIO_CTRL_FLUSH:
      ret = 1;
      break;
    case BIO_CTRL_PUSH:
    case BIO_CTRL_POP:
    default:
      ret = 0;
      break;
  }
  return ret;
}

} // namespace rb
} // namespace cass
