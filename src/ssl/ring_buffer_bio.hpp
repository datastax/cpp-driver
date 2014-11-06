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

#ifndef __CASS_RING_BUFFER_BIO_HPP_INCLUDED__
#define __CASS_RING_BUFFER_BIO_HPP_INCLUDED__

#include "ring_buffer.hpp"

#include <assert.h>
#include <openssl/bio.h>

namespace cass {
namespace rb {

class RingBufferBio {
public:
  static BIO* create(RingBuffer* ring_buffer);

  static RingBuffer* from_bio(BIO* bio) {
    assert(bio->ptr != NULL);
    return static_cast<RingBuffer*>(bio->ptr);
  }

private:
  static int create(BIO* bio);
  static int destroy(BIO* bio);
  static int read(BIO* bio, char* out, int len);
  static int write(BIO* bio, const char* data, int len);
  static int puts(BIO* bio, const char* str);
  static int gets(BIO* bio, char* out, int size);
  static long ctrl(BIO* bio, int cmd, long num, void* ptr);

  static const BIO_METHOD method;
};

} // namespace rb
} // namespace cass

#endif
