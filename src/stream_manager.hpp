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

#ifndef __CASS_STREAM_MANAGER_HPP_INCLUDED__
#define __CASS_STREAM_MANAGER_HPP_INCLUDED__

#include "macros.hpp"
#include "scoped_ptr.hpp"

#include <assert.h>
#include <stdint.h>
#include <string.h>

#include <sparsehash/dense_hash_map>

#if defined(_MSC_VER)
#include <intrin.h>
#endif

namespace cass {

inline int max_streams_for_protocol_version(int protocol_version) {
  // Note: Only half the range because negative values are reserved for
  // responses.

  // Protocol v3+: 2 ^ (16 - 1) (2 bytes)
  // Protocol v1 and v2: 2 ^ (8 - 1) (1 byte)
  return protocol_version >= 3 ? 32768 : 128;
}

template <class T>
class StreamManager {
public:
  StreamManager(int protocol_version)
      : max_streams_(max_streams_for_protocol_version(protocol_version))
      , num_words_(max_streams_ / NUM_BITS_PER_WORD)
      , offset_(0)
      , words_(new word_t[num_words_]) {
    // Client request stream IDs are always positive values so it's
    // safe to use negative values for the empty and deleted keys.
    pending_.set_empty_key(-1);
    pending_.set_deleted_key(-2);
    memset(words_.get(), 0xFF, sizeof(word_t) * num_words_);
  }

  int acquire(const T& item) {
    int stream = acquire_stream();
    if (stream < 0) return -1;
    pending_[stream] = item;
    return stream;
  }

  void release(int stream) {
    assert(stream >= 0 && static_cast<size_t>(stream) < max_streams_);
    pending_.erase(stream);
    release_stream(stream);
  }

  bool get_pending_and_release(int stream, T& output) {
    typename PendingMap::iterator i = pending_.find(stream);
    if (i != pending_.end()) {
      output = i->second;
      pending_.erase(i);
      release_stream(stream);
      return true;
    }
    return false;
  }

  size_t available_streams() const { return max_streams_ - pending_.size(); }
  size_t pending_streams() const { return pending_.size(); }
  size_t max_streams() const { return max_streams_; }

private:
  typedef sparsehash::dense_hash_map<int, T> PendingMap;

#if defined(_MSC_VER) && defined(_M_AMD64)
  typedef __int64 word_t;
#else
  typedef unsigned long word_t;
#endif

  static const size_t NUM_BITS_PER_WORD = sizeof(word_t) * 8;

  static inline int count_trailing_zeros(word_t word) {
#if defined(__GNUC__)
    return __builtin_ctzl(word);
#elif defined(_MSC_VER)
    unsigned long index;
#  if defined(_M_AMD64)
    _BitScanForward64(&index, word);

#  else
    _BitScanForward(&index, word);
#  endif
    return static_cast<int>(index);
#else
#endif
  }

private:
  int acquire_stream() {
    const size_t offset = offset_;
    const size_t num_words = num_words_;

    ++offset_;

    for (size_t i = 0; i < num_words; ++i) {
      size_t index = (i + offset) % num_words;
      int stream = get_and_set_first_available_stream(index);
      if (stream >= 0) return stream + (NUM_BITS_PER_WORD * index);
    }

    return -1;
  }

  inline void release_stream(int stream) {
    assert((words_[stream / NUM_BITS_PER_WORD] & (static_cast<word_t>(1) << (stream % NUM_BITS_PER_WORD))) == 0);
    words_[stream / NUM_BITS_PER_WORD] |=
        (static_cast<word_t>(1) << (stream % NUM_BITS_PER_WORD));
  }

  inline int get_and_set_first_available_stream(size_t index) {
    word_t word = words_[index];
    if (word == 0) return -1;
    int stream = count_trailing_zeros(word);
    words_[index] ^= (static_cast<word_t>(1) << stream);
    return stream;
  }

private:
  const size_t max_streams_;
  const size_t num_words_;
  size_t offset_;
  ScopedPtr<word_t[]> words_;
  PendingMap pending_;

private:
  DISALLOW_COPY_AND_ASSIGN(StreamManager);
};

} // namespace cass

#endif
