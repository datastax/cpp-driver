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

#ifndef DATASTAX_INTERNAL_STREAM_MANAGER_HPP
#define DATASTAX_INTERNAL_STREAM_MANAGER_HPP

#include "constants.hpp"
#include "dense_hash_map.hpp"
#include "macros.hpp"
#include "scoped_ptr.hpp"

#include <assert.h>
#include <stdint.h>
#include <string.h>

#if defined(_MSC_VER)
#include <intrin.h>
#endif

namespace datastax { namespace internal { namespace core {

struct StreamHash {
  std::size_t operator()(int stream) const { return ((stream & 0x3F) << 10) | (stream >> 6); }
};

template <class T>
class StreamManager {
public:
  StreamManager()
      : max_streams_(CASS_MAX_STREAMS)
      , num_words_(max_streams_ / NUM_BITS_PER_WORD)
      , offset_(0)
      , words_(num_words_, ~static_cast<word_t>(0)) {
    // Client request stream IDs are always positive values so it's
    // safe to use negative values for the empty and deleted keys.
    pending_.set_empty_key(-1);
    pending_.set_deleted_key(-2);
    pending_.max_load_factor(0.4f);
  }

  int acquire(const T& item) {
    int stream = acquire_stream();
    if (stream < 0) return -1;
    pending_.insert(std::pair<int, T>(stream, item));
    return stream;
  }

  void release(int stream) {
    assert(stream >= 0 && static_cast<size_t>(stream) < max_streams_);
    assert(pending_.count(stream) > 0);
    pending_.erase(stream);
    release_stream(stream);
  }

  bool get(int stream, T& output) {
    typename PendingMap::iterator i = pending_.find(stream);
    if (i != pending_.end()) {
      output = i->second;
      return true;
    }
    return false;
  }

  size_t available_streams() const { return max_streams_ - pending_.size(); }
  size_t pending_streams() const { return pending_.size(); }
  size_t max_streams() const { return max_streams_; }

private:
  typedef DenseHashMap<int, T, StreamHash> PendingMap;

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
#if defined(_M_AMD64)
    _BitScanForward64(&index, word);

#else
    _BitScanForward(&index, word);
#endif
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
      int bit = get_and_set_first_available_stream(index);
      if (bit >= 0) {
        return bit + (NUM_BITS_PER_WORD * index);
      }
    }

    return -1;
  }

  inline void release_stream(int stream) {
    size_t index = stream / NUM_BITS_PER_WORD;
    int bit = stream % NUM_BITS_PER_WORD;
    assert((words_[index] & (static_cast<word_t>(1) << (bit))) == 0);
    words_[index] |= (static_cast<word_t>(1) << (bit));
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
  Vector<word_t> words_;
  PendingMap pending_;

private:
  DISALLOW_COPY_AND_ASSIGN(StreamManager);
};

}}} // namespace datastax::internal::core

#endif
