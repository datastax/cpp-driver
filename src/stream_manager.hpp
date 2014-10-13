/*
  Copyright 2014 DataStax

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

#include <assert.h>

#include "third_party/boost/boost/cstdint.hpp"

namespace cass {

template <class T>
class StreamManager {
public:
  static const int MAX_STREAMS = 128;

  StreamManager()
      : available_stream_index_(0) {
    for (int i = 0; i < MAX_STREAMS; ++i) {
      available_streams_[i] = i;
      allocated_streams_[i] = false;
    }
  }

  int8_t acquire_stream(const T& item) {
    if (available_stream_index_ >= MAX_STREAMS) {
      return -1;
    }
    int8_t stream = available_streams_[available_stream_index_++];
    items_[stream] = item;
    allocated_streams_[stream] = true;
    return stream;
  }

  void release_stream(int8_t stream) {
    assert(allocated_streams_[stream]);
    available_streams_[--available_stream_index_] = stream;
    allocated_streams_[stream] = false;
  }

  bool get_item(int8_t stream, T& output, bool release = true) {
    if (allocated_streams_[stream]) {
      output = items_[stream];
      if (release) {
        release_stream(stream);
      }
      return true;
    }
    return false;
  }

  size_t available_streams() const { return MAX_STREAMS - available_stream_index_; }
  size_t pending_streams() const { return available_stream_index_; }

private:
  int available_stream_index_;
  int8_t available_streams_[MAX_STREAMS];
  bool allocated_streams_[MAX_STREAMS];
  T items_[MAX_STREAMS];
};

} // namespace cass

#endif
