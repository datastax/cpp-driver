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

namespace cass {

template <class T>
class StreamManager {
 public:
    static const int MAX_STREAM = 127;

  StreamManager()
    : available_stream_index_(0) {
    for (size_t i = 1; i <= MAX_STREAM; ++i) {
      available_streams_[i - 1] = i;
      allocated_streams_[i - 1] = false;
    }
  }

  int8_t acquire_stream(const T& item) {
    if (available_stream_index_ >= MAX_STREAM) {
      return -1;
    }
    int8_t stream = available_streams_[available_stream_index_++];
    items_[stream] = item;
    allocated_streams_[stream] = true;
    return stream;
  }

  void release_stream(int8_t stream) {
    assert(stream > 0 && stream <= MAX_STREAM && allocated_streams_[stream]);
    available_streams_[--available_stream_index_] = stream;
    allocated_streams_[stream] = false;
  }

  bool get_item(int8_t stream, T& output, bool release = true) {
    assert(stream > 0 && stream <= MAX_STREAM);
    if(allocated_streams_[stream]) {
      output = items_[stream];
      if(release) {
        release_stream(stream);
      }
      return true;
    }
    return false;
  }

  inline size_t available_streams() {
    return MAX_STREAM - available_stream_index_;
  }

 private:
  int available_stream_index_;
  int8_t available_streams_[MAX_STREAM];
  bool allocated_streams_[MAX_STREAM];
  T items_[MAX_STREAM];
};

} // namespace cass

#endif
