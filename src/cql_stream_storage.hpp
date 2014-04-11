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

#ifndef __STREAM_STORAGE_HPP_INCLUDED__
#define __STREAM_STORAGE_HPP_INCLUDED__

namespace cql {

template <typename IdType,
          typename StorageType,
          size_t   Max>
class StreamStorage {
 public:
  StreamStorage() :
      available_streams_index_(0) {
    for (size_t i = 1; i <= Max; ++i) {
      available_streams_[i - 1] = i;
      allocated_streams_[i - 1] = false;
    }
  }

  inline Error*
  set_stream(
      const StorageType& input,
      IdType&      output) {
    if (available_streams_index_ >= Max) {
      return new Error(
          CQL_ERROR_SOURCE_LIBRARY,
          CQL_ERROR_LIB_NO_STREAMS,
          "no available streams",
          __FILE__,
          __LINE__);
    }

    intptr_t index             = available_streams_index_++;
    output                     = available_streams_[index];
    storage_[output]           = input;
    allocated_streams_[output] = true;
    return CQL_ERROR_NO_ERROR;
  }

  inline Error*
  get_stream(
      const IdType& input,
      StorageType&  output,
      bool          releaseStream = true) {
    output = storage_[static_cast<int>(input)];
    if (releaseStream) {
      if (allocated_streams_[input]) {
        available_streams_[--available_streams_index_] = input;
        allocated_streams_[input] = false;
      } else {
        return new Error(
            CQL_ERROR_SOURCE_LIBRARY,
            CQL_ERROR_LIB_NO_STREAMS,
            "this stream has already been released",
            __FILE__,
            __LINE__);
      }
    }
    return CQL_ERROR_NO_ERROR;
  }

  inline size_t
  available_streams() {
    return Max - available_streams_index_;
  }

 private:
  uintptr_t   available_streams_index_;
  IdType      available_streams_[Max];
  bool        allocated_streams_[Max];
  StorageType storage_[Max];
};
}
#endif
