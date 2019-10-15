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

#include "query_request.hpp"

#include "constants.hpp"
#include "logger.hpp"
#include "request_callback.hpp"
#include "serialization.hpp"

using namespace datastax;
using namespace datastax::internal::core;

int QueryRequest::encode(ProtocolVersion version, RequestCallback* callback,
                         BufferVec* bufs) const {
  int32_t result;
  int32_t length = encode_query_or_id(bufs);
  if (has_names_for_values()) {
    length += encode_begin(version, static_cast<uint16_t>(value_names_->size()), callback, bufs);
    result = encode_values_with_names(version, callback, bufs);
  } else {
    length += encode_begin(version, static_cast<uint16_t>(elements().size()), callback, bufs);
    result = encode_values(version, callback, bufs);
  }
  if (result < 0) return result;
  length += result;
  length += encode_end(version, callback, bufs);
  return length;
}

// Format: [<name_1><value_1>...<name_n><value_n>]
// where:
// <name> is a [string]
// <value> is a [bytes]
int32_t QueryRequest::encode_values_with_names(ProtocolVersion version, RequestCallback* callback,
                                               BufferVec* bufs) const {
  int32_t size = 0;
  for (size_t i = 0; i < value_names_->size(); ++i) {
    const Buffer& name_buf = (*value_names_)[i].buf;
    bufs->push_back(name_buf);

    Buffer value_buf(elements()[i].get_buffer());
    bufs->push_back(value_buf);

    size += name_buf.size() + value_buf.size();
  }
  return size;
}

size_t QueryRequest::get_indices(StringRef name, IndexVec* indices) {
  if (!value_names_) {
    set_has_names_for_values(true);
    value_names_.reset(new ValueNameHashTable(elements().size()));
  }

  if (value_names_->get_indices(name, indices) == 0) {
    if (value_names_->size() > elements().size()) {
      // No more space left for new named values
      return 0;
    }
    if (name.size() > 0 && name.front() == '"' && name.back() == '"') {
      name = name.substr(1, name.size() - 2);
    }
    indices->push_back(value_names_->add(ValueName(name.to_string())));
  }

  return indices->size();
}
