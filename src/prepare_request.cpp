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

#include "prepare_request.hpp"
#include "serialization.hpp"

namespace cass {

bool PrepareRequest::prepare(size_t reserved, char** output, size_t& size) {
  size = reserved + sizeof(int32_t) + statement_.size();
  *output = new char[size];
  encode_long_string(*output + reserved, statement_.c_str(), statement_.size());
  return true;
}

} // namespace cas
