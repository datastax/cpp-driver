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

#ifndef __CASS_COMMON_HPP_INCLUDED__
#define __CASS_COMMON_HPP_INCLUDED__

#include "cassandra.h"

#include <uv.h>

#include <string>

namespace cass {

uv_buf_t alloc_buffer(size_t suggested_size);
uv_buf_t alloc_buffer(uv_handle_t* handle, size_t suggested_size);
void free_buffer(uv_buf_t buf);

std::string opcode_to_string(int opcode);

std::string& trim(std::string& str);

} // namespace cass

#endif
