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

#include "batch_request.hpp"

#include <string.h>

extern "C" {

CassError cass_batch_set_execute_as_n(CassBatch* batch, const char* name, size_t name_length) {
  batch->set_custom_payload("ProxyExecute", reinterpret_cast<const uint8_t*>(name), name_length);
  return CASS_OK;
}

CassError cass_batch_set_execute_as(CassBatch* batch, const char* name) {
  return cass_batch_set_execute_as_n(batch, name, SAFE_STRLEN(name));
}

} // extern "C"
