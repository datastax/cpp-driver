/*
  Copyright (c) 2014 DataStax

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

#include "types.hpp"
#include "prepared.hpp"
#include "execute_request.hpp"

extern "C" {

void cass_prepared_free(const CassPrepared* prepared) {
  prepared->release();
}

CassStatement* cass_prepared_bind(const CassPrepared* prepared) {
  cass::ExecuteRequest* execute
      = new cass::ExecuteRequest(prepared);
  execute->retain();
  return CassStatement::to(execute);
}

} // extern "C"
