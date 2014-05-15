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

#include "cassandra.hpp"
#include "prepared.hpp"

extern "C" {

void
cass_prepared_free(
    cass_prepared_t* prepared) {
  delete prepared->from();
}

cass_code_t
cass_prepared_bind(
    cass_prepared_t*   prepared,
    size_t         parameter_count,
    cass_consistency_t consistency,
    cass_statement_t** output) {
  cass::Statement* bound_statement = new cass::Bound(*prepared, parameter_count, consistency);
  *output = cass_statement_t::to(bound_statement);
  return CASS_OK;
}


} // extern "C"
