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
#include "batch.hpp"

extern "C" {

cass_batch_t*
cass_batch_new(cass_consistency_t consistency) {
  return cass_batch_t::to(new cass::Batch(consistency));
}

CASS_EXPORT void
cass_batch_free(cass_batch_t* batch) {
  delete batch->from();
}

cass_code_t
cass_batch_add_statement(
    cass_batch_t* batch,
    cass_statement_t*      statement) {
  batch->add_statement(statement);
  return CASS_OK;
}

CASS_EXPORT void
cass_batch_set_timestamp(cass_batch_t* batch,
                         cass_int64_t timestamp) {
}


} // extern "C"
