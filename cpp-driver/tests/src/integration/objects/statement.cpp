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

#include "statement.hpp"

#include "objects/custom_payload.hpp"
#include "objects/result.hpp"

void test::driver::Statement::set_custom_payload(CustomPayload custom_payload) {
  ASSERT_EQ(CASS_OK, cass_statement_set_custom_payload(get(), custom_payload.get()));
}

void test::driver::Statement::set_paging_state(const test::driver::Result& result) {
  ASSERT_EQ(CASS_OK, cass_statement_set_paging_state(get(), result.get()));
}
