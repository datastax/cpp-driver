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

#include "driver_utils.hpp"

#include "address.hpp"
#include "future.hpp"
#include "request_handler.hpp"
#include "statement.hpp"

const std::vector<std::string> test::driver::internals::Utils::attempted_hosts(
  CassFuture* future) {
  std::vector<std::string> attempted_hosts;
  if (future) {
  cass::Future* cass_future = static_cast<cass::Future*>(future);
  if (cass_future->type() == cass::CASS_FUTURE_TYPE_RESPONSE) {
    cass::ResponseFuture* response = static_cast<cass::ResponseFuture*>(cass_future);
    cass::AddressVec attempted_addresses = response->attempted_addresses();
    for (cass::AddressVec::iterator iterator = attempted_addresses.begin();
      iterator != attempted_addresses.end(); ++iterator) {
      attempted_hosts.push_back(iterator->to_string().c_str());
    }
    std::sort(attempted_hosts.begin(), attempted_hosts.end());
  }
  }
  return attempted_hosts;
}

const std::string test::driver::internals::Utils::host(CassFuture* future) {
  if (future) {
    cass::Future* cass_future = static_cast<cass::Future*>(future);
    if (cass_future->type() == cass::CASS_FUTURE_TYPE_RESPONSE) {
      return static_cast<cass::ResponseFuture*>(cass_future)->address().to_string().c_str();
    }
  }
  return "";
}

void test::driver::internals::Utils::set_record_attempted_hosts(
  CassStatement* statement, bool enable) {
  if (statement) {
    static_cast<cass::Statement*>(statement)
      ->set_record_attempted_addresses(enable);
  }
}
