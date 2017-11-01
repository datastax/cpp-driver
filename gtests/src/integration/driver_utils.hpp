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

#ifndef __TEST_DRIVER_UTILS_HPP__
#define __TEST_DRIVER_UTILS_HPP__

#include "cassandra.h"

#include <string>
#include <vector>

namespace test {
namespace driver {
namespace internals {

class Utils {
public:
  /**
   * Get the attempted hosts/addresses of the future (sorted)
   *
   * @param future Future to retrieve hosts/addresses from
   * @return Attempted hosts/Addresses (sorted)
   */
  static const std::vector<std::string> attempted_hosts(CassFuture* future);

  /**
   * Get the host/address of the future
   *
   * @param future Future to retrieve hosts/addresses from
   * @return Host/Address
   */
  static const std::string host(CassFuture* future);

  /**
   * Enable/Disable the recording of hosts attempted during the execution of
   * a statement
   *
   * @param statement Statement to enable/disable attempted host recording
   * @param enable True if attempted host should be recorded; false otherwise
   */
  static void set_record_attempted_hosts(CassStatement* statement, bool enable);

};

} // namespace internals
} // namespace driver
} // namespace test

#endif // __TEST_DRIVER_UTILS_HPP__