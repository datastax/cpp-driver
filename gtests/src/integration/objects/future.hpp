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

#ifndef __TEST_FUTURE_HPP__
#define __TEST_FUTURE_HPP__
#include "cassandra.h"

#include "objects/object_base.hpp"

#include "driver_utils.hpp"

#include <string>

#include <gtest/gtest.h>

namespace test { namespace driver {

/**
 * Wrapped future object
 */
class Future : public Object<CassFuture, cass_future_free> {
public:
  /**
   * Create the empty future object
   */
  Future()
      : Object<CassFuture, cass_future_free>() {}

  /**
   * Create the future object from the native driver object
   *
   * @param future Native driver object
   */
  Future(CassFuture* future)
      : Object<CassFuture, cass_future_free>(future) {}

  /**
   * Create the future object from a shared reference
   *
   * @param future Shared reference
   */
  Future(Ptr future)
      : Object<CassFuture, cass_future_free>(future) {}

  /**
   * Get the attempted hosts/addresses of the future (sorted)
   *
   * @return Attempted hosts/Addresses (sorted)
   */
  const std::vector<std::string> attempted_hosts() {
    return internals::Utils::attempted_hosts(get());
  }

  /**
   * Get the error code from the future
   *
   * @return Error code of the future
   */
  CassError error_code() { return cass_future_error_code(get()); }

  /**
   * Get the human readable description of the error code
   *
   * @return Error description
   */
  const std::string error_description() { return std::string(cass_error_desc(error_code())); }

  /**
   * Get the error message of the future if an error occurred
   *
   * @return Error message
   */
  const std::string error_message() {
    const char* message;
    size_t message_length;
    cass_future_error_message(get(), &message, &message_length);
    return std::string(message, message_length);
  }

  /**
   * Get the host/address of the future
   *
   * @return Host/Address
   */
  const std::string host() { return internals::Utils::host(get()); }

  /**
   * Get the server name of the future
   *
   * @return Server name
   */
  const std::string server_name() { return internals::Utils::server_name(get()); }

  /**
   * Get the result from the future
   *
   * @return Result from future
   */
  const CassResult* result() { return cass_future_get_result(get()); }

  /**
   * Wait for the future to resolve itself
   *
   * @param assert_ok True if error code for future should be asserted
   *                  CASS_OK; false otherwise (default: true)
   */
  void wait(bool assert_ok = true) {
    CassError wait_code = error_code();
    if (assert_ok) {
      ASSERT_EQ(CASS_OK, wait_code) << error_description() << ": " << error_message();
    }
  }

  /**
   * Wait for the future to resolve itself or timeout after the specified
   * duration
   *
   * @param timeout Timeout (in microseconds) for the future to resolve itself
   *                (default: 60s)
   * @param assert_true True if timeout should be asserted; false otherwise
   *                 (default: true)
   */
  void wait_timed(cass_duration_t timeout = 60000000, bool assert_true = true) {
    cass_bool_t timed_out = cass_future_wait_timed(get(), timeout);
    if (assert_true) {
      ASSERT_TRUE(timed_out) << "Timed out waiting for result";
    }
  }
};

}} // namespace test::driver

#endif // __TEST_FUTURE_HPP__
