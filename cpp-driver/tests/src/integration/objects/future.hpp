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

#include "string_ref.hpp"
#include "testing.hpp"

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
  std::vector<std::string> attempted_hosts() {
    datastax::StringVec internal_attempted_hosts =
        datastax::internal::testing::get_attempted_hosts_from_future(get());
    std::vector<std::string> attempted_hosts;
    for (datastax::StringVec::iterator it = internal_attempted_hosts.begin(),
                                       end = internal_attempted_hosts.end();
         it != end; ++it) {
      attempted_hosts.push_back(std::string(it->data(), it->size()));
    }
    return attempted_hosts;
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
  std::string host() {
    datastax::String host = datastax::internal::testing::get_host_from_future(get());
    return std::string(host.data(), host.size());
  }

  /**
   * Get the server name of the future
   *
   * @return Server name
   */
  std::string server_name() {
    datastax::String server_name = datastax::internal::testing::get_server_name(get());
    return std::string(server_name.data(), server_name.size());
  }

  /**
   * Get the result from the future
   *
   * @return Result from future
   */
  const CassResult* result() { return cass_future_get_result(get()); }

  /**
   * Get the error result from the future
   *
   * @return Error result from future
   */
  const CassErrorResult* error_result() { return cass_future_get_error_result(get()); }

  /**
   * Get the prepared from the future
   *
   * @return A prepared statement
   */
  const CassPrepared* prepared() { return cass_future_get_prepared(get()); }

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
