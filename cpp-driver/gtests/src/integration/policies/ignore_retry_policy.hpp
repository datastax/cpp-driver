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

#ifndef __ALWAYS_IGNORE_RETRY_POLICY_HPP__
#define __ALWAYS_IGNORE_RETRY_POLICY_HPP__

#include "objects/retry_policy.hpp"

#include "retry_policy.hpp"

namespace test { namespace driver {

/**
 * Retry policy that will create an ignore decision for retry
 */
class IgnoreRetryPolicy : public datastax::internal::core::DefaultRetryPolicy {
public:
  IgnoreRetryPolicy()
      : datastax::internal::core::DefaultRetryPolicy() {}

  /**
   * Create an instance of the retry policy for use with the driver
   *
   * @return Driver ready retry policy
   */
  static ::test::driver::RetryPolicy policy() {
    datastax::internal::core::RetryPolicy* policy = new IgnoreRetryPolicy();
    policy->inc_ref();
    return CassRetryPolicy::to(policy);
  }

  RetryDecision on_read_timeout(const datastax::internal::core::Request* request,
                                CassConsistency cl, int received, int required, bool data_recevied,
                                int num_retries) const {
    return RetryDecision::ignore();
  }
  RetryDecision on_write_timeout(const datastax::internal::core::Request* request,
                                 CassConsistency cl, int received, int required,
                                 CassWriteType write_type, int num_retries) const {
    return RetryDecision::ignore();
  }
  virtual RetryDecision on_unavailable(const datastax::internal::core::Request* request,
                                       CassConsistency cl, int required, int alive,
                                       int num_retries) const {
    return RetryDecision::ignore();
  }
  virtual RetryDecision on_request_error(const datastax::internal::core::Request* request,
                                         CassConsistency cl,
                                         const datastax::internal::core::ErrorResponse* error,
                                         int num_retries) const {
    return RetryDecision::ignore();
  }
};

}} // namespace test::driver

#endif // __ALWAYS_IGNORE_RETRY_POLICY_HPP__
