/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __ALWAYS_IGNORE_RETRY_POLICY_HPP__
#define __ALWAYS_IGNORE_RETRY_POLICY_HPP__

#include "objects/retry_policy.hpp"

#include "retry_policy.hpp"

namespace test {
namespace driver {

/**
 * Retry policy that will create an ignore decision for retry
 */
class IgnoreRetryPolicy : public cass::DefaultRetryPolicy {
public:
  IgnoreRetryPolicy()
    : cass::DefaultRetryPolicy() { }

  /**
   * Create an instance of the retry policy for use with the driver
   *
   * @return Driver ready retry policy
   */
  static test::driver::RetryPolicy policy() {
    cass::RetryPolicy* policy = cass::Memory::allocate<IgnoreRetryPolicy>();
    policy->inc_ref();
    return CassRetryPolicy::to(policy);
  }

  RetryDecision on_read_timeout(const cass::Request* request,
                                CassConsistency cl,
                                int received,
                                int required,
                                bool data_recevied,
                                int num_retries) const {
    return RetryDecision::ignore();
  }
  RetryDecision on_write_timeout(const cass::Request* request,
                                 CassConsistency cl,
                                 int received,
                                 int required,
                                 CassWriteType write_type,
                                 int num_retries) const {
    return RetryDecision::ignore();
  }
  virtual RetryDecision on_unavailable(const cass::Request* request,
                                       CassConsistency cl,
                                       int required,
                                       int alive,
                                       int num_retries) const {
    return RetryDecision::ignore();
  }
  virtual RetryDecision on_request_error(const cass::Request* request,
                                         CassConsistency cl,
                                         const cass::ErrorResponse* error,
                                         int num_retries) const {
    return RetryDecision::ignore();
  }
};

} // namespace driver
} // namespace test

#endif // __ALWAYS_IGNORE_RETRY_POLICY_HPP__
