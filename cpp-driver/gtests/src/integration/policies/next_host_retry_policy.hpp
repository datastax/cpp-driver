/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __NEXT_HOST_RETRY_POLICY_HPP__
#define __NEXT_HOST_RETRY_POLICY_HPP__

#include "objects/retry_policy.hpp"

#include "retry_policy.hpp"

namespace test {
namespace driver {

/**
 * Retry policy that will retry the statement on the next host
 */
class NextHostRetryPolicy : public cass::DefaultRetryPolicy {
public:
  NextHostRetryPolicy()
    : cass::DefaultRetryPolicy() { }

  /**
   * Create an instance of the retry policy for use with the driver
   *
   * @return Driver ready retry policy
   */
  static test::driver::RetryPolicy policy() {
    cass::RetryPolicy* policy = cass::Memory::allocate<NextHostRetryPolicy>();
    policy->inc_ref();
    return CassRetryPolicy::to(policy);
  }

  RetryDecision on_read_timeout(const cass::Request* request,
                                CassConsistency cl,
                                int received,
                                int required,
                                bool data_recevied,
                                int num_retries) const {
    return RetryDecision::retry_next_host(cl);
  }
  RetryDecision on_write_timeout(const cass::Request* request,
                                 CassConsistency cl,
                                 int received,
                                 int required,
                                 CassWriteType write_type,
                                 int num_retries) const {
    return RetryDecision::retry_next_host(cl);
  }
  virtual RetryDecision on_unavailable(const cass::Request* request,
                                       CassConsistency cl,
                                       int required,
                                       int alive,
                                       int num_retries) const {
    return RetryDecision::retry_next_host(cl);
  }
  virtual RetryDecision on_request_error(const cass::Request* request,
                                         CassConsistency cl,
                                         const cass::ErrorResponse* error,
                                         int num_retries) const {
    return RetryDecision::retry_next_host(cl);
  }
};

} // namespace driver
} // namespace test

#endif // __NEXT_HOST_RETRY_POLICY_HPP__
