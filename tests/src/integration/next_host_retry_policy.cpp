/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/
#include "next_host_retry_policy.hpp"

NextHostRetryPolicy::NextHostRetryPolicy()
  : cass::DefaultRetryPolicy() {}

test::driver::RetryPolicy NextHostRetryPolicy::policy() {
  cass::RetryPolicy* policy = new NextHostRetryPolicy();
  policy->inc_ref();
  return CassRetryPolicy::to(policy);
}

cass::RetryPolicy::RetryDecision NextHostRetryPolicy::on_read_timeout(
  const cass::Request* request, CassConsistency cl, int received, int required,
  bool data_recevied, int num_retries) const {
  return RetryDecision::retry_next_host(cl);
}

cass::RetryPolicy::RetryDecision NextHostRetryPolicy::on_write_timeout(
  const cass::Request* request, CassConsistency cl, int received, int required,
  CassWriteType write_type, int num_retries) const {
  return RetryDecision::retry_next_host(cl);
}

cass::RetryPolicy::RetryDecision NextHostRetryPolicy::on_unavailable(
  const cass::Request* request, CassConsistency cl, int required, int alive,
  int num_retries) const {
  return RetryDecision::retry_next_host(cl);
}

cass::RetryPolicy::RetryDecision NextHostRetryPolicy::on_request_error(
  const cass::Request* request, CassConsistency cl,
  const cass::ErrorResponse* error, int num_retries) const {
  return RetryDecision::retry_next_host(cl);
}