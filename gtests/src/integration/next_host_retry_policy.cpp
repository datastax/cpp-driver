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