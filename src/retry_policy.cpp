/*
  Copyright (c) 2014-2015 DataStax

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

#include "retry_policy.hpp"

namespace cass {

RetryPolicy::RetryDecision DefaultRetryPolicy::on_read_timeout(CassConsistency cl, int received, int required, bool data_recevied, int num_retries) const {
  if (num_retries != 0) {
    return RetryDecision::return_error();
  }

  if (received >= required && !data_recevied) {
    return RetryDecision::retry(cl);
  } else {
    return RetryDecision::return_error();
  }
}

RetryPolicy::RetryDecision DefaultRetryPolicy::on_write_timeout(CassConsistency cl, int received, int required, WriteType write_type, int num_retries) const {
  if (num_retries != 0) {
    return RetryDecision::return_error();
  }

  if (write_type == BATCH_LOG) {
    return RetryDecision::retry(cl);
  } else {
    return RetryDecision::return_error();
  }
}

RetryPolicy::RetryDecision DefaultRetryPolicy::on_unavailable(CassConsistency cl, int required, int alive, int num_retries) const {
  if (num_retries == 0) {
    return RetryDecision::retry_next_host(cl);
  } else {
    return RetryDecision::return_error();
  }
}

RetryPolicy::RetryDecision DowngradingConsistencyRetryPolicy::on_read_timeout(CassConsistency cl, int received, int required, bool data_recevied, int num_retries) const {
  if (num_retries != 0) {
    return RetryDecision::return_error();
  }


}

RetryPolicy::RetryDecision DowngradingConsistencyRetryPolicy::on_write_timeout(CassConsistency cl, int received, int required, WriteType write_type, int num_retries) const {
  if (num_retries != 0) {
    return RetryDecision::return_error();
  }


}

RetryPolicy::RetryDecision DowngradingConsistencyRetryPolicy::on_unavailable(CassConsistency cl, int required, int alive, int num_retries) const {
  if (num_retries != 0) {
    return RetryDecision::return_error();
  }


}

RetryPolicy::RetryDecision FallthroughRetryPolicy::on_read_timeout(CassConsistency cl, int received, int required, bool data_recevied, int num_retries) const {
  return RetryDecision::return_error();
}

RetryPolicy::RetryDecision FallthroughRetryPolicy::on_write_timeout(CassConsistency cl, int received, int required, WriteType write_type, int num_retries) const {
  return RetryDecision::return_error();
}

RetryPolicy::RetryDecision FallthroughRetryPolicy::on_unavailable(CassConsistency cl, int required, int alive, int num_retries) const {
  return RetryDecision::return_error();
}

RetryPolicy::RetryDecision LoggingRetryPolicy::on_read_timeout(CassConsistency cl, int received, int required, bool data_recevied, int num_retries) const {
  // TODO: implement
  return RetryDecision::return_error();
}

RetryPolicy::RetryDecision LoggingRetryPolicy::on_write_timeout(CassConsistency cl, int received, int required, WriteType write_type, int num_retries) const {
  // TODO: implement
  return RetryDecision::return_error();
}

RetryPolicy::RetryDecision LoggingRetryPolicy::on_unavailable(CassConsistency cl, int required, int alive, int num_retries) const {
  // TODO: implement
  return RetryDecision::return_error();
}

} // namespace cass
