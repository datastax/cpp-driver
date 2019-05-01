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

#include "retry_policy.hpp"

#include "external.hpp"
#include "logger.hpp"
#include "request.hpp"

using namespace datastax::internal;
using namespace datastax::internal::core;

extern "C" {

CassRetryPolicy* cass_retry_policy_default_new() {
  RetryPolicy* policy = new DefaultRetryPolicy();
  policy->inc_ref();
  return CassRetryPolicy::to(policy);
}

CassRetryPolicy* cass_retry_policy_downgrading_consistency_new() {
  RetryPolicy* policy = new DowngradingConsistencyRetryPolicy();
  policy->inc_ref();
  return CassRetryPolicy::to(policy);
}

CassRetryPolicy* cass_retry_policy_fallthrough_new() {
  RetryPolicy* policy = new FallthroughRetryPolicy();
  policy->inc_ref();
  return CassRetryPolicy::to(policy);
}

CassRetryPolicy* cass_retry_policy_logging_new(CassRetryPolicy* child_retry_policy) {
  if (child_retry_policy->type() == RetryPolicy::LOGGING) {
    return NULL;
  }
  RetryPolicy* policy = new LoggingRetryPolicy(SharedRefPtr<RetryPolicy>(child_retry_policy));
  policy->inc_ref();
  return CassRetryPolicy::to(policy);
}

void cass_retry_policy_free(CassRetryPolicy* policy) { policy->dec_ref(); }

} // extern "C"

inline RetryPolicy::RetryDecision max_likely_to_work(int received) {
  if (received >= 3) {
    return RetryPolicy::RetryDecision::retry(CASS_CONSISTENCY_THREE);
  } else if (received == 2) {
    return RetryPolicy::RetryDecision::retry(CASS_CONSISTENCY_TWO);
  } else if (received == 1) {
    return RetryPolicy::RetryDecision::retry(CASS_CONSISTENCY_ONE);
  } else {
    return RetryPolicy::RetryDecision::return_error();
  }
}

// Default retry policy

RetryPolicy::RetryDecision DefaultRetryPolicy::on_read_timeout(const Request* request,
                                                               CassConsistency cl, int received,
                                                               int required, bool data_recevied,
                                                               int num_retries) const {
  if (num_retries != 0) {
    return RetryDecision::return_error();
  }

  if (received >= required && !data_recevied) {
    return RetryDecision::retry(cl);
  } else {
    return RetryDecision::return_error();
  }
}

RetryPolicy::RetryDecision DefaultRetryPolicy::on_write_timeout(const Request* request,
                                                                CassConsistency cl, int received,
                                                                int required,
                                                                CassWriteType write_type,
                                                                int num_retries) const {
  if (num_retries != 0) {
    return RetryDecision::return_error();
  }

  if (write_type == CASS_WRITE_TYPE_BATCH_LOG) {
    return RetryDecision::retry(cl);
  } else {
    return RetryDecision::return_error();
  }
}

RetryPolicy::RetryDecision DefaultRetryPolicy::on_unavailable(const Request* request,
                                                              CassConsistency cl, int required,
                                                              int alive, int num_retries) const {
  if (num_retries == 0) {
    return RetryDecision::retry_next_host(cl);
  } else {
    return RetryDecision::return_error();
  }
}

RetryPolicy::RetryDecision DefaultRetryPolicy::on_request_error(const Request* request,
                                                                CassConsistency cl,
                                                                const ErrorResponse* error,
                                                                int num_retries) const {
  return RetryDecision::retry_next_host(cl);
}

// Downgrading retry policy

RetryPolicy::RetryDecision
DowngradingConsistencyRetryPolicy::on_read_timeout(const Request* request, CassConsistency cl,
                                                   int received, int required, bool data_recevied,
                                                   int num_retries) const {
  if (num_retries != 0) {
    return RetryDecision::return_error();
  }

  if (cl == CASS_CONSISTENCY_SERIAL || cl == CASS_CONSISTENCY_LOCAL_SERIAL) {
    return RetryDecision::return_error();
  }

  if (received < required) {
    return max_likely_to_work(received);
  }

  if (!data_recevied) {
    return RetryDecision::retry(cl);
  } else {
    return RetryDecision::return_error();
  }
}

RetryPolicy::RetryDecision DowngradingConsistencyRetryPolicy::on_write_timeout(
    const Request* request, CassConsistency cl, int received, int required,
    CassWriteType write_type, int num_retries) const {
  if (num_retries != 0) {
    return RetryDecision::return_error();
  }

  switch (write_type) {
    case CASS_WRITE_TYPE_SIMPLE:
    case CASS_WRITE_TYPE_BATCH: // Fallthrough intended
      if (received > 0) {
        return RetryDecision::ignore();
      } else {
        return RetryDecision::return_error();
      }

    case CASS_WRITE_TYPE_UNLOGGED_BATCH:
      return max_likely_to_work(received);

    case CASS_WRITE_TYPE_BATCH_LOG:
      return RetryDecision::retry(cl);

    default:
      return RetryDecision::return_error();
  }
}

RetryPolicy::RetryDecision
DowngradingConsistencyRetryPolicy::on_unavailable(const Request* request, CassConsistency cl,
                                                  int required, int alive, int num_retries) const {
  if (num_retries != 0) {
    return RetryDecision::return_error();
  }
  return max_likely_to_work(alive);
}

RetryPolicy::RetryDecision DowngradingConsistencyRetryPolicy::on_request_error(
    const Request* request, CassConsistency cl, const ErrorResponse* error, int num_retries) const {
  return RetryDecision::retry_next_host(cl);
}

// Fallthrough retry policy

RetryPolicy::RetryDecision FallthroughRetryPolicy::on_read_timeout(const Request* request,
                                                                   CassConsistency cl, int received,
                                                                   int required, bool data_recevied,
                                                                   int num_retries) const {
  return RetryDecision::return_error();
}

RetryPolicy::RetryDecision FallthroughRetryPolicy::on_write_timeout(const Request* request,
                                                                    CassConsistency cl,
                                                                    int received, int required,
                                                                    CassWriteType write_type,
                                                                    int num_retries) const {
  return RetryDecision::return_error();
}

RetryPolicy::RetryDecision FallthroughRetryPolicy::on_unavailable(const Request* request,
                                                                  CassConsistency cl, int required,
                                                                  int alive,
                                                                  int num_retries) const {
  return RetryDecision::return_error();
}

RetryPolicy::RetryDecision FallthroughRetryPolicy::on_request_error(const Request* request,
                                                                    CassConsistency cl,
                                                                    const ErrorResponse* error,
                                                                    int num_retries) const {
  return RetryDecision::return_error();
}

// Logging retry policy

RetryPolicy::RetryDecision LoggingRetryPolicy::on_read_timeout(const Request* request,
                                                               CassConsistency cl, int received,
                                                               int required, bool data_recevied,
                                                               int num_retries) const {
  RetryDecision decision =
      retry_policy_->on_read_timeout(request, cl, received, required, data_recevied, num_retries);

  switch (decision.type()) {
    case RetryDecision::IGNORE:
      LOG_INFO("Ignoring read timeout (initial consistency: %s, required responses: %d, received "
               "responses: %d, data retrieved: %s, retries: %d)",
               cass_consistency_string(cl), required, received, data_recevied ? "true" : "false",
               num_retries);
      break;

    case RetryDecision::RETRY:
      LOG_INFO("Retrying on read timeout at consistency %s (initial consistency: %s, required "
               "responses: %d, received responses: %d, data retrieved: %s, retries: %d)",
               cass_consistency_string(decision.retry_consistency()), cass_consistency_string(cl),
               required, received, data_recevied ? "true" : "false", num_retries);
      break;

    default:
      break;
  }

  return decision;
}

RetryPolicy::RetryDecision LoggingRetryPolicy::on_write_timeout(const Request* request,
                                                                CassConsistency cl, int received,
                                                                int required,
                                                                CassWriteType write_type,
                                                                int num_retries) const {
  RetryDecision decision =
      retry_policy_->on_write_timeout(request, cl, received, required, write_type, num_retries);

  switch (decision.type()) {
    case RetryDecision::IGNORE:
      LOG_INFO("Ignoring write timeout (initial consistency: %s, required acknowledgments: %d, "
               "received acknowledgments: %d, write type: %s, retries: %d)",
               cass_consistency_string(cl), required, received, cass_write_type_string(write_type),
               num_retries);
      break;

    case RetryDecision::RETRY:
      LOG_INFO("Retrying on write timeout at consistency %s (initial consistency: %s, required "
               "acknowledgments: %d, received acknowledgments: %d, write type: %s, retries: %d)",
               cass_consistency_string(decision.retry_consistency()), cass_consistency_string(cl),
               required, received, cass_write_type_string(write_type), num_retries);
      break;

    default:
      break;
  }

  return decision;
}

RetryPolicy::RetryDecision LoggingRetryPolicy::on_unavailable(const Request* request,
                                                              CassConsistency cl, int required,
                                                              int alive, int num_retries) const {
  RetryDecision decision = retry_policy_->on_unavailable(request, cl, required, alive, num_retries);

  switch (decision.type()) {
    case RetryDecision::IGNORE:
      LOG_INFO("Ignoring unavailable error (initial consistency: %s, required replica: %d, alive "
               "replica: %d, retries: %d)",
               cass_consistency_string(cl), required, alive, num_retries);
      break;

    case RetryDecision::RETRY:
      LOG_INFO("Retrying on unavailable error at consistency %s (initial consistency: %s, required "
               "replica: %d, alive replica: %d, retries: %d)",
               cass_consistency_string(decision.retry_consistency()), cass_consistency_string(cl),
               required, alive, num_retries);
      break;

    default:
      break;
  }

  return decision;
}

RetryPolicy::RetryDecision LoggingRetryPolicy::on_request_error(const Request* request,
                                                                CassConsistency cl,
                                                                const ErrorResponse* error,
                                                                int num_retries) const {
  RetryDecision decision = retry_policy_->on_request_error(request, cl, error, num_retries);

  switch (decision.type()) {
    case RetryDecision::IGNORE:
      LOG_INFO("Ignoring request error (initial consistency: %s, error: %s, retries: %d)",
               cass_consistency_string(cl), error->message().to_string().c_str(), num_retries);
      break;

    case RetryDecision::RETRY:
      LOG_INFO("Retrying on request error at consistency %s (initial consistency: %s, error: %s, "
               "retries: %d)",
               cass_consistency_string(decision.retry_consistency()), cass_consistency_string(cl),
               error->message().to_string().c_str(), num_retries);
      break;

    default:
      break;
  }

  return decision;
}
