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

#ifndef DATASTAX_INTERNAL_RETRY_POLICY_HPP
#define DATASTAX_INTERNAL_RETRY_POLICY_HPP

#include "cassandra.h"
#include "error_response.hpp"
#include "external.hpp"
#include "ref_counted.hpp"

#ifdef _WIN32
#ifdef IGNORE
#undef IGNORE
#endif
#endif

namespace datastax { namespace internal { namespace core {

class ErrorResponse;
class Request;

class RetryPolicy : public RefCounted<RetryPolicy> {
public:
  typedef SharedRefPtr<RetryPolicy> Ptr;

  enum Type { DEFAULT, DOWNGRADING, FALLTHROUGH, LOGGING };

  class RetryDecision {
  public:
    enum Type { RETURN_ERROR, RETRY, IGNORE };

    RetryDecision(Type type, CassConsistency retry_cl, bool retry_current_host)
        : type_(type)
        , retry_cl_(retry_cl)
        , retry_current_host_(retry_current_host) {}

    Type type() const { return type_; }
    CassConsistency retry_consistency() const { return retry_cl_; }
    bool retry_current_host() const { return retry_current_host_; }

    static RetryDecision return_error() {
      return RetryDecision(RETURN_ERROR, CASS_CONSISTENCY_UNKNOWN, false);
    }

    static RetryDecision retry(CassConsistency cl) { return RetryDecision(RETRY, cl, true); }

    static RetryDecision retry_next_host(CassConsistency cl) {
      return RetryDecision(RETRY, cl, false);
    }

    static RetryDecision ignore() { return RetryDecision(IGNORE, CASS_CONSISTENCY_UNKNOWN, false); }

  private:
    Type type_;
    CassConsistency retry_cl_;
    bool retry_current_host_;
  };

  RetryPolicy(Type type)
      : type_(type) {}

  virtual ~RetryPolicy() {}

  Type type() const { return type_; }

  virtual RetryDecision on_read_timeout(const Request* request, CassConsistency cl, int received,
                                        int required, bool data_recevied,
                                        int num_retries) const = 0;
  virtual RetryDecision on_write_timeout(const Request* request, CassConsistency cl, int received,
                                         int required, CassWriteType write_type,
                                         int num_retries) const = 0;
  virtual RetryDecision on_unavailable(const Request* request, CassConsistency cl, int required,
                                       int alive, int num_retries) const = 0;
  virtual RetryDecision on_request_error(const Request* request, CassConsistency cl,
                                         const ErrorResponse* error, int num_retries) const = 0;

private:
  Type type_;
};

class DefaultRetryPolicy : public RetryPolicy {
public:
  DefaultRetryPolicy()
      : RetryPolicy(DEFAULT) {}

  virtual RetryDecision on_read_timeout(const Request* request, CassConsistency cl, int received,
                                        int required, bool data_recevied, int num_retries) const;
  virtual RetryDecision on_write_timeout(const Request* request, CassConsistency cl, int received,
                                         int required, CassWriteType write_type,
                                         int num_retries) const;
  virtual RetryDecision on_unavailable(const Request* request, CassConsistency cl, int required,
                                       int alive, int num_retries) const;
  virtual RetryDecision on_request_error(const Request* request, CassConsistency cl,
                                         const ErrorResponse* error, int num_retries) const;
};

class DowngradingConsistencyRetryPolicy : public RetryPolicy {
public:
  DowngradingConsistencyRetryPolicy()
      : RetryPolicy(DOWNGRADING) {}

  virtual RetryDecision on_read_timeout(const Request* request, CassConsistency cl, int received,
                                        int required, bool data_recevied, int num_retries) const;
  virtual RetryDecision on_write_timeout(const Request* request, CassConsistency cl, int received,
                                         int required, CassWriteType write_type,
                                         int num_retries) const;
  virtual RetryDecision on_unavailable(const Request* request, CassConsistency cl, int required,
                                       int alive, int num_retries) const;
  virtual RetryDecision on_request_error(const Request* request, CassConsistency cl,
                                         const ErrorResponse* error, int num_retries) const;
};

class FallthroughRetryPolicy : public RetryPolicy {
public:
  FallthroughRetryPolicy()
      : RetryPolicy(FALLTHROUGH) {}

  virtual RetryDecision on_read_timeout(const Request* request, CassConsistency cl, int received,
                                        int required, bool data_recevied, int num_retries) const;
  virtual RetryDecision on_write_timeout(const Request* request, CassConsistency cl, int received,
                                         int required, CassWriteType write_type,
                                         int num_retries) const;
  virtual RetryDecision on_unavailable(const Request* request, CassConsistency cl, int required,
                                       int alive, int num_retries) const;
  virtual RetryDecision on_request_error(const Request* request, CassConsistency cl,
                                         const ErrorResponse* error, int num_retries) const;
};

class LoggingRetryPolicy : public RetryPolicy {
public:
  LoggingRetryPolicy(const RetryPolicy::Ptr& retry_policy)
      : RetryPolicy(LOGGING)
      , retry_policy_(retry_policy) {}

  virtual RetryDecision on_read_timeout(const Request* request, CassConsistency cl, int received,
                                        int required, bool data_recevied, int num_retries) const;
  virtual RetryDecision on_write_timeout(const Request* request, CassConsistency cl, int received,
                                         int required, CassWriteType write_type,
                                         int num_retries) const;
  virtual RetryDecision on_unavailable(const Request* request, CassConsistency cl, int required,
                                       int alive, int num_retries) const;
  virtual RetryDecision on_request_error(const Request* request, CassConsistency cl,
                                         const ErrorResponse* response, int num_retries) const;

private:
  RetryPolicy::Ptr retry_policy_;
};

}}} // namespace datastax::internal::core

EXTERNAL_TYPE(datastax::internal::core::RetryPolicy, CassRetryPolicy)

#endif
