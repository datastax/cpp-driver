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

#ifndef __CASS_RETRY_POLICY_HPP_INCLUDED__
#define __CASS_RETRY_POLICY_HPP_INCLUDED__

#include "cassandra.h"
#include "ref_counted.hpp"

namespace cass {

class RetryPolicy : public RefCounted<RetryPolicy> {
public:
  enum WriteType {
    SIMPLE,
    BATCH,
    UNLOGGED_BATCH,
    COUNTER,
    BATCH_LOG
  };

  class RetryDecision {
  public:
    enum Type {
      RETURN_ERROR,
      RETRY,
      IGNORE
    };

    RetryDecision(Type type, CassConsistency retry_cl, bool retry_current_host)
      : type_(type)
      , retry_cl_(retry_cl)
      , retry_current_host_(retry_current_host) { }

    Type type() const { return type_; }
    CassConsistency retry_consistency() const { return retry_cl_; }
    bool retry_current_host() const { return retry_current_host_; }

    static RetryDecision return_error() {
      return RetryDecision(RETURN_ERROR, CASS_CONSISTENCY_UNKNOWN, false);
    }

    static RetryDecision retry(CassConsistency cl) {
      return RetryDecision(RETRY, cl, true);
    }

    static RetryDecision retry_next_host(CassConsistency cl) {
      return RetryDecision(RETRY, cl, false);
    }

    static RetryDecision ignore() {
      return RetryDecision(IGNORE, CASS_CONSISTENCY_UNKNOWN, false);
    }

  private:
    Type type_;
    CassConsistency retry_cl_;
    bool retry_current_host_;
  };

  virtual ~RetryPolicy() { }

  virtual RetryDecision on_read_timeout(CassConsistency cl, int received, int required, bool data_recevied, int num_retries) const = 0;
  virtual RetryDecision on_write_timeout(CassConsistency cl, int received, int required, WriteType write_type, int num_retries) const = 0;
  virtual RetryDecision on_unavailable(CassConsistency cl, int required, int alive, int num_retries) const = 0;
};

class DefaultRetryPolicy : public RetryPolicy {
public:
  virtual RetryDecision on_read_timeout(CassConsistency cl, int received, int required, bool data_recevied, int num_retries) const;
  virtual RetryDecision on_write_timeout(CassConsistency cl, int received, int required, WriteType write_type, int num_retries) const;
  virtual RetryDecision on_unavailable(CassConsistency cl, int required, int alive, int num_retries) const;
};

class DowngradingConsistencyRetryPolicy : public RetryPolicy {
public:
  virtual RetryDecision on_read_timeout(CassConsistency cl, int received, int required, bool data_recevied, int num_retries) const;
  virtual RetryDecision on_write_timeout(CassConsistency cl, int received, int required, WriteType write_type, int num_retries) const;
  virtual RetryDecision on_unavailable(CassConsistency cl, int required, int alive, int num_retries) const;
};

class FallthroughRetryPolicy : public RetryPolicy {
public:
  virtual RetryDecision on_read_timeout(CassConsistency cl, int received, int required, bool data_recevied, int num_retries) const;
  virtual RetryDecision on_write_timeout(CassConsistency cl, int received, int required, WriteType write_type, int num_retries) const;
  virtual RetryDecision on_unavailable(CassConsistency cl, int required, int alive, int num_retries) const;
};

class LoggingRetryPolicy : public RetryPolicy {
public:
  virtual RetryDecision on_read_timeout(CassConsistency cl, int received, int required, bool data_recevied, int num_retries) const;
  virtual RetryDecision on_write_timeout(CassConsistency cl, int received, int required, WriteType write_type, int num_retries) const;
  virtual RetryDecision on_unavailable(CassConsistency cl, int required, int alive, int num_retries) const;
};

} // namespace cass

#endif

