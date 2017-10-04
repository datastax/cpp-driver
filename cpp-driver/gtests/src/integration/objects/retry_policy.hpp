/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_RETRY_POLICY_HPP__
#define __TEST_RETRY_POLICY_HPP__
#include "cassandra.h"

#include "objects/object_base.hpp"

namespace test {
namespace driver {

/**
 * Wrapped retry policy object
 */
class RetryPolicy : public Object<CassRetryPolicy, cass_retry_policy_free> {
public:
  /**
   * Create the retry policy object from the native driver retry policy object
   *
   * @param retry_policy Native driver object
   */
  RetryPolicy(CassRetryPolicy* retry_policy)
    : Object<CassRetryPolicy, cass_retry_policy_free>(retry_policy) {}

  /**
   * Create the retry policy object from the shared reference
   *
   * @param retry_policy Shared reference
   */
  RetryPolicy(Ptr retry_policy)
    : Object<CassRetryPolicy, cass_retry_policy_free>(retry_policy) {}
};

/**
 * Wrapped default retry policy
 */
class DefaultRetryPolicy : public RetryPolicy {
public:
  /**
   * Create the default retry policy object from the native driver default retry
   * policy object
   */
  DefaultRetryPolicy()
    : RetryPolicy(cass_retry_policy_default_new()) {}
};

/**
 * Wrapped downgrading consistency retry policy
 */
class DowngradingConsistencyRetryPolicy : public RetryPolicy {
public:
  /**
   * Create the downgrading consistency retry policy object from the native
   * driver downgrading consistency retry policy object
   */
  DowngradingConsistencyRetryPolicy()
    : RetryPolicy(cass_retry_policy_downgrading_consistency_new()) {}
};

/**
 * Wrapped fallthrough retry policy
 */
class FallthroughRetryPolicy : public RetryPolicy {
public:
  /**
   * Create the fallthrough retry policy object from the native driver
   * fallthrough retry policy object
   */
  FallthroughRetryPolicy()
    : RetryPolicy(cass_retry_policy_fallthrough_new()) {}
};

/**
 * Wrapped logging retry policy
 */
class LoggingRetryPolicy : public RetryPolicy {
public:
  /**
   * Create the logging retry policy object from the native driver logging retry
   * policy object with the given child policy
   *
   * @param child_policy Child retry policy being logged (CASS_LOG_INFO)
   */
  LoggingRetryPolicy(RetryPolicy child_policy)
    : RetryPolicy(cass_retry_policy_logging_new(child_policy.get())) {}
};

} // namespace driver
} // namespace test

#endif // __TEST_RETRY_POLICY_HPP__