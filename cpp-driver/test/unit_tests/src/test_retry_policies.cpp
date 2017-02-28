/*
  Copyright (c) 2014-2016 DataStax

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

#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "retry_policy.hpp"

#include <boost/test/unit_test.hpp>

typedef cass::RetryPolicy RetryPolicy;
typedef cass::RetryPolicy::RetryDecision RetryDecision;

void check_decision(RetryDecision decision,
                    RetryDecision::Type type,
                    CassConsistency cl,
                    bool retry_current_host) {
  BOOST_CHECK_EQUAL(decision.type(), type);
  BOOST_CHECK_EQUAL(decision.retry_consistency(), cl);
  BOOST_CHECK_EQUAL(decision.retry_current_host(), retry_current_host);
}


void check_default(cass::RetryPolicy& policy) {
  // Read timeout
  {
    // Retry because data wasn't present
    check_decision(policy.on_read_timeout(NULL, CASS_CONSISTENCY_QUORUM, 3, 3, false, 0),
                   RetryDecision::RETRY,
                   CASS_CONSISTENCY_QUORUM,
                   true);

    // Return error because recieved < required
    check_decision(policy.on_read_timeout(NULL, CASS_CONSISTENCY_QUORUM, 2, 3, false, 0),
                   RetryDecision::RETURN_ERROR,
                   CASS_CONSISTENCY_UNKNOWN,
                   false);

    // Return error because a retry has already happened
    check_decision(policy.on_read_timeout(NULL, CASS_CONSISTENCY_QUORUM, 3, 3, false, 1),
                   RetryDecision::RETURN_ERROR,
                   CASS_CONSISTENCY_UNKNOWN,
                   false);
  }

  // Write timeout
  {
    // Retry because of batch log failed to write
    check_decision(policy.on_write_timeout(NULL, CASS_CONSISTENCY_QUORUM, 3, 3, CASS_WRITE_TYPE_BATCH_LOG, 0),
                   RetryDecision::RETRY,
                   CASS_CONSISTENCY_QUORUM,
                   true);

    // Return error because a retry has already happened
    check_decision(policy.on_write_timeout(NULL, CASS_CONSISTENCY_QUORUM, 3, 3, CASS_WRITE_TYPE_BATCH_LOG, 1),
                   RetryDecision::RETURN_ERROR,
                   CASS_CONSISTENCY_UNKNOWN,
                   false);
  }

  // Unavailable
  {
    // Retry with next host
    check_decision(policy.on_unavailable(NULL, CASS_CONSISTENCY_QUORUM, 3, 3, 0),
                   RetryDecision::RETRY,
                   CASS_CONSISTENCY_QUORUM,
                   false);

    // Return error because a retry has already happened
    check_decision(policy.on_unavailable(NULL, CASS_CONSISTENCY_QUORUM, 3, 3, 1),
                   RetryDecision::RETURN_ERROR,
                   CASS_CONSISTENCY_UNKNOWN,
                   false);
  }
}

BOOST_AUTO_TEST_SUITE(retry_policies)

BOOST_AUTO_TEST_CASE(default_policy)
{
  cass::DefaultRetryPolicy policy;
  check_default(policy);
}

BOOST_AUTO_TEST_CASE(downgrading)
{
  cass::DowngradingConsistencyRetryPolicy policy;

  // Read timeout
  {
    // Retry because data wasn't present
    check_decision(policy.on_read_timeout(NULL, CASS_CONSISTENCY_QUORUM, 3, 3, false, 0),
                   RetryDecision::RETRY,
                   CASS_CONSISTENCY_QUORUM,
                   true);

    // Downgrade consistency to three
    check_decision(policy.on_read_timeout(NULL, CASS_CONSISTENCY_QUORUM, 3, 4, false, 0),
                   RetryDecision::RETRY,
                   CASS_CONSISTENCY_THREE,
                   true);

    // Downgrade consistency to two
    check_decision(policy.on_read_timeout(NULL, CASS_CONSISTENCY_QUORUM, 2, 4, false, 0),
                   RetryDecision::RETRY,
                   CASS_CONSISTENCY_TWO,
                   true);

    // Downgrade consistency to one
    check_decision(policy.on_read_timeout(NULL, CASS_CONSISTENCY_QUORUM, 1, 4, false, 0),
                   RetryDecision::RETRY,
                   CASS_CONSISTENCY_ONE,
                   true);

    // Return error because no copies
    check_decision(policy.on_read_timeout(NULL, CASS_CONSISTENCY_QUORUM, 0, 4, false, 0),
                   RetryDecision::RETURN_ERROR,
                   CASS_CONSISTENCY_UNKNOWN,
                   false);

    // Return error because a retry has already happened
    check_decision(policy.on_read_timeout(NULL, CASS_CONSISTENCY_QUORUM, 3, 3, false, 1),
                   RetryDecision::RETURN_ERROR,
                   CASS_CONSISTENCY_UNKNOWN,
                   false);
  }

  // Write timeout
  {
    // Ignore if at least one copy
    check_decision(policy.on_write_timeout(NULL, CASS_CONSISTENCY_QUORUM, 1, 3, CASS_WRITE_TYPE_SIMPLE, 0),
                   RetryDecision::IGNORE,
                   CASS_CONSISTENCY_UNKNOWN,
                   false);

    // Ignore if at least one copy
    check_decision(policy.on_write_timeout(NULL, CASS_CONSISTENCY_QUORUM, 1, 3, CASS_WRITE_TYPE_BATCH, 0),
                   RetryDecision::IGNORE,
                   CASS_CONSISTENCY_UNKNOWN,
                   false);

    // Return error if no copies
    check_decision(policy.on_write_timeout(NULL, CASS_CONSISTENCY_QUORUM, 0, 3, CASS_WRITE_TYPE_SIMPLE, 0),
                   RetryDecision::RETURN_ERROR,
                   CASS_CONSISTENCY_UNKNOWN,
                   false);

    // Downgrade consistency to two
    check_decision(policy.on_write_timeout(NULL, CASS_CONSISTENCY_QUORUM, 2, 3, CASS_WRITE_TYPE_UNLOGGED_BATCH, 0),
                   RetryDecision::RETRY,
                   CASS_CONSISTENCY_TWO,
                   true);

    // Retry because of batch log failed to write
    check_decision(policy.on_write_timeout(NULL, CASS_CONSISTENCY_QUORUM, 3, 3, CASS_WRITE_TYPE_BATCH_LOG, 0),
                   RetryDecision::RETRY,
                   CASS_CONSISTENCY_QUORUM,
                   true);

    // Return error because a retry has already happened
    check_decision(policy.on_write_timeout(NULL, CASS_CONSISTENCY_QUORUM, 3, 3, CASS_WRITE_TYPE_BATCH_LOG, 1),
                   RetryDecision::RETURN_ERROR,
                   CASS_CONSISTENCY_UNKNOWN,
                   false);
  }

  // Unavailable
  {
    // Retry with next host
    check_decision(policy.on_unavailable(NULL, CASS_CONSISTENCY_QUORUM, 3, 2, 0),
                   RetryDecision::RETRY,
                   CASS_CONSISTENCY_TWO,
                   true);

    // Return error because a retry has already happened
    check_decision(policy.on_unavailable(NULL, CASS_CONSISTENCY_QUORUM, 3, 3, 1),
                   RetryDecision::RETURN_ERROR,
                   CASS_CONSISTENCY_UNKNOWN,
                   false);
  }
}

BOOST_AUTO_TEST_CASE(fallthrough)
{
  cass::FallthroughRetryPolicy policy;

  // Always fail

  check_decision(policy.on_read_timeout(NULL, CASS_CONSISTENCY_QUORUM, 3, 3, false, 0),
                 RetryDecision::RETURN_ERROR,
                 CASS_CONSISTENCY_UNKNOWN,
                 false);

  check_decision(policy.on_write_timeout(NULL, CASS_CONSISTENCY_QUORUM, 3, 3, CASS_WRITE_TYPE_SIMPLE, 0),
                 RetryDecision::RETURN_ERROR,
                 CASS_CONSISTENCY_UNKNOWN,
                 false);

  check_decision(policy.on_unavailable(NULL, CASS_CONSISTENCY_QUORUM, 3, 3, 0),
                 RetryDecision::RETURN_ERROR,
                 CASS_CONSISTENCY_UNKNOWN,
                 false);
}

BOOST_AUTO_TEST_CASE(logging)
{
  cass::SharedRefPtr<cass::DefaultRetryPolicy> policy(new cass::DefaultRetryPolicy());
  cass::LoggingRetryPolicy logging_policy(policy);
  cass_log_set_level(CASS_LOG_INFO);
  check_default(logging_policy);
}

BOOST_AUTO_TEST_SUITE_END()


