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

#include "integration.hpp"

#include "constants.hpp"

/**
 * Execution profile integration tests
 */
class ExecutionProfileTest : public Integration {
public:
  ExecutionProfileTest()
      : insert_(NULL)
      , child_retry_policy_(IgnoreRetryPolicy::policy()) // Used for counting retry
      , logging_retry_policy_(child_retry_policy_)
      , skip_base_execution_profile_(false) {
    replication_factor_ = 2;
    number_dc1_nodes_ = 2;
  }

  void SetUp() {
    // Calculate the total number of nodes being used
    total_nodes_ = number_dc1_nodes_ + number_dc2_nodes_;

    // Create the execution profiles for the test cases
    if (!skip_base_execution_profile_) {
      profiles_["request_timeout"] = ExecutionProfile::build().with_request_timeout(1);
      profiles_["consistency"] =
          ExecutionProfile::build().with_consistency(CASS_CONSISTENCY_SERIAL);
      profiles_["serial_consistency"] =
          ExecutionProfile::build().with_serial_consistency(CASS_CONSISTENCY_ONE);
      profiles_["round_robin"] =
          ExecutionProfile::build().with_load_balance_round_robin().with_token_aware_routing(false);
      profiles_["latency_aware"] =
          ExecutionProfile::build().with_latency_aware_routing().with_load_balance_round_robin();
      profiles_["token_aware"] =
          ExecutionProfile::build().with_token_aware_routing().with_load_balance_round_robin();
      profiles_["blacklist"] = ExecutionProfile::build()
                                   .with_blacklist_filtering(Options::host_prefix() + "1")
                                   .with_load_balance_round_robin();
      profiles_["whitelist"] = ExecutionProfile::build()
                                   .with_whitelist_filtering(Options::host_prefix() + "1")
                                   .with_load_balance_round_robin();
      profiles_["retry_policy"] = ExecutionProfile::build()
                                      .with_retry_policy(logging_retry_policy_)
                                      .with_consistency(CASS_CONSISTENCY_THREE);
      profiles_["speculative_execution"] =
          ExecutionProfile::build().with_constant_speculative_execution_policy(100, 20);
    }

    // Call the parent setup function
    Integration::SetUp();

    // Create the table
    session_.execute(
        format_string("CREATE TABLE %s (key text PRIMARY KEY, value int)", table_name_.c_str()));

    // Create the insert statement for later use
    insert_ = Statement(format_string("INSERT INTO %s (key, value) VALUES (?, ?) IF NOT EXISTS",
                                      table_name_.c_str()),
                        2);
    insert_.bind<Text>(0, Text(test_name_));
    insert_.bind<Integer>(1, Integer(1000));

    // Insert an expected value (if not the serial consistency test)
    session_.execute(insert_);
  }

protected:
  /**
   * Total number of nodes being used in the cluster
   */
  size_t total_nodes_;
  /**
   * Simple insert statement (bounded)
   */
  Statement insert_;
  /**
   * Child retry policy for 'retry_policy' execution profile
   */
  RetryPolicy child_retry_policy_;
  /**
   * Logging retry policy for 'retry_policy' execution profile
   */
  LoggingRetryPolicy logging_retry_policy_;
  /**
   * Flag to determine if base execution profiles should be built or not
   */
  bool skip_base_execution_profile_;

  /**
   * Get the primary replica host/IP address for an executed statement with a
   * given routing value.
   *
   * @param value Value to use when determining token for replica evaluation
   */
  std::string get_primary_replica(const std::string& value) {
    // Ensure the tokens for the cluster have been discovered
    if (tokens_.empty()) {
      build_tokens();
    }

    // Generate the Murmur3 hash lookup token
    int64_t token = murmur3_hash(value);

    // Determine the host/IP address for the token
    TokenMap::const_iterator it = tokens_.upper_bound(token);
    if (it == tokens_.end()) it = tokens_.begin();
    return it->second.first;
  }

private:
  /**
   * Simple pair for host mapping
   *   first:  IP Address
   *   second: Data Center
   */
  typedef std::pair<std::string, std::string> Host;
  /**
   * Token mapping
   *   first:  Token
   *   second: Host mapping
   */
  typedef std::map<int64_t, Host> TokenMap;
  /**
   * Token pair (insert)
   */
  typedef std::pair<int64_t, Host> TokenPair;

  /**
   * Token/host mapping
   */
  TokenMap tokens_;

  /**
   * Build the token/host mapping for the current cluster
   */
  void build_tokens() {
    // Build the token/host mapping
    Session session = cluster_.connect();
    Statement statement("SELECT data_center, tokens FROM system.local");
    statement.set_execution_profile("round_robin");
    for (size_t i = 0; i < total_nodes_; ++i) {
      // Execute the statement and retrieve the host IP address
      Result result = session.execute(statement);
      std::string ip_address = result.host();

      // Get the data center and token values
      Row row = result.first_row();
      Text data_center = row.next().as<Text>();
      std::set<Text> tokens = row.next().as<Set<Text> >().value();

      // Iterate over the tokens and update the token/host mapping
      for (std::set<Text>::const_iterator it = tokens.begin(); it != tokens.end(); ++it) {
        int64_t token = 0;
        std::stringstream token_value(it->value());
        if ((token_value >> token).fail()) {
          FAIL() << "Unable To Parse Tokens from Cluster: Test cannot complete";
        }
        tokens_.insert(TokenPair(token, Host(ip_address, data_center.value())));
      }
    }
  }
};

/**
 * Execution profile integration tests using data centers.
 */
class DCExecutionProfileTest : public ExecutionProfileTest {
public:
  DCExecutionProfileTest() { number_dc2_nodes_ = 1; }

  void SetUp() {
    // Create the execution profiles for the test cases
    profiles_["dc_aware"] = ExecutionProfile::build()
                                .with_load_balance_dc_aware("dc1", 1, false)
                                .with_consistency(CASS_CONSISTENCY_LOCAL_ONE);
    profiles_["blacklist_dc"] = ExecutionProfile::build()
                                    .with_blacklist_dc_filtering("dc1")
                                    .with_load_balance_dc_aware("dc1", 1, true)
                                    .with_consistency(CASS_CONSISTENCY_LOCAL_ONE);
    profiles_["whitelist_dc"] = ExecutionProfile::build()
                                    .with_whitelist_dc_filtering("dc2")
                                    .with_load_balance_dc_aware("dc1", 1, true)
                                    .with_consistency(CASS_CONSISTENCY_LOCAL_ONE);

    // Call the parent setup function
    skip_base_execution_profile_ = true;
    ExecutionProfileTest::SetUp();
  }
};

/**
 * Attempt to utilize an invalid execution profile on a statement
 *
 * This test will perform a query using statement requests with an invalid
 * execution profile assigned.The statement request will not be executed and
 * a `CASS_ERROR_LIB_EXECUTION_PROFILE_INVALID` should be returned by the
 * future profile.
 *
 * @jira_ticket CPP-492
 * @test_category execution_profiles
 * @since DSE 1.4.0
 * @expected_result Statement request will not execute and an invalid profile
 *                  error will occur.
 */
CASSANDRA_INTEGRATION_TEST_F(ExecutionProfileTest, InvalidName) {
  CHECK_FAILURE;

  // Create a statement for failed execution profile execution
  Statement statement(default_select_all());

  // Execute a batched query with assigned profile (should timeout)
  // NOTE: Selects are not allowed in batches but is OK for this test case
  Batch batch;
  batch.add(statement);
  batch.set_execution_profile("invlalid_execution_profile");
  Result result = session_.execute(batch, false);
  ASSERT_EQ(CASS_ERROR_LIB_EXECUTION_PROFILE_INVALID, result.error_code());

  // Execute a simple query with assigned profile (should timeout)
  statement.set_execution_profile("invlalid_execution_profile");
  result = session_.execute(statement, false);
  ASSERT_EQ(CASS_ERROR_LIB_EXECUTION_PROFILE_INVALID, result.error_code());
}

/**
 * Utilize the execution profile to override statement request timeout
 *
 * This test will perform a query using the statement request timeout and the
 * execution profile 'request_timeout'; overriding the default request timeout
 * setting. The default request timeout should succeed where as the execution
 * profile should fail due to 1ms request timeout applied to profile.
 *
 * @jira_ticket CPP-492
 * @test_category execution_profiles
 * @since DSE 1.4.0
 * @expected_result Default request timeout will succeed; where as execution
 *                  profile will fail.
 */
CASSANDRA_INTEGRATION_TEST_F(ExecutionProfileTest, RequestTimeout) {
  CHECK_FAILURE;

  // Execute a simple query without assigned profile
  Statement statement(default_select_all());
  Result result = session_.execute(statement);
  ASSERT_EQ(CASS_OK, result.error_code());

  // Execute a batched query with assigned profile (should timeout)
  // NOTE: Selects are not allowed in batches but is OK for this test case
  Batch batch;
  batch.add(statement);
  batch.set_execution_profile("request_timeout");
  result = session_.execute(batch, false);
  ASSERT_EQ(CASS_ERROR_LIB_REQUEST_TIMED_OUT, result.error_code());

  // Execute a simple query with assigned profile (should timeout)
  statement.set_execution_profile("request_timeout");
  result = session_.execute(statement, false);
  ASSERT_EQ(CASS_ERROR_LIB_REQUEST_TIMED_OUT, result.error_code());
}

/**
 * Utilize the execution profile to override statement consistency
 *
 * This test will perform a query using the default consistency and the
 * execution profile 'consistency'; overriding the default setting. The
 * default consistency should succeed where as the execution profile should fail
 * due to an invalid consistency applied to profile.
 *
 * @jira_ticket CPP-492
 * @test_category execution_profiles
 * @since DSE 1.4.0
 * @expected_result Default consistency will succeed; where as execution profile
 *                  will fail.
 */
CASSANDRA_INTEGRATION_TEST_F(ExecutionProfileTest, Consistency) {
  CHECK_FAILURE;

  // Execute a simple query without assigned profile
  Result result = session_.execute(insert_);
  ASSERT_EQ(CASS_OK, result.error_code());

  // Execute a batched query with assigned profile (should fail)
  Batch batch;
  batch.add(insert_);
  batch.set_execution_profile("consistency");
  result = session_.execute(batch, false);
  ASSERT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, result.error_code());
  CCM::CassVersion cass_version = server_version_;
  if (!Options::is_cassandra()) {
    cass_version = static_cast<CCM::DseVersion>(cass_version).get_cass_version();
  }
  std::string expected_message = "SERIAL is not supported as conditional update commit consistency";
  ASSERT_TRUE(contains(result.error_message(), expected_message));

  // Execute a simple query with assigned profile (should fail)
  insert_.set_execution_profile("consistency");
  result = session_.execute(insert_, false);
  ASSERT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, result.error_code());
  ASSERT_TRUE(contains(result.error_message(),
                       "SERIAL is not supported as conditional update commit consistency"));
}

/**
 * Utilize the execution profile to override statement serial consistency
 *
 * This test will perform an insert query using the execution profile
 * 'serial_consistency'; overriding the default setting. The execution profile
 * should fail due to an invalid serial consistency applied to profile.
 *
 * @jira_ticket CPP-492
 * @test_category execution_profiles
 * @since DSE 1.4.0
 * @expected_result Execution profile will fail (invalid serial consistency)
 */
CASSANDRA_INTEGRATION_TEST_F(ExecutionProfileTest, SerialConsistency) {
  CHECK_VERSION(2.0.0);
  CHECK_FAILURE;

  // Execute a batched query with assigned profile (should fail
  Batch batch;
  batch.add(insert_);
  batch.set_execution_profile("serial_consistency");
  Result result = session_.execute(batch, false);
  ASSERT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, result.error_code());
  ASSERT_TRUE(contains(
      result.error_message(),
      "Invalid consistency for conditional update. Must be one of SERIAL or LOCAL_SERIAL"));

  // Execute a simple query with assigned profile (should fail)
  insert_.set_execution_profile("serial_consistency");
  result = session_.execute(insert_, false);
  ASSERT_EQ(CASS_ERROR_SERVER_INVALID_QUERY, result.error_code());
  ASSERT_TRUE(contains(
      result.error_message(),
      "Invalid consistency for conditional update. Must be one of SERIAL or LOCAL_SERIAL"));
}

/**
 * Utilize the execution profile to override the statement load balancing policy
 *
 * This test will perform an insert query using the execution profile
 * 'round_robin' and the default cluster profile configuration. Both profiles
 * will succeed and the round robin statement execution will be validated to
 * ensure that all nodes are acted upon in sequential order.
 *
 * @jira_ticket CPP-492
 * @test_category execution_profiles
 * @since DSE 1.4.0
 * @expected_result Execution profile will execute in sequential order across
 *                  all nodes in the cluster (local and remote data centers)
 */
CASSANDRA_INTEGRATION_TEST_F(ExecutionProfileTest, RoundRobin) {
  CHECK_FAILURE;

  // Execute statements over all the nodes in the cluster twice
  for (size_t i = 0; i < total_nodes_ * 2; ++i) {
    // Execute the same query with the cluster default profile
    insert_.set_execution_profile(""); // Reset the insert statement
    Result result = session_.execute(insert_);
    ASSERT_EQ(CASS_OK, result.error_code());

    // Determine the expected IP address for the statement execution
    int expected_node = ((i * 2) % total_nodes_) + 1;
    std::stringstream expected_ip_address;
    expected_ip_address << ccm_->get_ip_prefix() << expected_node;

    // Execute a batched query with assigned profile
    Batch batch;
    batch.add(insert_);
    batch.set_execution_profile("round_robin");
    result = session_.execute(batch);
    ASSERT_EQ(CASS_OK, result.error_code());
    ASSERT_STREQ(expected_ip_address.str().c_str(), result.host().c_str());

    // Increment the expected round robin IP address
    expected_ip_address.str("");
    expected_ip_address << ccm_->get_ip_prefix() << expected_node + 1;

    // Execute a simple query with assigned profile
    insert_.set_execution_profile("round_robin");
    result = session_.execute(insert_);
    ASSERT_EQ(CASS_OK, result.error_code());
    ASSERT_STREQ(expected_ip_address.str().c_str(), result.host().c_str());
  }
}

/**
 * Utilize the execution profile to override the statement load balancing policy
 *
 * This test will perform an insert query using the execution profile
 * 'latency_aware'. The profile will succeed and the latency aware statement
 * execution will be validated to ensure that a new minimum average was
 * calculated on execution.
 *
 * NOTE: This test will not test the validity of the latency aware routing
 * only the fact that it was executed with the statement.
 *
 * @jira_ticket CPP-492
 * @test_category execution_profiles
 * @since DSE 1.4.0
 * @expected_result Execution profile will execute the latency aware routing
 */
CASSANDRA_INTEGRATION_TEST_F(ExecutionProfileTest, LatencyAwareRouting) {
  CHECK_FAILURE;

  // Execute batch with the assigned profile and add criteria for the logger
  logger_.add_critera("Calculated new minimum");
  for (int i = 0; i < 1000; ++i) {
    Batch batch;
    batch.add(insert_);
    batch.set_execution_profile("latency_aware");
    Result result = session_.execute(batch);
    ASSERT_EQ(CASS_OK, result.error_code());
  }
  ASSERT_GE(logger_.count(), 1u);

  // Execute the insert statement multiple times and result logger count
  insert_.set_execution_profile("latency_aware");
  logger_.reset_count();
  ASSERT_EQ(0u, logger_.count());
  for (int i = 0; i < 1000; ++i) {
    Result result = session_.execute(insert_);
    ASSERT_EQ(CASS_OK, result.error_code());
  }

  // Ensure the latency aware routing average was updated for the profile
  ASSERT_GE(logger_.count(), 1u);
}

/**
 * Utilize the execution profile to override the statement load balancing policy
 *
 * This test will perform an insert query using the execution profile
 * 'token_aware'. The profile will succeed and the token aware routing statement
 * execution will be validated to ensure that the appropriate replica was used.
 *
 * @jira_ticket CPP-492
 * @test_category execution_profiles
 * @since DSE 1.4.0
 * @expected_result Execution profile will execute the token aware routing will
 *                  execute on the appropriate replica
 */
CASSANDRA_INTEGRATION_TEST_F(ExecutionProfileTest, TokenAwareRouting) {
  CHECK_FAILURE;

  // Update the existing insert statement for token aware routing
  insert_.add_key_index(0);
  insert_.set_keyspace(keyspace_name_);

  // Execute batch statements multiple time to generate a multitude of tokens
  for (int i = 0; i < 10; ++i) {
    // Generate the value for the routing key
    std::stringstream value;
    value << i;

    // Execute a batched query with assigned profile
    insert_.bind<Text>(0, Text(value.str()));
    Batch batch;
    batch.add(insert_);
    batch.set_execution_profile("token_aware");
    Result result = session_.execute(batch);
    ASSERT_EQ(CASS_OK, result.error_code());

    // Validate the correct replica/token was used
    ASSERT_STREQ(get_primary_replica(value.str()).c_str(), result.host().c_str());
  }

  // Assign the execution profile for token aware routing
  insert_.set_execution_profile("token_aware");

  // Execute statements multiple time to generate a multitude of tokens
  for (int i = 0; i < 10; ++i) {
    // Generate the value for the routing key
    std::stringstream value;
    value << i;

    // Execute a simple query with assigned profile and set for token aware
    insert_.bind<Text>(0, Text(value.str()));
    Result result = session_.execute(insert_);
    ASSERT_EQ(CASS_OK, result.error_code());

    // Validate the correct replica/token was used
    ASSERT_STREQ(get_primary_replica(value.str()).c_str(), result.host().c_str());
  }
}

/**
 * Utilize the execution profile to override the statement load balancing policy
 *
 * This test will perform an insert query using the execution profile
 * 'blacklist' and the default cluster profile configuration. Both profiles
 * will succeed and the blacklist statement execution will be validated to
 * ensure that only one node is acted upon.
 *
 * @jira_ticket CPP-492
 * @test_category execution_profiles
 * @since DSE 1.4.0
 * @expected_result Execution profile will execute using only one node
 */
CASSANDRA_INTEGRATION_TEST_F(ExecutionProfileTest, BlacklistFiltering) {
  CHECK_FAILURE;

  // Indicate the expected IP address for the batch and statement execution
  std::string expected_ip_address = ccm_->get_ip_prefix() + "2";

  // Create a batched query with assigned profile
  Batch batch;
  batch.add(insert_);
  batch.set_execution_profile("blacklist");

  // Execute batch statements over all the nodes in the cluster twice
  for (size_t i = 0; i < total_nodes_ * 2; ++i) {
    Result result = session_.execute(batch);
    ASSERT_EQ(CASS_OK, result.error_code());
    ASSERT_STREQ(expected_ip_address.c_str(), result.host().c_str());
  }

  // Execute statements over all the nodes in the cluster twice
  for (size_t i = 0; i < total_nodes_ * 2; ++i) {
    // Execute the same query with the cluster default profile
    insert_.set_execution_profile(""); // Reset the insert statement
    Result result = session_.execute(insert_);
    ASSERT_EQ(CASS_OK, result.error_code());

    // Execute a simple query with assigned profile
    insert_.set_execution_profile("blacklist");
    result = session_.execute(insert_);
    ASSERT_EQ(CASS_OK, result.error_code());
    ASSERT_STREQ(expected_ip_address.c_str(), result.host().c_str());
  }
}

/**
 * Utilize the execution profile to override the statement load balancing policy
 *
 * This test will perform an insert query using the execution profile
 * 'whitelist' and the default cluster profile configuration. Both profiles
 * will succeed and the whitelist statement execution will be validated to
 * ensure that only one node is acted upon.
 *
 * @jira_ticket CPP-492
 * @test_category execution_profiles
 * @since DSE 1.4.0
 * @expected_result Execution profile will execute using only one node
 */
CASSANDRA_INTEGRATION_TEST_F(ExecutionProfileTest, WhitelistFiltering) {
  CHECK_FAILURE;

  // Indicate the expected IP address for the batch and statement execution
  std::string expected_ip_address = ccm_->get_ip_prefix() + "1";

  // Create a batched query with assigned profile
  Batch batch;
  batch.add(insert_);
  batch.set_execution_profile("whitelist");

  // Execute batch statements over all the nodes in the cluster twice
  for (size_t i = 0; i < total_nodes_ * 2; ++i) {
    Result result = session_.execute(batch);
    ASSERT_EQ(CASS_OK, result.error_code());
    ASSERT_STREQ(expected_ip_address.c_str(), result.host().c_str());
  }

  // Execute statements over all the nodes in the cluster twice
  for (size_t i = 0; i < total_nodes_ * 2; ++i) {
    // Execute the same query with the cluster default profile
    insert_.set_execution_profile(""); // Reset the insert statement
    Result result = session_.execute(insert_);
    ASSERT_EQ(CASS_OK, result.error_code());

    // Execute a simple query with assigned profile
    insert_.set_execution_profile("whitelist");
    result = session_.execute(insert_);
    ASSERT_EQ(CASS_OK, result.error_code());
    ASSERT_STREQ(expected_ip_address.c_str(), result.host().c_str());
  }
}

/**
 * Utilize the execution profile to override the statement retry policy
 *
 * This test will perform an insert query using the execution profile
 * 'retry_policy' and the default cluster profile configuration. Both profiles
 * will succeed and the retry policy statement execution will be validated to
 * ensure that it was selected when execution against a profile.
 *
 * @jira_ticket CPP-492
 * @test_category execution_profiles
 * @since DSE 1.4.0
 * @expected_result Execution profile will execute retry policy will be
 *                  validated.
 */
CASSANDRA_INTEGRATION_TEST_F(ExecutionProfileTest, RetryPolicy) {
  CHECK_FAILURE;

  // Create a logger criteria for retry policy validation
  logger_.add_critera("Ignoring unavailable error");

  // Execute a simple query without assigned profile
  Statement statement(default_select_all());
  Result result = session_.execute(statement);
  ASSERT_EQ(CASS_OK, result.error_code());
  ASSERT_EQ(0u, logger_.count());

  // NOTE: Tested locally with batch to ensure profiles are set with correct
  //      retry policy (if available)

  // Execute a simple query with assigned profile
  statement.set_execution_profile("retry_policy");
  result = session_.execute(statement);
  ASSERT_EQ(CASS_OK, result.error_code());
  ASSERT_EQ(1u, logger_.count());
}

/**
 * Utilize the execution profile to override the default speculative execution
 * policy
 *
 * This test will perform a select query using the execution profile
 * 'speculative_execution' and the default cluster profile configuration.
 * The default profile will only execute against one node while the execution
 * profile will execute against all the nodes in the cluster.
 *
 * @jira_ticket CPP-404
 * @test_category execution_profiles
 * @since DSE 1.6.0
 * @cassandra_version 2.2.0 (Required only for testing due to UDF usage)
 * @expected_result Execution profile will execute speculative execution policy
 *                  and validate attempted hosts.
 */
CASSANDRA_INTEGRATION_TEST_F(ExecutionProfileTest, SpeculativeExecutionPolicy) {
  CHECK_FAILURE;
  CHECK_VERSION(2.2.0);

  // Create the UDF timeout
  session_.execute("CREATE OR REPLACE FUNCTION timeout(arg int) "
                   "RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java "
                   "AS $$ long start = System.currentTimeMillis(); "
                   "while(System.currentTimeMillis() - start < arg) {"
                   ";;"
                   "}"
                   "return arg;"
                   "$$;");

  // Execute a simple query without assigned profile using timeout UDF
  Statement statement(format_string("SELECT timeout(value) FROM %s WHERE key='%s'",
                                    table_name_.c_str(), test_name_.c_str()));
  statement.set_idempotent(true);
  statement.set_record_attempted_hosts(true);
  Result result = session_.execute(statement);
  ASSERT_EQ(CASS_OK, result.error_code());
  ASSERT_EQ(1u, result.attempted_hosts().size());

  // Execute a simple query with assigned profile
  statement.set_execution_profile("speculative_execution");
  result = session_.execute(statement);
  ASSERT_EQ(CASS_OK, result.error_code());
  ASSERT_EQ(number_dc1_nodes_, result.attempted_hosts().size());
}

/**
 * Utilize the execution profile to override the statement load balancing policy
 *
 * This test will perform an insert query using the execution profile
 * 'dc_aware' and the default cluster profile configuration. Both profiles
 * will succeed and the data center aware statement execution will be validated
 * to ensure that all nodes are acted upon in the data center in sequential
 * order.
 *
 * NOTE: The local data center will be 'dc1'
 *
 * @jira_ticket CPP-492
 * @test_category execution_profiles
 * @since DSE 1.4.0
 * @expected_result Execution profile will execute in sequential order across
 *                  all nodes in the local data center for the cluster
 */
CASSANDRA_INTEGRATION_TEST_F(DCExecutionProfileTest, DCAware) {
  CHECK_FAILURE;

  // Execute statements over all the nodes in the cluster twice
  for (size_t i = 0; i < total_nodes_ * 2; ++i) {
    // Execute the same query with the cluster default profile
    insert_.set_execution_profile(""); // Reset the insert statement
    Result result = session_.execute(insert_);
    ASSERT_EQ(CASS_OK, result.error_code());

    // Determine the expected IP address for the statement execution
    int expected_node = ((i * 2) % number_dc1_nodes_) + 1;
    std::stringstream expected_ip_address;
    expected_ip_address << ccm_->get_ip_prefix() << expected_node;

    // Execute a batched query with assigned profile
    Batch batch;
    batch.add(insert_);
    batch.set_execution_profile("dc_aware");
    result = session_.execute(batch);
    ASSERT_EQ(CASS_OK, result.error_code());
    ASSERT_STREQ(expected_ip_address.str().c_str(), result.host().c_str());

    // Increment the expected round robin IP address
    expected_ip_address.str("");
    expected_ip_address << ccm_->get_ip_prefix() << expected_node + 1;

    // Execute a simple query with assigned profile
    insert_.set_execution_profile("dc_aware");
    result = session_.execute(insert_);
    ASSERT_EQ(CASS_OK, result.error_code());
    ASSERT_STREQ(expected_ip_address.str().c_str(), result.host().c_str());
  }
}

/**
 * Utilize the execution profile to override the statement load balancing policy
 *
 * This test will perform an insert query using the execution profile
 * 'blacklist_dc' and the default cluster profile configuration. Both profiles
 * will succeed and the blacklist statement execution will be validated to
 * ensure that only one data center is acted upon; 'dc2'.
 *
 * @jira_ticket CPP-492
 * @test_category execution_profiles
 * @since DSE 1.4.0
 * @expected_result Execution profile will execute using only nodes in 'dc2'
 */
CASSANDRA_INTEGRATION_TEST_F(DCExecutionProfileTest, BlacklistDCFiltering) {
  CHECK_FAILURE;

  // Execute statements over all the nodes in the cluster twice
  for (size_t i = 0; i < total_nodes_ * 2; ++i) {
    // Execute the same query with the cluster default profile
    insert_.set_execution_profile(""); // Reset the insert statement
    Result result = session_.execute(insert_);
    ASSERT_EQ(CASS_OK, result.error_code());

    // DC2 is the expected target (only contains one node)
    std::string expected_ip_address = ccm_->get_ip_prefix() + "3";

    // Execute a batched query with assigned profile
    Batch batch;
    batch.add(insert_);
    batch.set_execution_profile("blacklist_dc");
    result = session_.execute(batch);
    ASSERT_EQ(CASS_OK, result.error_code());
    ASSERT_STREQ(expected_ip_address.c_str(), result.host().c_str());

    // Execute a simple query with assigned profile
    insert_.set_execution_profile("blacklist_dc");
    result = session_.execute(insert_);
    ASSERT_EQ(CASS_OK, result.error_code());
    ASSERT_STREQ(expected_ip_address.c_str(), result.host().c_str());
  }
}

/**
 * Utilize the execution profile to override the statement load balancing policy
 *
 * This test will perform an insert query using the execution profile
 * 'whitelist_dc' and the default cluster profile configuration. Both profiles
 * will succeed and the whitelist statement execution will be validated to
 * ensure that only one data center is acted upon; 'dc2'.
 *
 * @jira_ticket CPP-492
 * @test_category execution_profiles
 * @since DSE 1.4.0
 * @expected_result Execution profile will execute using only nodes in 'dc2'
 */
CASSANDRA_INTEGRATION_TEST_F(DCExecutionProfileTest, WhitelistDCFiltering) {
  CHECK_FAILURE;

  // Execute statements over all the nodes in the cluster twice
  for (size_t i = 0; i < total_nodes_ * 2; ++i) {
    // Execute the same query with the cluster default profile
    insert_.set_execution_profile(""); // Reset the insert statement
    Result result = session_.execute(insert_);
    ASSERT_EQ(CASS_OK, result.error_code());

    // DC2 is the expected target (only contains one node)
    std::string expected_ip_address = ccm_->get_ip_prefix() + "3";

    // Execute a batched query with assigned profile
    Batch batch;
    batch.add(insert_);
    batch.set_execution_profile("whitelist_dc");
    result = session_.execute(batch);
    ASSERT_EQ(CASS_OK, result.error_code());
    ASSERT_STREQ(expected_ip_address.c_str(), result.host().c_str());

    // Execute a simple query with assigned profile
    insert_.set_execution_profile("whitelist_dc");
    result = session_.execute(insert_);
    ASSERT_EQ(CASS_OK, result.error_code());
    ASSERT_STREQ(expected_ip_address.c_str(), result.host().c_str());
  }
}
