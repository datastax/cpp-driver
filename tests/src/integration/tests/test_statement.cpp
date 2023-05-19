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

class StatementTests : public Integration {
public:
  StatementTests() { number_dc1_nodes_ = 2; }
};

/**
 * Set host on a statement and verify that query goes to the correct node.
 *
 * @jira_ticket CPP-597
 * @test_category configuration
 * @expected_result The local "rpc_address" matches the host set on the
 * statement.
 */
CASSANDRA_INTEGRATION_TEST_F(StatementTests, SetHost) {
  CHECK_FAILURE;

  for (int i = 1; i <= 2; ++i) {
    std::stringstream ip_address;
    ip_address << ccm_->get_ip_prefix() << i;
    Statement statement("SELECT rpc_address FROM system.local");
    statement.set_host(ip_address.str(), 9042);
    Result result = session_.execute(statement);
    Inet rpc_address = result.first_row().column_by_name<Inet>("rpc_address");
    EXPECT_EQ(ip_address.str(), rpc_address.str());
  }
}

/**
 * Set host on a statement using a `CassInet` and verify that query goes to the
 * correct node.
 *
 * @jira_ticket CPP-597
 * @test_category configuration
 * @expected_result The local "rpc_address" matches the host set on the
 * statement.
 */
CASSANDRA_INTEGRATION_TEST_F(StatementTests, SetHostInet) {
  CHECK_FAILURE;

  for (int i = 1; i <= 2; ++i) {
    std::stringstream ip_address;
    ip_address << ccm_->get_ip_prefix() << i;
    CassInet inet;
    ASSERT_EQ(cass_inet_from_string(ip_address.str().c_str(), &inet), CASS_OK);
    Statement statement("SELECT rpc_address FROM system.local");
    statement.set_host(&inet, 9042);
    Result result = session_.execute(statement);
    Inet rpc_address = result.first_row().column_by_name<Inet>("rpc_address");
    EXPECT_EQ(ip_address.str(), rpc_address.str());
  }
}

/**
 * Set node on a statement and verify that query goes to the correct node.
 *
 * @test_category configuration
 * @expected_result The local "rpc_address" matches a second query to the same
 * coordinator.
 */
CASSANDRA_INTEGRATION_TEST_F(StatementTests, SetNode) {
  CHECK_FAILURE;

  Statement statement("SELECT rpc_address FROM system.local");
  Result result1 = session_.execute(statement);
  Inet rpc_address1 = result1.first_row().column_by_name<Inet>("rpc_address");
  const CassNode* node = result1.coordinator();
  ASSERT_TRUE(node != NULL);

  statement.set_node(node);

  for (int i = 0; i < 4; ++i) {
    Result result2 = session_.execute(statement);
    Inet rpc_address2 = result1.first_row().column_by_name<Inet>("rpc_address");
    ASSERT_EQ(rpc_address1, rpc_address2);
  }
}

/**
 * Set a host on a statement that has an invalid port.
 *
 * @jira_ticket CPP-597
 * @test_category configuration
 * @expected_result The query should fail with a no host available error
 * because no such host exists.
 */
CASSANDRA_INTEGRATION_TEST_F(StatementTests, SetHostWithInvalidPort) {
  CHECK_FAILURE;

  Statement statement("SELECT rpc_address FROM system.local");
  statement.set_host("127.0.0.1", 8888); // Invalid port
  Result result = session_.execute(statement, false);
  EXPECT_EQ(result.error_code(), CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
}

/**
 * Set a host on a statement for host that is down.
 *
 * @jira_ticket CPP-597
 * @test_category configuration
 * @expected_result The query should fail with a no host available error
 * because the host is down.
 */
CASSANDRA_INTEGRATION_TEST_F(StatementTests, SetHostWhereHostIsDown) {
  CHECK_FAILURE;

  stop_node(1);

  Statement statement("SELECT rpc_address FROM system.local");
  statement.set_host("127.0.0.1", 9042);
  Result result = session_.execute(statement, false);
  EXPECT_EQ(result.error_code(), CASS_ERROR_LIB_NO_HOSTS_AVAILABLE);
}

class StatementNoClusterTests : public StatementTests {
public:
  StatementNoClusterTests() { is_ccm_requested_ = false; }
};

/**
 * Set a host on a statement using valid host strings.
 *
 * @jira_ticket CPP-597
 * @test_category configuration
 * @expected_result Success
 */
CASSANDRA_INTEGRATION_TEST_F(StatementNoClusterTests, SetHostWithValidHostString) {
  Statement statement("");
  EXPECT_EQ(cass_statement_set_host(statement.get(), "127.0.0.1", 9042), CASS_OK);
  EXPECT_EQ(cass_statement_set_host(statement.get(), "::1", 9042), CASS_OK);
  EXPECT_EQ(
      cass_statement_set_host(statement.get(), "2001:0db8:85a3:0000:0000:8a2e:0370:7334", 9042),
      CASS_OK);
}

/**
 * Set a host on a statement using invalid host strings.
 *
 * @jira_ticket CPP-597
 * @test_category configuration
 * @expected_result Failure with the bad parameters error.
 */
CASSANDRA_INTEGRATION_TEST_F(StatementNoClusterTests, SetHostWithInvalidHostString) {
  Statement statement("");
  EXPECT_EQ(cass_statement_set_host(statement.get(), "notvalid", 9042), CASS_ERROR_LIB_BAD_PARAMS);
  EXPECT_EQ(cass_statement_set_host(statement.get(), "", 9042), CASS_ERROR_LIB_BAD_PARAMS);
  EXPECT_EQ(cass_statement_set_host(statement.get(), NULL, 9042), CASS_ERROR_LIB_BAD_PARAMS);
}

/**
 * Set a host on a statement using valid `CassInet` types.
 *
 * @jira_ticket CPP-597
 * @test_category configuration
 * @expected_result Success
 */
CASSANDRA_INTEGRATION_TEST_F(StatementNoClusterTests, SetHostWithValidHostInet) {
  Statement statement("");
  CassInet valid;
  ASSERT_EQ(cass_inet_from_string("127.0.0.1", &valid), CASS_OK);
  EXPECT_EQ(valid.address_length, 4);
  EXPECT_EQ(cass_statement_set_host_inet(statement.get(), &valid, 9042), CASS_OK);
  ASSERT_EQ(cass_inet_from_string("::1", &valid), CASS_OK);
  EXPECT_EQ(valid.address_length, 16);
  EXPECT_EQ(cass_statement_set_host_inet(statement.get(), &valid, 9042), CASS_OK);
  ASSERT_EQ(cass_inet_from_string("2001:0db8:85a3:0000:0000:8a2e:0370:7334", &valid), CASS_OK);
  EXPECT_EQ(valid.address_length, 16);
  EXPECT_EQ(cass_statement_set_host_inet(statement.get(), &valid, 9042), CASS_OK);
}

/**
 * Set a host on a statement using invalid `CassInet` types.
 *
 * @jira_ticket CPP-597
 * @test_category configuration
 * @expected_result Failure with the bad parameters error.
 */
CASSANDRA_INTEGRATION_TEST_F(StatementNoClusterTests, SetHostWithInvalidHostInet) {
  Statement statement("");
  CassInet invalid;
  invalid.address_length = 3; // Only 4 or 16 is valid (IPv4 and IPv6)
  EXPECT_EQ(cass_statement_set_host_inet(statement.get(), &invalid, 9042),
            CASS_ERROR_LIB_BAD_PARAMS);
}
