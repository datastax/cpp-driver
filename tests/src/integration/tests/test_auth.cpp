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

#define CASS_LOWEST_SUPPORTED_PROTOCOL_VERSION CASS_PROTOCOL_VERSION_V3
#define CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION CASS_PROTOCOL_VERSION_V4

/**
 * Authentication integration tests
 */
class AuthenticationTests : public Integration {
public:
  void SetUp() {
    // Call the parent setup function (override startup and session connection)
    is_ccm_start_requested_ = false;
    is_password_authenticator_ = true;
    is_session_requested_ = false;
    Integration::SetUp();

    // Configure and start the CCM cluster for plain text authentication usage
    ccm_->update_cluster_configuration("authenticator", "PasswordAuthenticator");
    ccm_->start_cluster("-Dcassandra.superuser_setup_delay_ms=0");
    cluster_ = default_cluster();
  }

protected:
  /**
   * Establish a connection with the given protocol version and credentials
   *
   * @param protocol_version Protocol version to use
   * @param username Username credentials
   * @param password Password credentials
   * @return Session instance
   */
  Session connect_using_credentials(int protocol_version, const char* username,
                                    const char* password) {
    // Establish a connection using the protocol version
    cluster_.with_protocol_version(protocol_version).with_credentials(username, password);
    return cluster_.connect("", false);
  }

  /**
   * Authenticator callback used in AuthenticatorSetErrorNullError test to
   * assign a NULL error for CPP-368 validation
   *
   * @param authenticator Driver authenticator
   * @oaram data Data associated with the callback
   */
  static void handle_authenticator_initial(CassAuthenticator* authuenticator, void* data) {
    cass_authenticator_set_error(authuenticator, NULL);
  }
};

/**
 * Perform plain text authentication session connections for all protocol
 * versions
 *
 * This test will perform a plain text authentication using each protocol
 * version and establish a session connection against a single node cluster.
 *
 * @test_category authentication
 * @since core:1.0.0
 * @expected_result Session will be established for all protocol versions
 */
CASSANDRA_INTEGRATION_TEST_F(AuthenticationTests, ProtocolVersions) {
  CHECK_FAILURE;

  // Iterate over all known/supported protocol versions
  for (int i = CASS_LOWEST_SUPPORTED_PROTOCOL_VERSION; i <= CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION;
       ++i) {
    // Establish a connection using the protocol version
    Session session = connect_using_credentials(i, "cassandra", "cassandra");
    ASSERT_EQ(CASS_OK, session.connect_error_code()) << session.connect_error_description();

    // Execute a query against the schema keyspaces table
    Result result = session.execute("SELECT * FROM " + system_schema_keyspaces_);
    ASSERT_EQ(CASS_OK, result.error_code());
    ASSERT_GT(result.row_count(), 0u);
  }
}

/**
 * Perform plain text authentication session connections for all protocol
 * versions using invalid/empty credentials
 *
 * This test will perform a plain text authentication using each protocol
 * version with invalid empty credentials and fail to establish a session
 * connection against a single node cluster.
 *
 * @test_category authentication
 * @since core:1.0.0
 * @expected_result Session will not be established for all protocol versions
 */
CASSANDRA_INTEGRATION_TEST_F(AuthenticationTests, InvalidEmptyCredentials) {
  CHECK_FAILURE;

  logger_.add_critera("Key may not be empty");
  logger_.add_critera("Password must not be null");

  // CPP-968
  //
  // Ordering actually changes between Cassandra 3.0.x and 3.11.x.  In 3.0.x we check for an empty
  // username first (and thus return this error) while in 3.11.x we check for an empty password first
  // (and thus return the "password must not be null" error above).
  logger_.add_critera("Authentication ID must not be null");

  // Iterate over all known/supported protocol versions
  for (int i = CASS_LOWEST_SUPPORTED_PROTOCOL_VERSION; i <= CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION;
       ++i) {
    /*
     * This is a case that could be guarded in the API entry point, or error out
     * in connection. However, auth is subject to major changes and this is just
     * a simple form. This test serves to characterize what is there presently.
     */
    Session session = connect_using_credentials(i, "", "");
    ASSERT_EQ(session.connect_error_code(), CASS_ERROR_SERVER_BAD_CREDENTIALS);
    ASSERT_GT(logger_.count(), 0u);
    logger_.reset_count();
  }
}

/**
 * Perform plain text authentication session connections for all protocol
 * versions using NULL username credentials
 *
 * This test will perform a plain text authentication using each protocol
 * version with NULL username credentials and fail to establish a session
 * connection against a single node cluster.
 *
 * @test_category authentication
 * @since core:1.0.0
 * @expected_result Session will not be established for all protocol versions
 */
CASSANDRA_INTEGRATION_TEST_F(AuthenticationTests, InvalidNullUsernameCredentials) {
  CHECK_FAILURE;

  logger_.add_critera("Key may not be empty");
  logger_.add_critera("Authentication ID must not be null");

  // Iterate over all known/supported protocol versions
  for (int i = CASS_LOWEST_SUPPORTED_PROTOCOL_VERSION; i <= CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION;
       ++i) {
    /*
     * This is a case that could be guarded in the API entry point, or error out
     * in connection. However, auth is subject to major changes and this is just
     * a simple form. This test serves to characterize what is there presently.
     */
    Session session = connect_using_credentials(i, NULL, "pass");
    ASSERT_EQ(session.connect_error_code(), CASS_ERROR_SERVER_BAD_CREDENTIALS);
    ASSERT_GT(logger_.count(), 0u);
    logger_.reset_count();
  }
}

/**
 * Perform plain text authentication session connections for all protocol
 * versions using NULL password credentials
 *
 * This test will perform a plain text authentication using each protocol
 * version with NULL password credentials and fail to establish a session
 * connection against a single node cluster.
 *
 * @test_category authentication
 * @since core:1.0.0
 * @expected_result Session will not be established for all protocol versions
 */
CASSANDRA_INTEGRATION_TEST_F(AuthenticationTests, InvalidNullPasswordCredentials) {
  CHECK_FAILURE;

  logger_.add_critera("and/or password are incorrect");
  logger_.add_critera("Password must not be null");

  // Iterate over all known/supported protocol versions
  for (int i = CASS_LOWEST_SUPPORTED_PROTOCOL_VERSION; i <= CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION;
       ++i) {
    /*
     * This is a case that could be guarded in the API entry point, or error out
     * in connection. However, auth is subject to major changes and this is just
     * a simple form. This test serves to characterize what is there presently.
     */
    Session session = connect_using_credentials(i, "user", NULL);
    ASSERT_EQ(session.connect_error_code(), CASS_ERROR_SERVER_BAD_CREDENTIALS);
    ASSERT_GT(logger_.count(), 0u);
    logger_.reset_count();
  }
}

/**
 * Perform plain text authentication session connections for all protocol
 * versions using "bad" credentials
 *
 * This test will perform a plain text authentication using each protocol
 * version with "bad" credentials and fail to establish a session connection
 * against a single node cluster.
 *
 * @test_category authentication
 * @since core:1.0.0
 * @expected_result Session will not be established for all protocol versions
 */
CASSANDRA_INTEGRATION_TEST_F(AuthenticationTests, BadCredentials) {
  CHECK_FAILURE;

  // Add the proper logging criteria (based on server version)
  CCM::CassVersion cass_version = this->server_version_;
  if (!Options::is_cassandra()) {
    cass_version = static_cast<CCM::DseVersion>(cass_version).get_cass_version();
  }
  if (cass_version >= "3.10") {
    logger_.add_critera("Provided username invalid and/or password are incorrect");
  } else {
    logger_.add_critera("Username and/or password are incorrect");
  }

  // Iterate over all known/supported protocol versions
  for (int i = CASS_LOWEST_SUPPORTED_PROTOCOL_VERSION; i <= CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION;
       ++i) {
    /*
     * This is a case that could be guarded in the API entry point, or error out
     * in connection. However, auth is subject to major changes and this is just
     * a simple form. This test serves to characterize what is there presently.
     */
    Session session = connect_using_credentials(i, "invalid", "invalid");
    ASSERT_EQ(session.connect_error_code(), CASS_ERROR_SERVER_BAD_CREDENTIALS);
    ASSERT_GE(logger_.count(), 1u);
    logger_.reset_count();
  }
}

/**
 * Perform plain text authentication session connections using custom
 * authenticator callback
 *
 * This test will perform a plain text authentication using the custom
 * authenticator callback, assigning a NULL error,  and fail to establish a
 * session connection against a single node cluster.
 *
 * @jira_ticket: CPP-368
 * @test_category authentication
 * @since core:1.3.0
 * @expected_result Session will not be established
 */
CASSANDRA_INTEGRATION_TEST_F(AuthenticationTests, AuthenticatorSetErrorNull) {
  CHECK_FAILURE;

  // Add the proper logging criteria (based on server version)
  CCM::CassVersion cass_version = this->server_version_;
  if (!Options::is_cassandra()) {
    cass_version = static_cast<CCM::DseVersion>(cass_version).get_cass_version();
  }
  if (cass_version >= "3.10") {
    logger_.add_critera("Provided username invalid and/or password are incorrect");
  } else {
    logger_.add_critera("Username and/or password are incorrect");
  }

  // Attempt to establish session connection using authentication callback
  CassAuthenticatorCallbacks authentication_callbacks = {
    AuthenticationTests::handle_authenticator_initial, NULL, NULL, NULL
  };
  cluster_.with_authenticator_callbacks(&authentication_callbacks, NULL, NULL);
  Session session =
      connect_using_credentials(CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION, "invalid", "invalid");
  ASSERT_EQ(session.connect_error_code(), CASS_ERROR_SERVER_BAD_CREDENTIALS);
  ASSERT_GE(logger_.count(), 1u);
}
