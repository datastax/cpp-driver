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

#include "dse_integration.hpp"
#include "embedded_ads.hpp"
#include "options.hpp"

#include <utility>

// TODO: Update test to work with remote deployments
#ifdef _WIN32
#define CHECK_FOR_SKIPPED_TEST SKIP_TEST("Test cannot currently run on Windows");
#elif defined(CASS_USE_LIBSSH2)
#define CHECK_FOR_SKIPPED_TEST                                      \
  if (Options::deployment_type() == CCM::DeploymentType::REMOTE) {  \
    SKIP_TEST("Test cannot currently run using remote deployment"); \
  }
#else
#define CHECK_FOR_SKIPPED_TEST ((void)0)
#endif

#define ADS_WAIT_ATTEMPTS 500
#define DEFAULT_KEY "DataStax Enterprise"
#define DEFAULT_VALUE "DSE C/C++ Driver"
#define SELECT_ALL_ALICETABLE "SELECT key, value FROM aliceks.alicetable"

/**
 * Proxy authentication integration tests
 */
class ProxyAuthenticationTest : public DseIntegration {
public:
  static void SetUpTestCase() {
    CHECK_OPTIONS_VERSION(5.1.0);
    CHECK_FOR_SKIPPED_TEST;

    try {
      ads_ = new EmbeddedADS();
      ads_->start_process();
      TEST_LOG("Waiting for Initialization of ADS");
      int count = 0;
      while (!ads_->is_initialized() && count++ < ADS_WAIT_ATTEMPTS) {
        msleep(100);
      }
      if (ads_->is_initialized()) {
        TEST_LOG("ADS is Initialized and Ready");
        is_ads_available_ = true;
      } else {
        TEST_LOG_ERROR("ADS was not Initialized");
      }
    } catch (test::Exception& e) {
      TEST_LOG_ERROR(e.what());
    }
  }

  static void TearDownTestCase() {
    if (is_ads_available_) {
      ads_->terminate_process();
    }

    // Cluster configuration modified (remove cluster)
    if (is_ccm_configured_) {
      Options::ccm()->remove_cluster();
    }
  }

  void SetUp() {
    CHECK_VERSION(5.1.0);
    CHECK_FOR_SKIPPED_TEST;

    // TODO: Update test to work with remote deployments
    // Ensure test can run for current configuration
#ifdef _WIN32
    return;
#endif
#ifdef CASS_USE_LIBSSH2
    if (Options::deployment_type() == CCM::DeploymentType::REMOTE) {
      return;
    }
#endif
    CHECK_CONTINUE(ads_->is_initialized(), "Correct missing components for proper ADS launching");

    // Call the parent setup function (override startup and session connection)
    is_ccm_start_requested_ = false;
    is_session_requested_ = false;
    DseIntegration::SetUp();
    configure_dse_cluster();
  }

  void TearDown() {
    if (is_ads_available_) {
      // Remove all the cached authentication tickets
      ads_->destroy_tickets();
    }
  }

protected:
  /**
   * ADS instance
   */
  static SharedPtr<EmbeddedADS> ads_;
  /**
   * Flag to determine if the ADS is available
   */
  static bool is_ads_available_;
  /**
   * Flag to determine if the CCM cluster has already been initialized for
   * proxy authentication
   */
  static bool is_ccm_configured_;

  /**
   * Configure the DSE cluster for use with the ADS and proxy authentication
   */
  void configure_dse_cluster() {
    // Determine if proxy authentication has already been configured
    if (!is_ccm_configured_) {
      // Ensure the cluster is stopped
      ccm_->stop_cluster();

      // Configure the default proxy authentication options
      std::vector<std::string> update_configuration;
      std::vector<std::string> update_dse_configuration;
      update_configuration.push_back("authorizer:com.datastax.bdp.cassandra.auth.DseAuthorizer");
      update_configuration.push_back(
          "authenticator:com.datastax.bdp.cassandra.auth.DseAuthenticator");
      update_dse_configuration.push_back("authorization_options.enabled:true");
      update_dse_configuration.push_back("audit_logging_options.enabled:true");
      update_dse_configuration.push_back("kerberos_options.service_principal:" +
                                         std::string(DSE_SERVICE_PRINCIPAL));
      update_dse_configuration.push_back("kerberos_options.http_principal:" +
                                         std::string(DSE_SERVICE_PRINCIPAL));
      update_dse_configuration.push_back("kerberos_options.keytab:" + ads_->get_dse_keytab_file());
      update_dse_configuration.push_back("kerberos_options.qop:auth");
      std::string update_dse_configuration_authentication_options_yaml =
          "authentication_options:\n"
          "  enabled: true\n"
          "  default_scheme: kerberos\n"
          "  other_schemes:\n"
          "    - internal";

      // Apply the configuration options
      ccm_->update_cluster_configuration(update_configuration);
      ccm_->update_cluster_configuration(update_dse_configuration, true);
      ccm_->update_cluster_configuration_yaml(update_dse_configuration_authentication_options_yaml,
                                              true);

      // Start the cluster
      std::vector<std::string> jvm_arguments;
      jvm_arguments.push_back("-Dcassandra.superuser_setup_delay_ms=0");
      jvm_arguments.push_back("-Djava.security.krb5.conf=" + ads_->get_configuration_file());
      ccm_->start_cluster(jvm_arguments);
      msleep(5000); // DSE may not be 100% available (even though port is available)

      // Create the default connection to the cluster
      dse::Cluster cluster = default_cluster();
      cluster.with_plaintext_authenticator("cassandra", "cassandra");
      DseIntegration::connect(cluster);

      // Setup the keyspace, table, and roles on the cluster
      /*
       * Role information:
       * Ben and Bob are allowed to login as Alice, but not execute as Alice.
       * Charlie and Steve are allowed to execute as Alice, but not login as
       * Alice.
       */
      dse_session_.execute(
          "CREATE ROLE IF NOT EXISTS alice WITH PASSWORD = 'alice' AND LOGIN = FALSE");
      dse_session_.execute("CREATE ROLE IF NOT EXISTS ben WITH PASSWORD = 'ben' AND LOGIN = TRUE");
      dse_session_.execute("CREATE ROLE IF NOT EXISTS 'bob@DATASTAX.COM' WITH LOGIN = TRUE");
      dse_session_.execute("CREATE ROLE IF NOT EXISTS 'charlie@DATASTAX.COM' WITH PASSWORD = "
                           "'charlie' AND LOGIN = TRUE");
      dse_session_.execute(
          "CREATE ROLE IF NOT EXISTS steve WITH PASSWORD = 'steve' AND LOGIN = TRUE");
      dse_session_.execute("CREATE KEYSPACE IF NOT EXISTS aliceks WITH REPLICATION = {'class': "
                           "'SimpleStrategy', 'replication_factor': '1'}");
      dse_session_.execute(
          "CREATE TABLE IF NOT EXISTS aliceks.alicetable (key text PRIMARY KEY, value text)");
      dse_session_.execute("GRANT ALL ON KEYSPACE aliceks TO alice");
      dse_session_.execute("GRANT EXECUTE ON ALL AUTHENTICATION SCHEMES TO 'ben'");
      dse_session_.execute("GRANT EXECUTE ON ALL AUTHENTICATION SCHEMES TO 'bob@DATASTAX.COM'");
      dse_session_.execute("GRANT EXECUTE ON ALL AUTHENTICATION SCHEMES TO 'steve'");
      dse_session_.execute("GRANT EXECUTE ON ALL AUTHENTICATION SCHEMES TO 'charlie@DATASTAX.COM'");
      dse_session_.execute("GRANT PROXY.LOGIN ON ROLE 'alice' TO 'ben'");
      dse_session_.execute("GRANT PROXY.LOGIN ON ROLE 'alice' TO 'bob@DATASTAX.COM'");
      dse_session_.execute("GRANT PROXY.EXECUTE ON ROLE 'alice' TO 'steve'");
      dse_session_.execute("GRANT PROXY.EXECUTE ON ROLE 'alice' TO 'charlie@DATASTAX.COM'");

      // Insert the first row for most tests to verify query
      std::stringstream insert_query;
      insert_query << "INSERT INTO aliceks.alicetable (key, value) VALUES ('" << DEFAULT_KEY
                   << "', '" << DEFAULT_VALUE << "')";
      dse_session_.execute(insert_query.str());

      // Indicate cluster has been configured for proxy authentication
      is_ccm_configured_ = true;
    }
  }

  /**
   * Query the `alicetable`; if `as` parameter is provided query the table as
   * that user. Assert/Validate the key/value pairs after query is executed.
   *
   * @param session Session to execute query
   * @param as Name to execute the query as; default will execute the query
   *           normally
   * @throws test::CassException if query did not execute properly
   */
  void query(dse::Session session, const std::string& as = "") {
    // Execute the query and validate the results
    test::driver::Result result;
    if (!as.empty()) {
      result = session.execute_as(SELECT_ALL_ALICETABLE, as, CASS_CONSISTENCY_ONE, false, false);
    } else {
      result = session.execute(SELECT_ALL_ALICETABLE, CASS_CONSISTENCY_ONE, false, false);
    }

    // Determine if the results should be validated or exception thrown
    if (result.error_code() == CASS_OK) {
      assert_result(result);
    } else {
      throw test::CassException(result.error_message(), result.error_code());
    }
  }

  /**
   * Query the `alicetable` using batch inserts; if `as` parameter is provided
   * query the table as that user. Assert/Validate the key/value pairs after
   * query is executed.
   *
   * @param session Session to execute query
   * @param as Name to execute the query as; default will execute the query
   *           normally
   * @throws test::CassException if query did not execute properly
   */
  void batch_query(dse::Session session, const std::string& as = "") {
    // Create a bunch of batch inserts to execute
    test::driver::Batch batch;
    for (int i = 0; i < 10; ++i) {
      std::stringstream query;
      query << "INSERT INTO aliceks.alicetable (key, value) VALUES ('" << i << "', '" << i
            << "00')";
      batch.add(test::driver::Statement(query.str()));
    }

    // Execute the batch inserts
    test::driver::Result result;
    if (!as.empty()) {
      result = session.execute_as(batch, as, false);
    } else {
      result = session.execute(batch, false);
    }

    // Determine if the results should be validated or exception thrown
    if (result.error_code() == CASS_OK) {
      // Execute the select query and validate key/value pairs
      query(session, as);
    } else {
      throw test::CassException(result.error_message(), result.error_code());
    }
  }

  /**
   * Assert the key/value pairs in the result
   *
   * @param result Result to retrieve key/value pair from
   */
  void assert_result(test::driver::Result result) {
    // Gather the results for sorting
    std::vector<std::string> results;
    test::driver::Rows rows = result.rows();
    for (size_t i = 0; i < rows.row_count(); ++i) {
      // Retrieve the key/value pair
      Row row = rows.next();
      std::string key = row.next().as<driver::Varchar>().value();
      std::string value = row.next().as<driver::Varchar>().value();
      results.push_back(key + "," + value);
    }

    // Sort and validate the results
    std::sort(results.begin(), results.end());
    for (size_t i = 0; i < rows.row_count(); ++i) {
      // Validate the key/value pair
      std::vector<std::string> key_value_pair = test::Utils::explode(results.at(i), ',');
      if (i == rows.row_count() - 1) {
        ASSERT_EQ(DEFAULT_KEY, key_value_pair.at(0));
        ASSERT_EQ(DEFAULT_VALUE, key_value_pair.at(1));
      } else {
        // Generate a string for the expected key and validate
        std::stringstream expected_key;
        expected_key << i;
        ASSERT_EQ(expected_key.str(), key_value_pair.at(0));

        // Generate a string for the expected value and validate
        std::stringstream expected_value;
        expected_value << i << "00";
        ASSERT_EQ(expected_value.str(), key_value_pair.at(1));
      }
    }
  }
};

// Initialize static variables
SharedPtr<EmbeddedADS> ProxyAuthenticationTest::ads_;
bool ProxyAuthenticationTest::is_ads_available_ = false;
bool ProxyAuthenticationTest::is_ccm_configured_ = false;

/**
 * Perform connection to DSE using plain text proxy authentication
 *
 * This test will perform a connection to a DSE server using plain text proxy
 * authentication where the user "alice" is using the credentials of "ben" with
 * the assumption that "ben" has authorized "alice" via 'PROXY.LOGIN'.
 *
 * @jira_ticket CPP-426
 * @test_category dse:auth
 * @since 1.2.0
 * @expected_result Successful connection and query execution
 */
DSE_INTEGRATION_TEST_F(ProxyAuthenticationTest, PlainTextProxyAuthorizedUserLoginAs) {
  CHECK_VERSION(5.1.0);
  CHECK_FOR_SKIPPED_TEST;
  CHECK_FAILURE;

  // Build the cluster configuration and establish the session connection
  dse::Cluster cluster = default_cluster();
  dse::Session session =
      cluster.with_plaintext_authenticator_proxy("ben", "ben", "alice").connect();

  // Execute and validate the query
  query(session);
}

/**
 * Perform connection to DSE using plain text authentication and execute
 * a query as someone else
 *
 * This test will perform a connection to a DSE server using plain text
 * authentication where the user "alice" is executing a query using the
 * credentials of "steve" with the assumption that "steve" has authorized
 * "alice" via 'PROXY.EXECUTE'.
 *
 * @jira_ticket CPP-426
 * @test_category dse:auth
 * @since 1.2.0
 * @expected_result Successful connection and query execution
 */
DSE_INTEGRATION_TEST_F(ProxyAuthenticationTest, PlainTextAuthorizedUserLoginExecuteAs) {
  CHECK_VERSION(5.1.0);
  CHECK_FOR_SKIPPED_TEST;
  CHECK_FAILURE;

  // Build the cluster configuration and establish the session connection
  dse::Cluster cluster = default_cluster();
  dse::Session session = cluster.with_plaintext_authenticator("steve", "steve").connect();

  // Execute and validate the query as "alice"
  query(session, "alice");
}

/**
 * Perform connection to DSE using plain text authentication and execute
 * a batch query as someone else
 *
 * This test will perform a connection to a DSE server using plain text
 * authentication where the user "alice" is executing a batch query using the
 * credentials of "steve" with the assumption that "steve" has authorized
 * "alice" via 'PROXY.EXECUTE'.
 *
 * @jira_ticket CPP-426
 * @test_category dse:auth
 * @since 1.2.0
 * @expected_result Successful connection and query execution
 */
DSE_INTEGRATION_TEST_F(ProxyAuthenticationTest, PlainTextAuthorizedUserLoginExecuteBatchAs) {
  CHECK_VERSION(5.1.0);
  CHECK_FOR_SKIPPED_TEST;
  CHECK_FAILURE;

  // Build the cluster configuration and establish the session connection
  dse::Cluster cluster = default_cluster();
  dse::Session session = cluster.with_plaintext_authenticator("steve", "steve").connect();

  // Execute and validate the batch query as "alice"
  batch_query(session, "alice");
}

/**
 * Perform a connection to DSE using plain text proxy authentication
 * (e.g. authorization ID) and execute a query; query should fail
 *
 * This test will perform a connection to a DSE server using plain text proxy
 * authentication where the user "alice" is using the unauthorized credentials
 * of "steve" with the assumption that "steve" has not authorized "alice" via
 * 'PROXY.LOGIN'.
 *
 * @jira_ticket CPP-426
 * @test_category dse:auth
 * @since 1.2.0
 * @expected_result Connection is successful; however queries are unauthorized
 */
DSE_INTEGRATION_TEST_F(ProxyAuthenticationTest, PlainTextProxyUnauthorizedUserLoginAs) {
  CHECK_VERSION(5.1.0);
  CHECK_FOR_SKIPPED_TEST;
  CHECK_FAILURE;

  // Build the cluster configuration and attempt the session connection
  bool is_session_failure = false;
  try {
    dse::Cluster cluster = default_cluster();
    dse::Session session =
        cluster.with_plaintext_authenticator_proxy("steve", "steve", "alice").connect();
    query(session);
  } catch (test::CassException& ce) {
    TEST_LOG(ce.what());
    ASSERT_TRUE(CASS_ERROR_SERVER_UNAUTHORIZED == ce.error_code() ||
                CASS_ERROR_SERVER_BAD_CREDENTIALS == ce.error_code())
        << "Error code is not 'Unauthorized|Bad credentials'";
    is_session_failure = true;
  }
  ASSERT_EQ(true, is_session_failure) << "Session connection established";
}

/**
 * Perform a connection to DSE using plain text authentication and execute a
 * query as someone else that should fail
 *
 * This test will perform a connection to a DSE server using plain text
 * authentication where the user "alice" is unauthorized to execute queries
 * using the credential of "ben" with the assumption that "ben" has not
 * authorized "alice" via 'PROXY.EXECUTE'.
 *
 * @jira_ticket CPP-426
 * @test_category dse:auth
 * @since 1.2.0
 * @expected_result Connection is successful and query execution fails
 */
DSE_INTEGRATION_TEST_F(ProxyAuthenticationTest, PlainTextAuthorizedUserLoginUnauthorizedExecuteAs) {
  CHECK_VERSION(5.1.0);
  CHECK_FOR_SKIPPED_TEST;
  CHECK_FAILURE;

  // Build the cluster configuration and establish the session connection
  dse::Cluster cluster = default_cluster();
  dse::Session session = cluster.with_plaintext_authenticator("ben", "ben").connect();

  // Execute and validate the query as "alice" fails
  bool is_query_failure = false;
  try {
    query(session, "alice");
  } catch (test::CassException& ce) {
    TEST_LOG(ce.what());
    ASSERT_EQ(CASS_ERROR_SERVER_UNAUTHORIZED, ce.error_code())
        << "Error code is not 'Unauthorized'";
    is_query_failure = true;
  }
  ASSERT_EQ(true, is_query_failure) << "Query completed successfully";
}

/**
 * Perform a connection to DSE using plain text authentication and execute a
 * query as someone else that should fail
 *
 * This test will perform a connection to a DSE server using plain text
 * authentication where the user "alice" is unauthorized to execute batch
 * queries using the credential of "ben" with the assumption that "ben" has not
 * authorized "alice" via 'PROXY.EXECUTE'.
 *
 * @jira_ticket CPP-426
 * @test_category dse:auth
 * @since 1.2.0
 * @expected_result Connection is successful and query execution fails
 */
DSE_INTEGRATION_TEST_F(ProxyAuthenticationTest,
                       PlainTextAuthorizedUserLoginUnauthorizedExecuteBatchAs) {
  CHECK_VERSION(5.1.0);
  CHECK_FOR_SKIPPED_TEST;
  CHECK_FAILURE;

  // Build the cluster configuration and establish the session connection
  dse::Cluster cluster = default_cluster();
  dse::Session session = cluster.with_plaintext_authenticator("ben", "ben").connect();

  // Execute and validate the query as "alice" fails
  bool is_query_failure = false;
  try {
    batch_query(session, "alice");
  } catch (test::CassException& ce) {
    TEST_LOG(ce.what());
    ASSERT_EQ(CASS_ERROR_SERVER_UNAUTHORIZED, ce.error_code())
        << "Error code is not 'Unauthorized'";
    is_query_failure = true;
  }
  ASSERT_EQ(true, is_query_failure) << "Batch query completed successfully";
}

/**
 * Perform connection to DSE using Kerberos/GSSAPI proxy authentication
 *
 * This test will perform a connection to a DSE server using Kerberos/GSSAPI
 * proxy authentication where the user "alice" is using the credentials of "bob"
 * with the assumption that "bob" has authorized "alice" via 'PROXY.LOGIN'.
 *
 * @jira_ticket CPP-426
 * @test_category dse:auth
 * @since 1.2.0
 * @expected_result Successful connection and query execution
 */
DSE_INTEGRATION_TEST_F(ProxyAuthenticationTest, KerberosProxyAuthorizedUserLoginAs) {
  CHECK_VERSION(5.1.0);
  CHECK_FOR_SKIPPED_TEST;
  CHECK_FAILURE;

  // Acquire a key for the Bob user
  ads_->acquire_ticket(BOB_PRINCIPAL, ads_->get_bob_keytab_file());

  // Build the cluster configuration and establish the session connection
  dse::Cluster cluster = default_cluster();
  dse::Session session =
      cluster.with_gssapi_authenticator_proxy("dse", BOB_PRINCIPAL, "alice").connect();

  // Execute and validate the query
  query(session);
}

/**
 * Perform connection to DSE using Kerberos/GSSAPI authentication and execute
 * a query as someone else
 *
 * This test will perform a connection to a DSE server using Kerberos/GSSAPI
 * authentication where the user "alice" is executing a query using the
 * credentials of "charlie" with the assumption that "charlie" has authorized
 * "alice" via 'PROXY.EXECUTE'.
 *
 * @jira_ticket CPP-426
 * @test_category dse:auth
 * @since 1.2.0
 * @expected_result Successful connection and query execution
 */
DSE_INTEGRATION_TEST_F(ProxyAuthenticationTest, KerberosAuthorizedUserLoginExecuteAs) {
  CHECK_VERSION(5.1.0);
  CHECK_FOR_SKIPPED_TEST;
  CHECK_FAILURE;

  // Acquire a key for the Bob user
  ads_->acquire_ticket(CHARLIE_PRINCIPAL, ads_->get_charlie_keytab_file());

  // Build the cluster configuration and establish the session connection
  dse::Cluster cluster = default_cluster();
  dse::Session session = cluster.with_gssapi_authenticator("dse", CHARLIE_PRINCIPAL).connect();

  // Execute and validate the query as "alice"
  query(session, "alice");
}

/**
 * Perform connection to DSE using Kerberos/GSSAPI authentication and execute
 * a batch query as someone else
 *
 * This test will perform a connection to a DSE server using Kerberos/GSSAPI
 * authentication where the user "alice" is executing a batch query using
 * the credentials of "charlie" with the assumption that "charlie" has
 * authorized "alice" via 'PROXY.EXECUTE'.
 *
 * @jira_ticket CPP-426
 * @test_category dse:auth
 * @since 1.2.0
 * @expected_result Successful connection and query execution
 */
DSE_INTEGRATION_TEST_F(ProxyAuthenticationTest, KerberosAuthorizedUserLoginExecuteBatchAs) {
  CHECK_VERSION(5.1.0);
  CHECK_FOR_SKIPPED_TEST;
  CHECK_FAILURE;

  // Acquire a key for the Bob user
  ads_->acquire_ticket(CHARLIE_PRINCIPAL, ads_->get_charlie_keytab_file());

  // Build the cluster configuration and establish the session connection
  dse::Cluster cluster = default_cluster();
  dse::Session session = cluster.with_gssapi_authenticator("dse", CHARLIE_PRINCIPAL).connect();

  // Execute and validate the batch query as "alice"
  batch_query(session, "alice");
}

/**
 * Perform a failing connection to DSE using Kerberos/GSSAPI proxy
 * authentication
 *
 * This test will perform a connection to a DSE server using Kerberos/GSSAPI
 * proxy authentication where the user "alice" is using the unauthorized
 * credentials of "charlie" with the assumption that "charlie" has not
 * authorized "alice" via 'PROXY.LOGIN'.
 *
 * @jira_ticket CPP-426
 * @test_category dse:auth
 * @since 1.2.0
 * @expected_result Connection is unsuccessful; Bad credentials
 */
DSE_INTEGRATION_TEST_F(ProxyAuthenticationTest, KerberosProxyBadCredentialsUserLoginAs) {
  CHECK_VERSION(5.1.0);
  CHECK_FOR_SKIPPED_TEST;
  CHECK_FAILURE;

  // Acquire a key for the Charlie user
  ads_->acquire_ticket(CHARLIE_PRINCIPAL, ads_->get_charlie_keytab_file());

  // Build the cluster configuration and attempt the session connection
  bool is_session_failure = false;
  try {
    dse::Cluster cluster = default_cluster();
    dse::Session session =
        cluster.with_gssapi_authenticator_proxy("dse", CHARLIE_PRINCIPAL, "alice").connect();
  } catch (Session::Exception& se) {
    TEST_LOG(se.what());
    ASSERT_EQ(CASS_ERROR_SERVER_BAD_CREDENTIALS, se.error_code())
        << "Error code is not 'Bad credentials'";
    is_session_failure = true;
  }
  ASSERT_EQ(true, is_session_failure) << "Session connection established";
}

/**
 * Perform connection to DSE using Kerberos/GSSAPI authentication and execute
 * a query as someone else that should fail
 *
 * This test will perform a connection to a DSE server using Kerberos/GSSAPI
 * authentication where the user "alice" is unauthorized to execute queries
 * using the credentials of "bob" with the assumption that "bob" has not
 * authorized "alice" via 'PROXY.EXECUTE'.
 *
 * @jira_ticket CPP-426
 * @test_category dse:auth
 * @since 1.2.0
 * @expected_result Connection is successful and query execution fails
 */
DSE_INTEGRATION_TEST_F(ProxyAuthenticationTest, KerberosAuthorizedUserLoginUnauthorizedExecuteAs) {
  CHECK_VERSION(5.1.0);
  CHECK_FOR_SKIPPED_TEST;
  CHECK_FAILURE;

  // Acquire a key for the Bob user
  ads_->acquire_ticket(BOB_PRINCIPAL, ads_->get_bob_keytab_file());

  // Build the cluster configuration and establish the session connection
  dse::Cluster cluster = default_cluster();
  dse::Session session = cluster.with_gssapi_authenticator("dse", BOB_PRINCIPAL).connect();

  // Execute and validate the query as "alice" fails
  bool is_query_failure = false;
  try {
    query(session, "alice");
  } catch (test::CassException& ce) {
    TEST_LOG(ce.what());
    ASSERT_EQ(CASS_ERROR_SERVER_UNAUTHORIZED, ce.error_code())
        << "Error code is not 'Unauthorized'";
    is_query_failure = true;
  }
  ASSERT_EQ(true, is_query_failure) << "Query completed successfully";
}

/**
 * Perform connection to DSE using Kerberos/GSSAPI authentication and execute
 * a batch query as someone else that should fail
 *
 * This test will perform a connection to a DSE server using Kerberos/GSSAPI
 * authentication where the user "alice" is unauthorized to execute batch
 * queries using the credentials of "bob" with the assumption that "bob" has not
 * authorized "alice" via 'PROXY.EXECUTE'.
 *
 * @jira_ticket CPP-426
 * @test_category dse:auth
 * @since 1.2.0
 * @expected_result Connection is successful and query execution fails
 */
DSE_INTEGRATION_TEST_F(ProxyAuthenticationTest,
                       KerberosAuthorizedUserLoginUnauthorizedExecuteBatchAs) {
  CHECK_VERSION(5.1.0);
  CHECK_FOR_SKIPPED_TEST;
  CHECK_FAILURE;

  // Acquire a key for the Bob user
  ads_->acquire_ticket(BOB_PRINCIPAL, ads_->get_bob_keytab_file());

  // Build the cluster configuration and establish the session connection
  dse::Cluster cluster = default_cluster();
  dse::Session session = cluster.with_gssapi_authenticator("dse", BOB_PRINCIPAL).connect();

  // Execute and validate the query as "alice" fails
  bool is_query_failure = false;
  try {
    batch_query(session, "alice");
  } catch (test::CassException& ce) {
    TEST_LOG(ce.what());
    ASSERT_EQ(CASS_ERROR_SERVER_UNAUTHORIZED, ce.error_code())
        << "Error code is not 'Unauthorized'";
    is_query_failure = true;
  }
  ASSERT_EQ(true, is_query_failure) << "Query completed successfully";
}
