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

#define CHECK_FOR_KERBEROS_HEIMDAL                                    \
  do {                                                                \
    if (EmbeddedADS::is_kerberos_client_heimdal()) {                  \
      SKIP_TEST("Heimdal implementation is not valid for this test"); \
    }                                                                 \
  } while (0);

// TODO: Update test to work with remote deployments
#ifdef _WIN32
#define CHECK_FOR_SKIPPED_TEST SKIP_TEST("Test cannot currently run on Windows");
#elif defined(CASS_USE_LIBSSH2)
#define CHECK_FOR_SKIPPED_TEST                                        \
  do {                                                                \
    if (Options::deployment_type() == CCM::DeploymentType::REMOTE) {  \
      SKIP_TEST("Test cannot currently run using remote deployment"); \
    }                                                                 \
  } while (0);
#else
#define CHECK_FOR_SKIPPED_TEST ((void)0)
#endif

/**
 * Authentication integration tests
 */
class AuthenticationTest : public DseIntegration {
public:
  static void SetUpTestCase() {
    CHECK_FOR_SKIPPED_TEST;

    try {
      ads_ = new EmbeddedADS();
      ads_->start_process();
      TEST_LOG("Waiting for Initialization of ADS");
      while (!ads_->is_initialized()) {
        msleep(100);
      }
      TEST_LOG("ADS is Initialized and Ready");
      is_ads_available_ = true;
    } catch (test::Exception& e) {
      TEST_LOG_ERROR(e.what());
    }
  }

  static void TearDownTestCase() {
    if (is_ads_available_) {
      ads_->terminate_process();
    }

    // Cluster configuration modified (remove cluster)
    Options::ccm()->remove_cluster();
  }

  void SetUp() {
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
  }

  void TearDown() {
    if (is_ads_available_) {
      // Remove all the cached authentication tickets
      ads_->destroy_tickets();
      ads_->clear_keytab();
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
   * Configure the DSE cluster for use with the ADS
   *
   * @param is_kerberos True if Kerberos configuration should be enabled; false
   *                    otherwise (default: true)
   */
  void configure_dse_cluster(bool is_kerberos = true) {
    // Ensure the cluster is stopped
    ccm_->stop_cluster();

    // Configure the default authentication options
    std::vector<std::string> update_configuration;
    std::vector<std::string> update_dse_configuration;
    if (server_version_ >= "5.0.0") {
      update_configuration.push_back(
          "authenticator:com.datastax.bdp.cassandra.auth.DseAuthenticator");
      update_dse_configuration.push_back("authentication_options.enabled:true");
    }

    // Determine if Kerberos functionality should be configured
    std::vector<std::string> jvm_arguments;
    if (is_kerberos && is_ads_available_) {
      // Configure the cluster for use with the ADS
      if (server_version_ >= "5.0.0") {
        update_dse_configuration.push_back("authentication_options.default_scheme:kerberos");
        update_dse_configuration.push_back("authentication_options.scheme_permissions:true");
        update_dse_configuration.push_back(
            "authentication_options.allow_digest_with_kerberos:true");
        update_dse_configuration.push_back("authentication_options.transitional_mode:disabled");
      } else {
        update_configuration.push_back(
            "authenticator:com.datastax.bdp.cassandra.auth.KerberosAuthenticator");
      }
      update_dse_configuration.push_back("kerberos_options.service_principal:" +
                                         std::string(DSE_SERVICE_PRINCIPAL));
      update_dse_configuration.push_back("kerberos_options.keytab:" + ads_->get_dse_keytab_file());
      update_dse_configuration.push_back("kerberos_options.qop:auth");

      jvm_arguments.push_back("-Dcassandra.superuser_setup_delay_ms=0");
      jvm_arguments.push_back("-Djava.security.krb5.conf=" + ads_->get_configuration_file());
    } else {
      if (server_version_ >= "5.0.0") {
        update_dse_configuration.push_back("authentication_options.default_scheme:internal");
        update_dse_configuration.push_back("authentication_options.plain_text_without_ssl:allow");
      }
    }
    ccm_->update_cluster_configuration(update_configuration);
    ccm_->update_cluster_configuration(update_dse_configuration, true);

    // Start the cluster
    ccm_->start_cluster(jvm_arguments);
    msleep(5000); // DSE may not be 100% available (even though port is available)
  }

  /**
   * Establish a connection to the server and query the system table using
   * Kerberos/GSSAPI authentication
   *
   * @param principal Principal identity
   * @throws Session::Exception If session could not be established
   */
  void connect_using_kerberos_and_query_system_table(const std::string& principal) {
    // Update the CCM configuration for use with the ADS
    configure_dse_cluster();

    // Build the cluster configuration and establish the session connection
    Cluster cluster = dse::Cluster::build()
                          .with_gssapi_authenticator("dse", principal)
                          .with_contact_points(contact_points_)
                          .with_schema_metadata(false);
    Session session = cluster.connect();

    // Execute a simple query to ensure authentication
    Result result = session.execute(SELECT_ALL_SYSTEM_LOCAL_CQL);
    ASSERT_GT(result.row_count(), 0u);
  }

  /**
   * Establish a connection to the server and query the system table using
   * DSE internal authentication
   *
   * @param username Username to authenticate
   * @param password Password for username
   * @throws Session::Exception If session could not be established
   */
  void connect_using_internal_authentication_and_query_system_table(const std::string& username,
                                                                    const std::string& password) {
    // Update the CCM configuration for use with internal authentication
    configure_dse_cluster(false);

    // Build the cluster configuration and establish the session connection
    Cluster cluster = dse::Cluster::build()
                          .with_plaintext_authenticator(username, password)
                          .with_contact_points(contact_points_)
                          .with_schema_metadata(false);
    Session session = cluster.connect();

    // Execute a simple query to ensure authentication
    session.execute(SELECT_ALL_SYSTEM_LOCAL_CQL);
  }
};

// Initialize static variables
SharedPtr<EmbeddedADS> AuthenticationTest::ads_;
bool AuthenticationTest::is_ads_available_ = false;

/**
 * Perform connection to DSE using Kerberos authentication
 *
 * This test will perform a connection to a DSE server using Kerberos/GSSAPI
 * authentication against a single node cluster and execute a simple query.
 *
 * @jira_ticket CPP-350
 * @test_category dse:auth
 * @since 1.0.0
 * @expected_result Successful connection and query execution
 */
DSE_INTEGRATION_TEST_F(AuthenticationTest, KerberosAuthentication) {
  CHECK_FOR_SKIPPED_TEST;
  CHECK_FAILURE;

  // Acquire a key for the Cassandra user, connect, and query the system table
  ads_->acquire_ticket(CASSANDRA_USER_PRINCIPAL, ads_->get_cassandra_keytab_file());
  connect_using_kerberos_and_query_system_table(CASSANDRA_USER_PRINCIPAL);
}

/**
 * Perform a failing connection to DSE using bad credentials
 *
 * This test will perform a connection to a DSE server using Kerberos/GSSAPI
 * authentication against a single node cluster where an attempt is made to
 * authenticate a user with bad credentials.
 *
 * @jira_ticket CPP-350
 * @test_category dse:auth
 * @since 1.0.0
 * @expected_result Connection is unsuccessful; Bad credentials
 */
DSE_INTEGRATION_TEST_F(AuthenticationTest, KerberosAuthenticationFailureBadCredentials) {
  CHECK_FOR_SKIPPED_TEST;
  CHECK_FAILURE;

  // Acquire a key for the unknown user
  ads_->acquire_ticket(UNKNOWN_PRINCIPAL, ads_->get_unknown_keytab_file());

  // Attempt to connect and ensure failed connection
  bool is_session_failure = false;
  try {
    connect_using_kerberos_and_query_system_table(UNKNOWN_PRINCIPAL);
  } catch (Session::Exception& se) {
    TEST_LOG(se.what());
    ASSERT_EQ(CASS_ERROR_SERVER_BAD_CREDENTIALS, se.error_code())
        << "Error code is not 'Bad credentials'";
    is_session_failure = true;
  }
  ASSERT_EQ(true, is_session_failure) << "Session connection established";
}

/**
 * Perform a failing connection to DSE without valid ticket credentials
 *
 * This test will perform a connection to a DSE server using Kerberos/GSSAPI
 * authentication against a single node cluster where an attempt is made to
 * authenticate a user without acquiring a ticket.
 *
 * @jira_ticket CPP-350
 * @test_category dse:auth
 * @since 1.0.0
 * @expected_result Connection is unsuccessful; Bad credentials
 */
DSE_INTEGRATION_TEST_F(AuthenticationTest, KerberosAuthenticationFailureNoTicket) {
  CHECK_FOR_SKIPPED_TEST;
  CHECK_FAILURE;

  // Attempt to connect and ensure failed connection
  bool is_session_failure = false;
  try {
    connect_using_kerberos_and_query_system_table(CASSANDRA_USER_PRINCIPAL);
  } catch (Session::Exception& se) {
    TEST_LOG(se.what());
    ASSERT_EQ(CASS_ERROR_SERVER_BAD_CREDENTIALS, se.error_code())
        << "Error code is not 'Bad credentials'";
    is_session_failure = true;
  }
  ASSERT_EQ(true, is_session_failure) << "Session connection established";
}

/**
 * Perform connection to DSE using internal authentication
 *
 * This test will perform a connection to a DSE server using DSE internal
 * authentication against a single node cluster and execute a simple query.
 *
 * @jira_ticket CPP-350
 * @test_category dse:auth
 * @since 1.0.0
 * @dse_version 5.0.0
 * @expected_result Successful connection and query execution
 */
DSE_INTEGRATION_TEST_F(AuthenticationTest, InternalAuthentication) {
  CHECK_VERSION(5.0.0);
  CHECK_FOR_SKIPPED_TEST;
  CHECK_FAILURE;

  // Connect, and query the system table
  connect_using_internal_authentication_and_query_system_table(CASSANDRA_USER, CASSANDRA_PASSWORD);
}

/**
 * Perform a failing connection to DSE with bad credentials using internal
 * authentication
 *
 * This test will perform a connection to a DSE server using DSE internal
 * authentication against a single node cluster where an attempt is made to
 * authenticate a user with bad credentials.
 *
 * @jira_ticket CPP-350
 * @test_category dse:auth
 * @since 1.0.0
 * @dse_version 5.0.0
 * @expected_result Connection is unsuccessful; Bad credentials
 */
DSE_INTEGRATION_TEST_F(AuthenticationTest, InternalAuthenticationFailure) {
  CHECK_VERSION(5.0.0)
  CHECK_FOR_SKIPPED_TEST;
  CHECK_FAILURE;

  // Attempt to connect and ensure failed connection
  bool is_session_failure = false;
  try {
    connect_using_internal_authentication_and_query_system_table("invalid", "invalid");
  } catch (Session::Exception& se) {
    TEST_LOG(se.what());
    ASSERT_EQ(CASS_ERROR_SERVER_BAD_CREDENTIALS, se.error_code())
        << "Error code is not 'Bad credentials'";
    is_session_failure = true;
  }
  ASSERT_EQ(true, is_session_failure) << "Session connection established";
}

/**
 * Use an empty principal with a credential added to the credential cache.
 *
 * The default credential in the cache should be used to authenticate the
 * request.
 *
 * @test_category dse:auth
 * @since 1.0.0
 * @dse_version 5.0.0
 * @expected_result Successful connection and query execution
 */
DSE_INTEGRATION_TEST_F(AuthenticationTest, EmptyPrincipalCredentialCache) {
  CHECK_FOR_SKIPPED_TEST;
  CHECK_FAILURE;
  ads_->acquire_ticket(CASSANDRA_USER_PRINCIPAL, ads_->get_cassandra_keytab_file());
  connect_using_kerberos_and_query_system_table("");
}

/**
 * Use a keytab to authenticate the request.
 *
 * The cassandra principal will be use to find the principal in the provided
 * keytab.
 *
 * @test_category dse:auth
 * @since 1.0.0
 * @dse_version 5.0.0
 * @expected_result Successful connection and query execution
 */
DSE_INTEGRATION_TEST_F(AuthenticationTest, UseKeytab) {
  CHECK_FOR_SKIPPED_TEST;
  CHECK_FAILURE;

  ads_->use_keytab(ads_->get_cassandra_keytab_file());
  connect_using_kerberos_and_query_system_table(CASSANDRA_USER_PRINCIPAL);
}

/**
 * Use an empty principal with a keytab file.
 *
 * The default credential in the keytab should be used to authenticate the
 * request.
 *
 * NOTE: This test is not valid with Heimdal
 *
 * @test_category dse:auth
 * @since 1.0.0
 * @dse_version 5.0.0
 * @expected_result Successful connection and query execution
 */
DSE_INTEGRATION_TEST_F(AuthenticationTest, EmptyPrincipalKeytab) {
  CHECK_FOR_SKIPPED_TEST;
  CHECK_FOR_KERBEROS_HEIMDAL;
  CHECK_FAILURE;

  ads_->use_keytab(ads_->get_cassandra_keytab_file());
  connect_using_kerberos_and_query_system_table("");
}
