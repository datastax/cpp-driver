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

#include "ssl_certificates.hpp"

class SslTests : public Integration {
public:
  SslTests() {
    is_session_requested_ = false;
    is_ssl_ = true;
  }

  static void SetUpTestCase() { SslCertificates::write_ccm_server_files(); }

  /**
   * Perform simple write and read operations and ensure data is being encrypted
   */
  void write_and_read() {
    logger_.add_critera("encrypted bytes");

    session_.execute(
        format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, table_name_.c_str(), "int", "int"));
    Prepared insert_prepared = session_.prepare(
        format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, table_name_.c_str(), "?", "?"));
    Prepared select_prepared =
        session_.prepare(format_string(CASSANDRA_SELECT_VALUE_FORMAT, table_name_.c_str(), "?"));

    for (int i = 1; i < 10; ++i) {
      Statement statement = insert_prepared.bind();
      statement.bind<Integer>(0, Integer(i));
      statement.bind<Integer>(1, Integer(i + 100));
      Result result = session_.execute(statement);
      ASSERT_EQ(CASS_OK, result.error_code()) << result.error_message();
    }
    for (int i = 1; i < 10; ++i) {
      Statement statement = select_prepared.bind();
      statement.bind<Integer>(0, Integer(i));
      Result result = session_.execute(statement);
      ASSERT_EQ(CASS_OK, result.error_code()) << result.error_message();
      ASSERT_EQ(1u, result.column_count());
      ASSERT_EQ(1u, result.row_count());
      ASSERT_EQ(Integer(i + 100), result.first_row().next().as<Integer>());
    }

    ASSERT_GT(logger_.count(), 0u) << "Encrypted bytes were not sent to the server";
  }
};

class SslClientAuthenticationTests : public SslTests {
public:
  SslClientAuthenticationTests() { is_client_authentication_ = true; }
};

class SslNoClusterTests : public SslTests {
public:
  SslNoClusterTests() { is_ccm_requested_ = false; }
};

class SslNoSslOnClusterTests : public SslTests {
public:
  SslNoSslOnClusterTests() { is_ssl_ = false; }
};

/**
 * Ensures NULL and invalid client certificates return an error when using the C API.
 */
CASSANDRA_INTEGRATION_TEST_F(SslNoClusterTests, InvalidCert) {
  { // Null
    Ssl ssl;
    EXPECT_EQ(CASS_ERROR_SSL_INVALID_CERT, cass_ssl_set_cert(ssl.get(), NULL));
  }

  { // Invalid
    Ssl ssl;
    EXPECT_EQ(CASS_ERROR_SSL_INVALID_CERT, cass_ssl_set_cert(ssl.get(), "invalid"));
  }
}

/**
 * Ensures NULL and invalid peer/server certificates return an error when using the C API.
 */
CASSANDRA_INTEGRATION_TEST_F(SslNoClusterTests, InvalidPeerCert) {
  { // Null
    Ssl ssl;
    EXPECT_EQ(CASS_ERROR_SSL_INVALID_CERT, cass_ssl_add_trusted_cert(ssl.get(), NULL));
  }

  { // Invalid
    Ssl ssl;
    EXPECT_EQ(CASS_ERROR_SSL_INVALID_CERT, cass_ssl_add_trusted_cert(ssl.get(), "invalid"));
  }
}

/**
 * Ensures NULL and invalid private key values return an error when using the C API.
 */
CASSANDRA_INTEGRATION_TEST_F(SslNoClusterTests, InvalidPrivateKey) {
  { // Null
    Ssl ssl;
    EXPECT_EQ(CASS_ERROR_SSL_INVALID_PRIVATE_KEY, cass_ssl_set_private_key(ssl.get(), NULL, NULL));
    EXPECT_EQ(CASS_ERROR_SSL_INVALID_PRIVATE_KEY, cass_ssl_set_private_key(ssl.get(), NULL, ""));
    EXPECT_EQ(CASS_ERROR_SSL_INVALID_PRIVATE_KEY, cass_ssl_set_private_key(ssl.get(), "", NULL));
  }

  { // Invalid
    Ssl ssl;
    EXPECT_EQ(CASS_ERROR_SSL_INVALID_PRIVATE_KEY,
              cass_ssl_set_private_key(ssl.get(), "invalid", "invalid"));
  }
}

/**
 * Ensures SSL connection without verification while performing write and read operations.
 */
CASSANDRA_INTEGRATION_TEST_F(SslTests, VerifyNone) {
  CHECK_FAILURE;

  Ssl ssl;
  ssl.with_verify_flags(CASS_SSL_VERIFY_NONE);

  Cluster cluster = default_cluster().with_ssl(ssl);
  connect(cluster);
  write_and_read();
}

/**
 * Ensures SSL connection verifying peer/server certificate while performing write and read
 * operations.
 */
CASSANDRA_INTEGRATION_TEST_F(SslTests, VerifyPeer) {
  CHECK_FAILURE;

  Ssl ssl;
  ssl.with_verify_flags(CASS_SSL_VERIFY_PEER_CERT);
  ssl.add_trusted_cert(SslCertificates::cassandra_pem());

  Cluster cluster = default_cluster().with_ssl(ssl);
  connect(cluster);
  write_and_read();
}

/**
 * Ensures SSL connection verifying peer/server certificate and identity while performing write and
 * read operations.
 */
CASSANDRA_INTEGRATION_TEST_F(SslTests, VerifyPeerIdentity) {
  CHECK_FAILURE;

  Ssl ssl;
  ssl.with_verify_flags(CASS_SSL_VERIFY_PEER_IDENTITY);
  ssl.add_trusted_cert(SslCertificates::cassandra_pem());

  Cluster cluster = default_cluster().with_ssl(ssl);
  connect(cluster);
  write_and_read();
}

/**
 * Ensures SSL connection verifying peer/server certificate while performing write and read
 * operations.
 */
CASSANDRA_INTEGRATION_TEST_F(SslTests, VerifyPeerMultipleCerts) {
  CHECK_FAILURE;

  Ssl ssl;
  ssl.with_verify_flags(CASS_SSL_VERIFY_PEER_CERT);
  ssl.add_trusted_cert(SslCertificates::multi_cert_pem());

  Cluster cluster = default_cluster().with_ssl(ssl);
  connect(cluster);
  write_and_read();
}

/**
 * Ensures that when one node (in this case the whole cluster) is terminated and restarted the
 * driver will reconnect without throwing errors. Each stage (connect and reconnect) write and read
 * operations are performed.
 *
 * @since 2.6.0
 * @jira_ticket CPP-408
 */
CASSANDRA_INTEGRATION_TEST_F(SslTests, ReconnectAfterClusterCrashAndRestart) {
  CHECK_FAILURE;
  is_test_chaotic_ = true;

  Ssl ssl;
  ssl.with_verify_flags(CASS_SSL_VERIFY_PEER_CERT);
  ssl.add_trusted_cert(SslCertificates::cassandra_pem());

  Cluster cluster = default_cluster()
                        .with_constant_reconnect(100) // Quick reconnect
                        .with_ssl(ssl);
  connect(cluster);
  write_and_read();

  ccm_->hang_up_cluster(); // SIGHUP
  logger_.add_critera("Lost control connection to host");
  wait_for_logger(1);
  logger_.reset();
  ccm_->start_cluster();
  logger_.add_critera("Connected to host");
  write_and_read();
}

/**
 * Ensures SSL connection verifying peer/server certificate and identity with server verifying
 * client authentication while performing write and read operations.
 */
CASSANDRA_INTEGRATION_TEST_F(SslClientAuthenticationTests, VerifyPeerIdentity) {
  CHECK_FAILURE;

  Ssl ssl;
  ssl.with_verify_flags(CASS_SSL_VERIFY_PEER_IDENTITY);
  ssl.add_trusted_cert(SslCertificates::cassandra_pem());
  ssl.with_cert(SslCertificates::driver_pem());
  ssl.with_private_key(SslCertificates::driver_private_pem(),
                       SslCertificates::driver_private_pem_password());

  Cluster cluster = default_cluster().with_ssl(ssl);
  connect(cluster);
  write_and_read();
}

/**
 * Ensures SSL connection fails when attempting to connect to a server without SSL configured.
 */
CASSANDRA_INTEGRATION_TEST_F(SslNoSslOnClusterTests, FailToConnect) {
  CHECK_FAILURE;

  Ssl ssl;
  ssl.with_verify_flags(CASS_SSL_VERIFY_NONE);

  ASSERT_THROW(default_cluster().with_ssl(ssl).connect(), Session::Exception);
}
