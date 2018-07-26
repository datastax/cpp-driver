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

#ifdef _WIN32
#   ifndef CASS_STATIC
#      include <openssl/applink.c>
#   endif
#endif

#include <boost/chrono.hpp>
#include <boost/test/debug.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>
#include <boost/format.hpp>

#include "cassandra.h"
#include "test_utils.hpp"

#define CASSANDRA_PEM_CERTIFICATE_FILENAME "ssl/cassandra.pem"
#define DRIVER_PEM_CERTIFICATE_FILENAME "ssl/driver.pem"
#define DRIVER_PEM_PRIVATE_KEY_FILENAME "ssl/driver-private.pem"
#define DRIVER_PEM_PRIVATE_KEY_PASSWORD "driver"
#define INVALID_CASSANDRA_PEM_CERTIFICATE_FILENAME "ssl/invalid/cassandra-invalid.pem"
#define INVALID_DRIVER_PEM_CERTIFICATE_FILENAME "ssl/invalid/driver-invalid.pem"
#define INVALID_DRIVER_PEM_PRIVATE_KEY_FILENAME "ssl/invalid/driver-private-invalid.pem"
#define INVALID_DRIVER_PEM_PRIVATE_KEY_PASSWORD "invalid"

#define NUMBER_OF_ITERATIONS 4

/**
 * SSL Test Class
 *
 * The purpose of this class is to setup helper methods for a single session
 * integration test suite to initialize a cluster through CCM in order to
 * perform SSL tests.
 */
struct TestSSL {
  /**
   * CCM bridge instance for performing additional operations against cluster
   */
  boost::shared_ptr<CCM::Bridge> ccm_;
  /**
   * Cluster used for discovering nodes during the session connection
   */
  CassCluster* cluster_;
  /**
   * Future used to establish session connection with the cluster
   */
  CassFuture* connect_future_;
  /**
   * Session instance to the Cassandra cluster in order to perform query
   * operations
   */
  CassSession* session_;
  /**
   * SSL context for session
   */
  CassSsl* ssl_;
  /**
   * Cassandra certificate
   */
  std::string cassandra_certificate_;
  /**
   * Client/Driver certificate
   */
  std::string driver_certificate_;
  /**
   * Client/Driver private key
   */
  std::string driver_private_key_;
  /**
   * Invalid Cassandra certificate
   */
  std::string invalid_cassandra_certificate_;
  /**
   * Invalid client/driver certificate
   */
  std::string invalid_driver_certificate_;
  /**
   * Invalid client/driver private key
   */
  std::string invalid_driver_private_key_;

  /**
   * Constructor
   */
  TestSSL()
      : ccm_(new CCM::Bridge("config.txt"))
      , cluster_(NULL)
      , connect_future_(NULL)
      , session_(NULL)
      , ssl_(NULL) {
    //Load the PEM certificates and private keys
    cassandra_certificate_ = test_utils::load_ssl_certificate(CASSANDRA_PEM_CERTIFICATE_FILENAME);
    driver_certificate_ = test_utils::load_ssl_certificate(DRIVER_PEM_CERTIFICATE_FILENAME);
    driver_private_key_ = test_utils::load_ssl_certificate(DRIVER_PEM_PRIVATE_KEY_FILENAME);
    invalid_cassandra_certificate_ = test_utils::load_ssl_certificate(INVALID_CASSANDRA_PEM_CERTIFICATE_FILENAME);
    invalid_driver_certificate_ = test_utils::load_ssl_certificate(INVALID_DRIVER_PEM_CERTIFICATE_FILENAME);
    invalid_driver_private_key_ = test_utils::load_ssl_certificate(INVALID_DRIVER_PEM_PRIVATE_KEY_FILENAME);
  }

  /**
   * Destructor
   */
  ~TestSSL() {
    cleanup();
  }

  /**
   * Create the Cassandra cluster, initialize the cpp-driver cluster, and create
   * the connection to the cluster.
   *
   * @param is_ssl True if SSL should be enabled on Cassandra cluster; false
   *               otherwise (default: true)
   * @param is_client_authentication True if client authentication should be
   *                                 enabled on Cassandra cluster; false otherwise
   *                                 (default: false)
   * @param is_failure True if test is supposed to fail; false otherwise
   *                   (default: false)
   * @param nodes Number of nodes for the cluster (default: 1)
   */
  void ssl_setup(bool is_ssl = true, bool is_client_authentication = false, bool is_failure = false, unsigned int nodes = 1) {
    //Create a n-node cluster
    ccm_->create_cluster(nodes, 0, false, is_ssl, is_client_authentication);
    ccm_->start_cluster();

    //Initialize the cpp-driver
    cluster_ = cass_cluster_new();
    test_utils::initialize_contact_points(cluster_, ccm_->get_ip_prefix(), nodes);
    cass_cluster_set_connect_timeout(cluster_, 10000);
    cass_cluster_set_request_timeout(cluster_, 10000);
    cass_cluster_set_num_threads_io(cluster_, 1);
    cass_cluster_set_core_connections_per_host(cluster_, 2);
    cass_cluster_set_max_connections_per_host(cluster_, 4);
    cass_cluster_set_ssl(cluster_, ssl_);

    //Establish the connection (if ssl)
    session_ = cass_session_new();
    connect_future_ = cass_session_connect(session_, cluster_);
    if (!is_failure) {
      test_utils::wait_and_check_error(connect_future_);
    } else {
      BOOST_REQUIRE(cass_future_wait_timed(connect_future_, 2000000)); //Ensure the wait is long enough for slow machines
    }
  }

  /**
   * Alias to driver connection cleanup
   */
  void ssl_teardown() {
    cleanup();
  }

  /**
   * Cleanup the driver connection
   */
  void cleanup() {
    if (session_) {
      cass_session_free(session_);
      session_ = NULL;
    }

    if (cluster_) {
      cass_cluster_free(cluster_);
      cluster_ = NULL;
    }

    if (connect_future_) {
      cass_future_free(connect_future_);
      connect_future_ = NULL;
    }

    if (ssl_) {
      cass_ssl_free(ssl_);
      ssl_ = NULL;
    }
  }

  /**
   * Create the SSL context; clean-up existing context if exists
   */
  void create_ssl_context() {
    if (ssl_) {
      cass_ssl_free(ssl_);
      ssl_ = NULL;
    }
    ssl_ = cass_ssl_new();
  }

  /**
   * "Crash" the cluster by sending the SIGHUP signal while executing a simple
   * version query when applicable
   *
   * @param wait_s Wait time in seconds for each stage of the shutdown/restart
   */
  void crash_and_restart_cluster(unsigned int wait_s = 5) {
    test_utils::get_version(session_);
    ccm_->hang_up_cluster();
    boost::this_thread::sleep_for(boost::chrono::seconds(wait_s));
    ccm_->start_cluster();
    boost::this_thread::sleep_for(boost::chrono::seconds(wait_s));
    test_utils::get_version(session_);
  }

  /**
   * Test established connection with a normal load query
   */
  void test_normal_load() {
    //Create and use the simple keyspace
    test_utils::execute_query(session_, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % "1"));
    test_utils::execute_query(session_, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));

    //Create a table to fill with numbers and characters
    test_utils::execute_query(session_, "CREATE TABLE normal_load (key int PRIMARY KEY, a int, b float, c text)");

    //Perform queries and validate inserted data
    for (int n = 0; n < NUMBER_OF_ITERATIONS; ++n) {
      std::string text = test_utils::generate_random_string(16);
      test_utils::execute_query(session_, str(boost::format("INSERT INTO normal_load (key, a, b, c) VALUES (%d, %d, %f, '%s')") % n % (n * 100) % (n * .001f) % text));

      test_utils::CassResultPtr result;
      test_utils::execute_query(session_, str(boost::format("SELECT * FROM normal_load WHERE key = %d") % n), &result);
      BOOST_REQUIRE_EQUAL(cass_result_column_count(result.get()), 4u);
      BOOST_REQUIRE_EQUAL(cass_result_row_count(result.get()), 1u);

      const CassRow* row = cass_result_first_row(result.get());
      const CassValue* value;
      value = cass_row_get_column_by_name(row, "key");
      BOOST_REQUIRE(value != NULL);
      cass_int32_t key;
      BOOST_REQUIRE_EQUAL(cass_value_get_int32(value, &key), CASS_OK);
      BOOST_CHECK_EQUAL(key, n);

      value = cass_row_get_column_by_name(row, "a");
      BOOST_REQUIRE(value != NULL);
      cass_int32_t a;
      BOOST_REQUIRE_EQUAL(cass_value_get_int32(value, &a), CASS_OK);
      BOOST_CHECK_EQUAL(a, (n * 100));

      value = cass_row_get_column_by_name(row, "b");
      BOOST_REQUIRE(value != NULL);
      cass_float_t b;
      BOOST_REQUIRE_EQUAL(cass_value_get_float(value, &b), CASS_OK);
      BOOST_CHECK_EQUAL(b, (n * .001f));

      value = cass_row_get_column_by_name(row, "c");
      BOOST_REQUIRE(value != NULL);
      CassString c;
      BOOST_REQUIRE_EQUAL(cass_value_get_string(value, &c.data, &c.length), CASS_OK);
      BOOST_CHECK(test_utils::Value<CassString>::equal(c, CassString(text.c_str())));
    }

    //Drop the table and keyspace
    test_utils::execute_query(session_, "DROP TABLE normal_load");
    test_utils::execute_query(session_, str(boost::format("DROP KEYSPACE %s")  % test_utils::SIMPLE_KEYSPACE));
  }

  /**
   * Test established connection with a high load query
   */
  void test_high_load() {
    //Create and use the simple keyspace
    test_utils::execute_query(session_, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % "1"));
    test_utils::execute_query(session_, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));

    //Create a table to fill with large text fields
    test_utils::execute_query(session_, "CREATE TABLE high_load (key int PRIMARY KEY, a text, b text, c text)");

    //Perform queries and validate inserted data
    for (int n = 0; n < NUMBER_OF_ITERATIONS; ++n) {
      std::string text_a = test_utils::generate_random_string(10240);
      std::string text_b = test_utils::generate_random_string(20480);
      std::string text_c = test_utils::generate_random_string(40960);
      test_utils::execute_query(session_, str(boost::format("INSERT INTO high_load (key, a, b, c) VALUES (%d, '%s', '%s', '%s')") % n % text_a % text_b % text_c));

      test_utils::CassResultPtr result;
      test_utils::execute_query(session_, str(boost::format("SELECT * FROM high_load WHERE key = %d") % n), &result);
      BOOST_REQUIRE_EQUAL(cass_result_column_count(result.get()), 4u);
      BOOST_REQUIRE_EQUAL(cass_result_row_count(result.get()), 1u);

      const CassRow* row = cass_result_first_row(result.get());
      const CassValue* value;
      value = cass_row_get_column_by_name(row, "key");
      BOOST_REQUIRE(value != NULL);
      cass_int32_t key;
      BOOST_REQUIRE_EQUAL(cass_value_get_int32(value, &key), CASS_OK);
      BOOST_CHECK_EQUAL(key, n);

      value = cass_row_get_column_by_name(row, "a");
      BOOST_REQUIRE(value != NULL);
      CassString a;
      BOOST_REQUIRE_EQUAL(cass_value_get_string(value, &a.data, &a.length), CASS_OK);
      BOOST_CHECK(test_utils::Value<CassString>::equal(a, CassString(text_a.c_str())));

      value = cass_row_get_column_by_name(row, "b");
      BOOST_REQUIRE(value != NULL);
      CassString b;
      BOOST_REQUIRE_EQUAL(cass_value_get_string(value, &b.data, &b.length), CASS_OK);
      BOOST_CHECK(test_utils::Value<CassString>::equal(b, CassString(text_b.c_str())));

      value = cass_row_get_column_by_name(row, "c");
      BOOST_REQUIRE(value != NULL);
      CassString c;
      BOOST_REQUIRE_EQUAL(cass_value_get_string(value, &c.data, &c.length), CASS_OK);
      BOOST_CHECK(test_utils::Value<CassString>::equal(c, CassString(text_c.c_str())));
    }

    //Drop the table and keyspace
    test_utils::execute_query(session_, "DROP TABLE high_load");
    test_utils::execute_query(session_, str(boost::format("DROP KEYSPACE %s")  % test_utils::SIMPLE_KEYSPACE));
  }
};

BOOST_FIXTURE_TEST_SUITE(ssl, TestSSL)

BOOST_AUTO_TEST_CASE(connect) {
  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_NONE);
  ssl_setup();
  test_normal_load();
  test_high_load();
  ssl_teardown();

  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert_n(ssl_, cassandra_certificate_.data(), cassandra_certificate_.size()), CASS_OK);
  ssl_setup();
  test_normal_load();
  test_high_load();
  ssl_teardown();

  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_IDENTITY);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert_n(ssl_, cassandra_certificate_.data(), cassandra_certificate_.size()), CASS_OK);
  ssl_setup();
  test_normal_load();
  test_high_load();
  ssl_teardown();

  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert(ssl_, cassandra_certificate_.c_str()), CASS_OK);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_cert(ssl_, driver_certificate_.c_str()), CASS_OK);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_private_key(ssl_, driver_private_key_.c_str(), DRIVER_PEM_PRIVATE_KEY_PASSWORD), CASS_OK);
  ssl_setup(true, true);
  test_normal_load();
  test_high_load();
  ssl_teardown();
}

BOOST_AUTO_TEST_CASE(connect_failures) {
  //Load invalid certificates
  create_ssl_context();
  BOOST_REQUIRE_EQUAL(cass_ssl_set_cert(ssl_, "Invalid Client Certificate"), CASS_ERROR_SSL_INVALID_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert(ssl_, "Invalid Trusted Certificate"), CASS_ERROR_SSL_INVALID_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_private_key(ssl_, "Invalid Private Key", "invalid"), CASS_ERROR_SSL_INVALID_PRIVATE_KEY);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_private_key(ssl_, driver_private_key_.c_str(), "invalid"), CASS_ERROR_SSL_INVALID_PRIVATE_KEY);

  // Null certificate arg
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert(ssl_, NULL), CASS_ERROR_SSL_INVALID_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_cert(ssl_, NULL), CASS_ERROR_SSL_INVALID_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_private_key(ssl_, NULL, "invalid"), CASS_ERROR_SSL_INVALID_PRIVATE_KEY);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_private_key(ssl_, driver_private_key_.c_str(), NULL), CASS_ERROR_SSL_INVALID_PRIVATE_KEY);

  //Connect with SSL where the Cassandra server has SSL disabled
  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_NONE);
  ssl_setup(false, false, true);
  ssl_teardown();
  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert(ssl_, cassandra_certificate_.c_str()), CASS_OK);
  ssl_setup(false, false, true);
  ssl_teardown();
  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_IDENTITY);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert(ssl_, cassandra_certificate_.c_str()), CASS_OK);
  ssl_setup(false, false, true);
  ssl_teardown();
  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert(ssl_, cassandra_certificate_.c_str()), CASS_OK);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_cert(ssl_, driver_certificate_.c_str()), CASS_OK);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_private_key(ssl_, driver_private_key_.c_str(), DRIVER_PEM_PRIVATE_KEY_PASSWORD), CASS_OK);
  ssl_setup(false, false, true);
  ssl_teardown();

  //Connect with SSL with invalid peer and client certificates
  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert(ssl_, invalid_cassandra_certificate_.c_str()), CASS_OK);
  ssl_setup(true, false, true);
  ssl_teardown();
  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_IDENTITY);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert(ssl_, invalid_cassandra_certificate_.c_str()), CASS_OK);
  ssl_setup(true, false, true);
  ssl_teardown();
  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert(ssl_, invalid_cassandra_certificate_.c_str()), CASS_OK);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_cert(ssl_, driver_certificate_.c_str()), CASS_OK);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_private_key(ssl_, driver_private_key_.c_str(), DRIVER_PEM_PRIVATE_KEY_PASSWORD), CASS_OK);
  ssl_setup(true, true, true);
  ssl_teardown();
  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert(ssl_, cassandra_certificate_.c_str()), CASS_OK);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_cert(ssl_, invalid_driver_certificate_.c_str()), CASS_OK);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_private_key(ssl_, invalid_driver_private_key_.c_str(), INVALID_DRIVER_PEM_PRIVATE_KEY_PASSWORD), CASS_OK);
  ssl_setup(true, true, true);
  ssl_teardown();
}

/**
 * Driver reconnect when using SSL and node is terminated and restarted
 *
 * This test will ensure that one node is terminated (forced) and restarted and
 * the driver will reconnect without throwing LIB errors.
 *
 * @since 2.6.0
 * @jira_ticket CPP-408
 * @test_category connection:ssl
 * @test_category control_connection
 * @expected_results Driver will reconnect without issues to a cluster using
 *                   SSL that has crashed and brought back up
 */
BOOST_AUTO_TEST_CASE(reconnect_after_cluster_crash_and_restart) {
  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert_n(ssl_, cassandra_certificate_.data(), cassandra_certificate_.size()), CASS_OK);
  ssl_setup();
  crash_and_restart_cluster();
  ssl_teardown();
  test_utils::CassLog::set_output_log_level(CASS_LOG_DISABLED);
}

BOOST_AUTO_TEST_SUITE_END()

