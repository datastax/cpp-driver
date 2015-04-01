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

#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#ifdef _WIN32
#   ifndef CASS_STATIC
#      include <openssl/applink.c>
#   endif
#endif

#include <boost/chrono.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/thread.hpp>
#include <boost/format.hpp>

#include "cassandra.h"
#include "cql_ccm_bridge.hpp"
#include "test_utils.hpp"

#define CASSANDRA_PEM_CERTIFICATE_FILENAME "ssl/cassandra.pem"
#define DRIVER_PEM_CERTIFICATE_FILENAME "ssl/driver.pem"
#define DRIVER_PEM_PRIVATE_KEY_FILENAME "ssl/driver-private.pem"
#define DRIVER_PEM_PRIVATE_KEY_PASSWORD "driver"
#define INVALID_CASSANDRA_PEM_CERTIFICATE_FILENAME "ssl/invalid/cassandra-invalid.pem"
#define INVALID_DRIVER_PEM_CERTIFICATE_FILENAME "ssl/invalid/driver-invalid.pem"
#define INVALID_DRIVER_PEM_PRIVATE_KEY_FILENAME "ssl/invalid/driver-private-invalid.pem"
#define INVALID_DRIVER_PEM_PRIVATE_KEY_PASSWORD "invalid"
#define CCM_CLUSTER_NAME "test"

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
   * CCM bridge configuration
   */
  const cql::cql_ccm_bridge_configuration_t& configuration_;
  /**
   * CCM bridge instance for performing additional operations against cluster
   */
  boost::shared_ptr<cql::cql_ccm_bridge_t> ccm_;
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
      : configuration_(cql::get_ccm_bridge_configuration())
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
   * @param protocol_version Protocol version to use for connection (default: 2)
   */
  void setup(bool is_ssl = true, bool is_client_authentication = false, bool is_failure = false, unsigned int nodes = 1, unsigned int protocol_version = 2) {
    //Create a n-node cluster
    ccm_ = cql::cql_ccm_bridge_t::create_and_start(configuration_, CCM_CLUSTER_NAME, nodes, 0, is_ssl, is_client_authentication);

    //Initialize the cpp-driver
    cluster_ = cass_cluster_new();
    test_utils::initialize_contact_points(cluster_, configuration_.ip_prefix(), nodes, 0);
    cass_cluster_set_connect_timeout(cluster_, 10000);
    cass_cluster_set_request_timeout(cluster_, 10000);
    cass_cluster_set_num_threads_io(cluster_, 1);
    cass_cluster_set_protocol_version(cluster_, protocol_version);
    cass_cluster_set_ssl(cluster_, ssl_);

    //Establish the connection (if ssl)
    session_ = cass_session_new();
    connect_future_ = cass_session_connect(session_, cluster_);
    if (!is_failure) {
      test_utils::wait_and_check_error(connect_future_);
    } else {
      BOOST_REQUIRE(!cass_future_wait_timed(connect_future_, 2000)); //Ensure the wait is long enough for slow machines
    }
  }

  /**
   * Alias to driver connection cleanup
   */
  void teardown() {
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
  setup();
  test_normal_load();
  test_high_load();
  teardown();

  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert_n(ssl_, cassandra_certificate_.data(), cassandra_certificate_.size()), CASS_OK);
  setup();
  test_normal_load();
  test_high_load();
  teardown();
  
  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_IDENTITY);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert_n(ssl_, cassandra_certificate_.data(), cassandra_certificate_.size()), CASS_OK);
  setup();
  test_normal_load();
  test_high_load();
  teardown();

  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert(ssl_, cassandra_certificate_.c_str()), CASS_OK);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_cert(ssl_, driver_certificate_.c_str()), CASS_OK);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_private_key(ssl_, driver_private_key_.c_str(), DRIVER_PEM_PRIVATE_KEY_PASSWORD), CASS_OK);
  setup(true, true);
  test_normal_load();
  test_high_load();
  teardown();
}

BOOST_AUTO_TEST_CASE(connect_failures) {
  //Load invalid certificates
  create_ssl_context();
  BOOST_REQUIRE_EQUAL(cass_ssl_set_cert(ssl_, "Invalid Client Certificate"), CASS_ERROR_SSL_INVALID_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert(ssl_, "Invalid Trusted Certificate"), CASS_ERROR_SSL_INVALID_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_private_key(ssl_, "Invalid Private Key", "invalid"), CASS_ERROR_SSL_INVALID_PRIVATE_KEY);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_private_key(ssl_, driver_private_key_.c_str(), "invalid"), CASS_ERROR_SSL_INVALID_PRIVATE_KEY);

  //Connect with SSL where the Cassandra server has SSL disabled
  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_NONE);
  setup(false, false, true);
  teardown();
  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert(ssl_, cassandra_certificate_.c_str()), CASS_OK);
  setup(false, false, true);
  teardown();
  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_IDENTITY);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert(ssl_, cassandra_certificate_.c_str()), CASS_OK);
  setup(false, false, true);
  teardown();
  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert(ssl_, cassandra_certificate_.c_str()), CASS_OK);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_cert(ssl_, driver_certificate_.c_str()), CASS_OK);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_private_key(ssl_, driver_private_key_.c_str(), DRIVER_PEM_PRIVATE_KEY_PASSWORD), CASS_OK);
  setup(false, false, true);
  teardown();

  //Connect with SSL with invalid peer and client certificates
  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert(ssl_, invalid_cassandra_certificate_.c_str()), CASS_OK);
  setup(true, false, true);
  teardown();
  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_IDENTITY);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert(ssl_, invalid_cassandra_certificate_.c_str()), CASS_OK);
  setup(true, false, true);
  teardown();
  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert(ssl_, invalid_cassandra_certificate_.c_str()), CASS_OK);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_cert(ssl_, driver_certificate_.c_str()), CASS_OK);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_private_key(ssl_, driver_private_key_.c_str(), DRIVER_PEM_PRIVATE_KEY_PASSWORD), CASS_OK);
  setup(true, true, true);
  teardown();
  create_ssl_context();
  cass_ssl_set_verify_flags(ssl_, CASS_SSL_VERIFY_PEER_CERT);
  BOOST_REQUIRE_EQUAL(cass_ssl_add_trusted_cert(ssl_, cassandra_certificate_.c_str()), CASS_OK);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_cert(ssl_, invalid_driver_certificate_.c_str()), CASS_OK);
  BOOST_REQUIRE_EQUAL(cass_ssl_set_private_key(ssl_, invalid_driver_private_key_.c_str(), INVALID_DRIVER_PEM_PRIVATE_KEY_PASSWORD), CASS_OK);
  setup(true, true, true);
  teardown();
}

BOOST_AUTO_TEST_SUITE_END()

