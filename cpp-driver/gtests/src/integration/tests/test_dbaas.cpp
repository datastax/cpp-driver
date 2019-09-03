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

#include "process.hpp"

#define PROXY_CREDS_V1_INVALID_CA_FILENAME "creds-v1-invalid-ca.zip"
#define PROXY_CREDS_V1_UNREACHABLE_FILENAME "creds-v1-unreachable.zip"
#define PROXY_CREDS_V1_NO_CERT_FILENAME "creds-v1-wo-cert.zip"
#define PROXY_CREDS_V1_NO_CREDS_FILENAME "creds-v1-wo-creds.zip"
#define PROXY_CREDS_V1_FILENAME "creds-v1.zip"

#ifdef WIN32
#define PROXY_RUN_SCRIPT "run.ps1"
#define PROXY_CREDS_BUNDLES "certs\\bundles\\"
#else
#define PROXY_RUN_SCRIPT "run.sh"
#define PROXY_CREDS_BUNDLES "certs/bundles/"
#endif

using test::Utils;
using utils::Process;

/**
 * Database as a service integration tests
 */
class DbaasTests : public Integration {
public:
  typedef std::map<int, std::string> ServerNames;
  typedef std::pair<int, std::string> ServerPair;

  static void SetUpTestCase() {
    char* proxy_path = getenv("PROXY_PATH");
    if (proxy_path) {
      proxy_path_ = proxy_path;
    } else {
      proxy_path_ = Utils::home_directory() + Utils::PATH_SEPARATOR + "proxy";
    }
    proxy_path_ += Utils::PATH_SEPARATOR;
    proxy_run_script_ = proxy_path_ + PROXY_RUN_SCRIPT;

    // Allow the proxy to start itself or use a currently running proxy
    if (file_exists(proxy_run_script_)) {
      if (!start_proxy()) {
        FAIL() << "Unable to start SNI single endpoint proxy service. Check PROXY_PATH environment "
                  "variable"
#ifdef WIN32
               << " or ensure proper ExecutionPolicy is set (e.g. Set-ExecutionPolicy -Scope "
                  "CurrentUser Unrestricted); see "
                  "https:/go.microsoft.com/fwlink/?LinkID=135170"
#endif
               << ".";
      }
    } else {
      if (!is_proxy_running()) {
        FAIL()
            << "SNI single endpoint proxy is not available. Start container before executing test.";
      }
    }

    if (!file_exists(proxy_cred_bundles_path_)) {
      proxy_cred_bundles_path_ = proxy_path_ + proxy_cred_bundles_path_;
    }
    if (!file_exists(creds_v1_invalid_ca()) || !file_exists(creds_v1_unreachable()) ||
        !file_exists(creds_v1_no_cert()) || !file_exists(creds_v1_no_creds()) ||
        !file_exists(creds_v1())) {
      FAIL() << "Unable to locate SNI single endpoint credential bundles. Check PROXY_PATH "
                "environment variable.";
    }
  }

  void SetUp() {
    // Ensure CCM and session are not created for these tests
    is_ccm_requested_ = false;
    is_session_requested_ = false;
    Integration::SetUp();
  }

  static void TearDownTestCase() {
    if (!Options::keep_clusters()) {
      stop_proxy();
    }
  }

  static std::string creds_v1_invalid_ca() {
    return proxy_cred_bundles_path_ + PROXY_CREDS_V1_INVALID_CA_FILENAME;
  }

  static std::string creds_v1_unreachable() {
    return proxy_cred_bundles_path_ + PROXY_CREDS_V1_UNREACHABLE_FILENAME;
  }

  static std::string creds_v1_no_cert() {
    return proxy_cred_bundles_path_ + PROXY_CREDS_V1_NO_CERT_FILENAME;
  }

  static std::string creds_v1_no_creds() {
    return proxy_cred_bundles_path_ + PROXY_CREDS_V1_NO_CREDS_FILENAME;
  }

  static std::string creds_v1() { return proxy_cred_bundles_path_ + PROXY_CREDS_V1_FILENAME; }

  int get_node_id(const std::string& rpc_address) {
    std::vector<std::string> octects = explode(rpc_address, '.');
    std::stringstream ss(octects[octects.size() - 1]);
    int node = 0;
    if ((ss >> node).fail()) {
      EXPECT_TRUE(false) << "Unable to parse node number from rpc_address";
    }
    return node;
  }

  /**
   * Vector of server names sorted by node number (e.g. last octet in real IP address)
   */
  ServerNames get_server_names() {
    ServerNames map;
    {
      Cluster cluster = default_cluster(false)
                            .with_randomized_contact_points(false)
                            .with_load_balance_round_robin();
      cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(cluster.get(),
                                                                      creds_v1().c_str());
      Session session = cluster.connect();
      for (int i = 0; i < 3; ++i) {
        Row row = session.execute(SELECT_ALL_SYSTEM_LOCAL_CQL).first_row();
        int node = get_node_id(row.column_by_name<Inet>("rpc_address").str());
        map.insert(ServerPair(node, row.column_by_name<Uuid>("host_id").str()));
      }
    }
    return map;
  }

  bool start_cluster() {
    Process::Args args;
    args.push_back("start");
    args.push_back("--root");
    args.push_back("--wait-for-binary-proto");
    return ccm_execute(args);
  }

  bool stop_cluster() {
    Process::Args args;
    args.push_back("stop");
    return ccm_execute(args);
  }

  bool start_node(int node) {
    Process::Args args;
    args.push_back(node_name(node));
    args.push_back("start");
    args.push_back("--root");
    args.push_back("--wait-for-binary-proto");
    return ccm_execute(args);
  }

  bool stop_node(int node) {
    Process::Args args;
    args.push_back(node_name(node));
    args.push_back("stop");
    return ccm_execute(args);
  }

private:
  std::string node_name(int node) {
    std::stringstream node_name;
    node_name << "node" << node;
    return node_name.str();
  }

  bool ccm_execute(Process::Args args) {
    Process::Args command;
    command.push_back("docker");
    command.push_back("exec");
    command.push_back(get_proxy_id());
    command.push_back("ccm");
    command.insert(command.end(), args.begin(), args.end());
    Process::Result result = Process::execute(command);
    return result.exit_status == 0;
  }

private:
  static std::string get_proxy_id() {
    if (proxy_id_.empty()) {
      Process::Args command;
      command.push_back("docker");
      command.push_back("ps");
      command.push_back("-aqf");
      command.push_back("ancestor=single_endpoint");
      Process::Result result = Process::execute(command);
      proxy_id_ = Utils::trim(result.standard_output);
    }
    return proxy_id_;
  }

  static bool is_proxy_running() { return !get_proxy_id().empty(); }

  static bool start_proxy() {
    if (is_proxy_running()) return true;

    Process::Args command;
#ifdef WIN32
    command.push_back("powershell");
#endif
    command.push_back(proxy_run_script_);
    Process::Result result = Process::execute(command);
    return result.exit_status == 0;
  }

  static bool stop_proxy() {
    Process::Args command;
    command.push_back("docker");
    command.push_back("kill");
    command.push_back(get_proxy_id());
    Process::Result result = Process::execute(command);
    return result.exit_status == 0;
  }

private:
  static std::string proxy_path_;
  static std::string proxy_cred_bundles_path_;
  static std::string proxy_run_script_;
  static std::string proxy_id_;
};

std::string DbaasTests::proxy_path_;
std::string DbaasTests::proxy_cred_bundles_path_ = PROXY_CREDS_BUNDLES;
std::string DbaasTests::proxy_run_script_ = PROXY_RUN_SCRIPT;
std::string DbaasTests::proxy_id_;

/**
 * Perform connection to DBaaS SNI single endpoint docker image.
 *
 * This test will perform a connection to a DBaaS SNI single endpoint while ensuring proper
 * automatic cloud configuration with address resolution.
 *
 * @jira_ticket CPP-787
 * @test_category dbaas
 * @since 2.14.0
 * @expected_result Successful address resolution and connection.
 */
CASSANDRA_INTEGRATION_TEST_F(DbaasTests, ResolveAndConnect) {
  CHECK_FAILURE;

  Cluster cluster = default_cluster(false);
  cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(cluster.get(),
                                                                  creds_v1().c_str());
  cluster.connect();
}

/**
 * Perform query using a simple statement against the DBaaS SNI single endpoint docker image.
 *
 * This test will perform a connection and execute a simple statement query against the
 * system.local table to ensure query execution to a DBaaS SNI single endpoint while validating the
 * results. This test will also ensure that the configured keyspace is assigned as the DBaaS
 * configuration assigns `system` as the default keyspace.
 *
 * @jira_ticket CPP-787
 * @test_category dbaas
 * @test_category queries
 * @since 2.14.0
 * @expected_result Simple statement is executed and nodes are validated.
 */
CASSANDRA_INTEGRATION_TEST_F(DbaasTests, QueryEachNode) {
  CHECK_FAILURE;

  Cluster cluster = default_cluster(false).with_load_balance_round_robin();
  cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(cluster.get(),
                                                                  creds_v1().c_str());
  Session session = cluster.connect();

  ServerNames server_names;
  for (int i = 0; i < 3; ++i) {
    Result result = session.execute(SELECT_ALL_SYSTEM_LOCAL_CQL);
    Uuid expected_host_id = Uuid(result.server_name());
    Row row = result.first_row();

    Uuid host_id = row.column_by_name<Uuid>("host_id");
    int node = get_node_id(row.column_by_name<Inet>("rpc_address").str());
    EXPECT_NE(0, node);
    EXPECT_EQ(expected_host_id, host_id);
    server_names.insert(ServerPair(node, host_id.str()));
  }

  EXPECT_EQ(3u, server_names.size()); // Ensure all three nodes were queried
}

/**
 * Perform query ensuring token aware is enabled by default.
 *
 * This test will perform a connection and execute a insert query against to ensure that token
 * aware is enabled by default when automatically configured .
 *
 * @jira_ticket CPP-787
 * @test_category dbaas
 * @test_category queries
 * @since 2.14.0
 * @expected_result Simple statement is executed and validated against replicas.
 */
CASSANDRA_INTEGRATION_TEST_F(DbaasTests, DcAwareTokenAwareRoutingDefault) {
  CHECK_FAILURE;

  ServerNames server_names = get_server_names();

  // Validate replicas are used during token aware routing
  std::vector<std::pair<int, int> > replicas;
  replicas.push_back(std::pair<int, int>(0, 2)); // query key, node id (last octet of rpc_address)
  replicas.push_back(std::pair<int, int>(1, 2));
  replicas.push_back(std::pair<int, int>(2, 2));
  replicas.push_back(std::pair<int, int>(3, 1));
  replicas.push_back(std::pair<int, int>(4, 3));
  replicas.push_back(std::pair<int, int>(5, 2));

  Cluster cluster = default_cluster(false);
  cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(cluster.get(),
                                                                  creds_v1().c_str());
  Session session = cluster.connect();
  for (std::vector<std::pair<int, int> >::iterator it = replicas.begin(), end = replicas.end();
       it != end; ++it) {
    Statement statement(SELECT_ALL_SYSTEM_LOCAL_CQL, 1);
    statement.set_consistency(CASS_CONSISTENCY_ONE);
    statement.add_key_index(0);
    statement.set_keyspace("system");
    statement.bind<Integer>(0, Integer(it->first));

    Result result =
        session.execute(statement, false); // No bind variables exist so statement will return error
    EXPECT_EQ(server_names[it->second], result.server_name());
  }
}

/**
 * Attempt connection to DBaaS SNI single endpoint docker image manually setting auth.
 *
 * This test will perform a connection to a DBaaS SNI single endpoint while ensuring proper
 * automatic cloud configuration with address resolution where the authentication is not available.
 *
 * @jira_ticket CPP-787
 * @test_category dbaas:auth
 * @since 2.14.0
 * @expected_result Successful address resolution and connection.
 */
CASSANDRA_INTEGRATION_TEST_F(DbaasTests, ResolveAndConnectWithoutCredsInBundle) {
  CHECK_FAILURE;

  Cluster cluster = default_cluster(false);
  cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(cluster.get(),
                                                                  creds_v1_no_creds().c_str());
  cluster.with_credentials("cassandra", "cassandra");
  cluster.connect();
}

/**
 * Attempt connection to DBaaS SNI single endpoint docker image leaving auth unset.
 *
 * This test will perform a connection to a DBaaS SNI single endpoint while ensuring proper
 * automatic cloud configuration with address resolution where the authentication is not set.
 *
 * @jira_ticket CPP-787
 * @test_category dbaas
 * @since 2.14.0
 * @expected_result Failed to establish a connection.
 */
CASSANDRA_INTEGRATION_TEST_F(DbaasTests, InvalidWithoutCreds) {
  CHECK_FAILURE;

  Cluster cluster = default_cluster(false);
  cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(cluster.get(),
                                                                  creds_v1_no_creds().c_str());
  try {
    cluster.connect();
    EXPECT_TRUE(false) << "Connection established";
  } catch (Session::Exception& se) {
    EXPECT_EQ(CASS_ERROR_SERVER_BAD_CREDENTIALS, se.error_code());
  }
}

/**
 * Attempt connection to DBaaS SNI single endpoint docker image using invalid metadata server.
 *
 * This test will attempt a connection to a DBaaS SNI single endpoint using an invalid metadata
 * server. The connection should not succeed as no resolution will be possible.
 *
 * @jira_ticket CPP-787
 * @test_category dbaas
 * @since 2.14.0
 * @expected_result Failed to establish a connection.
 */
CASSANDRA_INTEGRATION_TEST_F(DbaasTests, InvalidMetadataServer) {
  CHECK_FAILURE;

  Cluster cluster = default_cluster(false);
  cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(cluster.get(),
                                                                  creds_v1_unreachable().c_str());
  try {
    cluster.connect();
    EXPECT_TRUE(false) << "Connection established";
  } catch (Session::Exception& se) {
    EXPECT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, se.error_code());
  }
}

/**
 * Attempt connection to DBaaS SNI single endpoint docker image using invalid certificate.
 *
 * This test will attempt a connection to a DBaaS SNI single endpoint using an invalid certificate.
 * The connection should not succeed as no resolution will be possible.
 *
 * @jira_ticket CPP-787
 * @test_category dbaas
 * @since 2.14.0
 * @expected_result Failed to establish a connection.
 */
CASSANDRA_INTEGRATION_TEST_F(DbaasTests, InvalidCertificate) {
  CHECK_FAILURE;

  Cluster cluster = default_cluster(false);
  cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(cluster.get(),
                                                                  creds_v1_no_cert().c_str());
  try {
    cluster.connect();
    EXPECT_TRUE(false) << "Connection established";
  } catch (Session::Exception& se) {
    EXPECT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, se.error_code());
  }
}

/**
 * Attempt connection to DBaaS SNI single endpoint docker image using invalid CA.
 *
 * This test will attempt a connection to a DBaaS SNI single endpoint using an invalid CA. The
 * connection should not succeed as no resolution will be possible.
 *
 * @jira_ticket CPP-787
 * @test_category dbaas
 * @since 2.14.0
 * @expected_result Failed to establish a connection.
 */
CASSANDRA_INTEGRATION_TEST_F(DbaasTests, InvalidCertificateAuthority) {
  CHECK_FAILURE;

  Cluster cluster = default_cluster(false);
  cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(cluster.get(),
                                                                  creds_v1_invalid_ca().c_str());
  try {
    cluster.connect();
    EXPECT_TRUE(false) << "Connection established";
  } catch (Session::Exception& se) {
    EXPECT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, se.error_code());
  }
}

/**
 * Perform query with nodes down against the DBaaS SNI single endpoint docker image.
 *
 * This test will perform a connection and execute a simple statement query against the
 * system.local table to ensure query execution to a DBaaS SNI single endpoint while validating the
 * results.
 *
 * @jira_ticket CPP-787
 * @test_category dbaas
 * @test_category queries
 * @since 2.14.0
 * @expected_result Simple statement is executed and validated while node(s) are down.
 */
CASSANDRA_INTEGRATION_TEST_F(DbaasTests, QueryWithNodesDown) {
  CHECK_FAILURE;

  ServerNames server_names = get_server_names();

  Cluster cluster = default_cluster(false);
  cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(cluster.get(),
                                                                  creds_v1().c_str());
  Session session = cluster.connect();

  EXPECT_TRUE(stop_node(1));
  for (int i = 0; i < 8; ++i) {
    EXPECT_NE(server_names[1], session.execute(SELECT_ALL_SYSTEM_LOCAL_CQL).server_name());
  }

  EXPECT_TRUE(stop_node(3));
  for (int i = 0; i < 8; ++i) {
    EXPECT_EQ(server_names[2], session.execute(SELECT_ALL_SYSTEM_LOCAL_CQL).server_name());
  }

  EXPECT_TRUE(start_cluster());
}

/**
 * Ensure reconnection occurs during full outage.
 *
 * This test will perform a connection, full outage will occur and the the cluster will be restarted
 * while executing a simple statement query against the system.local table to ensure reconnection
 * after full outage.
 *
 * @jira_ticket CPP-787
 * @test_category dbaas
 * @test_category queries
 * @since 2.14.0
 * @expected_result Simple statement is executed and validated after full outage.
 */
CASSANDRA_INTEGRATION_TEST_F(DbaasTests, FullOutage) {
  CHECK_FAILURE;

  ServerNames server_names = get_server_names();

  Cluster cluster = default_cluster(false).with_constant_reconnect(10); // Quick reconnect
  cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(cluster.get(),
                                                                  creds_v1().c_str());
  Session session = cluster.connect();

  EXPECT_TRUE(stop_cluster());

  Statement statement(SELECT_ALL_SYSTEM_LOCAL_CQL);
  EXPECT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, session.execute(statement, false).error_code());

  EXPECT_TRUE(start_cluster());
  EXPECT_EQ(CASS_OK, session.execute(statement).error_code());
}
