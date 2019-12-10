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
    is_schema_metadata_ = true; // Needed for prepared statements
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
      EXPECT_EQ(CASS_OK, cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(
                             cluster.get(), creds_v1().c_str()));
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
    args.push_back("--jvm_arg=-Ddse.product_type=DATASTAX_APOLLO");
    return ccm_execute(args);
  }

  bool stop_cluster() {
    Process::Args args;
    args.push_back("stop");
    return ccm_execute(args);
  }

  bool start_node(unsigned int node) {
    Process::Args args;
    args.push_back(node_name(node));
    args.push_back("start");
    args.push_back("--root");
    args.push_back("--wait-for-binary-proto");
    args.push_back("--jvm_arg=-Ddse.product_type=DATASTAX_APOLLO");
    return ccm_execute(args);
  }

  bool stop_node(unsigned int node, bool is_kill = false) {
    Process::Args args;
    args.push_back(node_name(node));
    args.push_back("stop");
    if (is_kill) {
      args.push_back("--not-gently");
    }
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
  ASSERT_EQ(CASS_OK, cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(
                         cluster.get(), creds_v1().c_str()));
  connect(cluster);
}

/**
 * Perform query using a simple statement against the DBaaS SNI single endpoint docker image.
 *
 * This test will perform a connection and execute a simple statement query against the
 * system.local table to ensure query execution to a DBaaS SNI single endpoint while validating the
 * results.
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
  ASSERT_EQ(CASS_OK, cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(
                         cluster.get(), creds_v1().c_str()));
  connect(cluster);

  ServerNames server_names;
  for (int i = 0; i < 3; ++i) {
    Result result = session_.execute(SELECT_ALL_SYSTEM_LOCAL_CQL);
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
 * Create function and aggregate definitions and ensure the schema metadata is reflected when
 * execute against the DBaaS SNI single endpoint docker image.
 *
 * This test will perform a connection and execute create function/aggregate queries to ensure
 * schema metadata using a DBaaS SNI single endpoint is handled properly.
 *
 * @jira_ticket CPP-815
 * @test_category dbaas
 * @test_category queries:schema_metadata:udf
 * @since 2.14.0
 * @expected_result Function/Aggregate definitions schema metadata are validated.
 */
CASSANDRA_INTEGRATION_TEST_F(DbaasTests, SchemaMetadata) {
  CHECK_FAILURE;

  Cluster cluster = default_cluster(false);
  ASSERT_EQ(CASS_OK, cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(
                         cluster.get(), creds_v1().c_str()));
  connect(cluster);

  // clang-format off
  session_.execute("CREATE OR REPLACE FUNCTION avg_state(state tuple<int, bigint>, val int) "
                   "CALLED ON NULL INPUT RETURNS tuple<int, bigint> "
                   "LANGUAGE java AS "
                     "'if (val != null) {"
                       "state.setInt(0, state.getInt(0) + 1);"
                       "state.setLong(1, state.getLong(1) + val.intValue());"
                     "};"
                     "return state;'"
                   ";");
  session_.execute("CREATE OR REPLACE FUNCTION avg_final (state tuple<int, bigint>) "
                   "CALLED ON NULL INPUT RETURNS double "
                   "LANGUAGE java AS "
                     "'double r = 0;"
                     "if (state.getInt(0) == 0) return null;"
                     "r = state.getLong(1);"
                     "r /= state.getInt(0);"
                     "return Double.valueOf(r);'"
                   ";");
  session_.execute("CREATE OR REPLACE AGGREGATE average(int) "
                   "SFUNC avg_state STYPE tuple<int, bigint> FINALFUNC avg_final "
                   "INITCOND(0, 0);");
  // clang-format on

  const CassSchemaMeta* schema_meta = cass_session_get_schema_meta(session_.get());
  ASSERT_TRUE(schema_meta != NULL);
  const CassKeyspaceMeta* keyspace_meta =
      cass_schema_meta_keyspace_by_name(schema_meta, default_keyspace().c_str());
  ASSERT_TRUE(keyspace_meta != NULL);

  { // Function `avg_state`
    const char* data = NULL;
    size_t length = 0;
    const CassDataType* datatype = NULL;

    const CassFunctionMeta* function_meta =
        cass_keyspace_meta_function_by_name(keyspace_meta, "avg_state", "tuple<int,bigint>,int");
    ASSERT_TRUE(function_meta != NULL);
    cass_function_meta_name(function_meta, &data, &length);
    EXPECT_EQ("avg_state", std::string(data, length));
    cass_function_meta_full_name(function_meta, &data, &length);
    EXPECT_EQ("avg_state(tuple<int,bigint>,int)", std::string(data, length));
    cass_function_meta_body(function_meta, &data, &length);
    EXPECT_EQ("if (val != null) {state.setInt(0, state.getInt(0) + 1);state.setLong(1, "
              "state.getLong(1) + val.intValue());};return state;",
              std::string(data, length));
    cass_function_meta_language(function_meta, &data, &length);
    EXPECT_EQ("java", std::string(data, length));
    EXPECT_TRUE(cass_function_meta_called_on_null_input(function_meta));
    ASSERT_EQ(2u, cass_function_meta_argument_count(function_meta));
    cass_function_meta_argument(function_meta, 0, &data, &length, &datatype);
    EXPECT_EQ("state", std::string(data, length));
    EXPECT_EQ(CASS_VALUE_TYPE_TUPLE, cass_data_type_type(datatype));
    ASSERT_EQ(2u, cass_data_type_sub_type_count(datatype));
    EXPECT_EQ(CASS_VALUE_TYPE_INT, cass_data_type_type(cass_data_type_sub_data_type(datatype, 0)));
    EXPECT_EQ(CASS_VALUE_TYPE_BIGINT,
              cass_data_type_type(cass_data_type_sub_data_type(datatype, 1)));
    cass_function_meta_argument(function_meta, 1, &data, &length, &datatype);
    EXPECT_EQ("val", std::string(data, length));
    EXPECT_EQ(CASS_VALUE_TYPE_INT, cass_data_type_type(datatype));
    datatype = cass_function_meta_argument_type_by_name(function_meta, "state");
    EXPECT_EQ(CASS_VALUE_TYPE_TUPLE, cass_data_type_type(datatype));
    ASSERT_EQ(2u, cass_data_type_sub_type_count(datatype));
    EXPECT_EQ(CASS_VALUE_TYPE_INT, cass_data_type_type(cass_data_type_sub_data_type(datatype, 0)));
    EXPECT_EQ(CASS_VALUE_TYPE_BIGINT,
              cass_data_type_type(cass_data_type_sub_data_type(datatype, 1)));
    datatype = cass_function_meta_argument_type_by_name(function_meta, "val");
    EXPECT_EQ(CASS_VALUE_TYPE_INT, cass_data_type_type(datatype));
    datatype = cass_function_meta_return_type(function_meta);
    EXPECT_EQ(CASS_VALUE_TYPE_TUPLE, cass_data_type_type(datatype));
    ASSERT_EQ(2u, cass_data_type_sub_type_count(datatype));
    EXPECT_EQ(CASS_VALUE_TYPE_INT, cass_data_type_type(cass_data_type_sub_data_type(datatype, 0)));
    EXPECT_EQ(CASS_VALUE_TYPE_BIGINT,
              cass_data_type_type(cass_data_type_sub_data_type(datatype, 1)));
  }

  { // Aggregate `average`
    const char* data = NULL;
    size_t length = 0;
    const CassDataType* datatype = NULL;

    const CassAggregateMeta* aggregate_meta =
        cass_keyspace_meta_aggregate_by_name(keyspace_meta, "average", "int");
    ASSERT_TRUE(aggregate_meta != NULL);
    cass_aggregate_meta_name(aggregate_meta, &data, &length);
    EXPECT_EQ("average", std::string(data, length));
    cass_aggregate_meta_full_name(aggregate_meta, &data, &length);
    EXPECT_EQ("average(int)", std::string(data, length));
    ASSERT_EQ(1u, cass_aggregate_meta_argument_count(aggregate_meta));
    datatype = cass_aggregate_meta_argument_type(aggregate_meta, 0);
    EXPECT_EQ(CASS_VALUE_TYPE_INT, cass_data_type_type(datatype));
    datatype = cass_aggregate_meta_return_type(aggregate_meta);
    EXPECT_EQ(CASS_VALUE_TYPE_DOUBLE, cass_data_type_type(datatype));
    datatype = cass_aggregate_meta_state_type(aggregate_meta);
    EXPECT_EQ(CASS_VALUE_TYPE_TUPLE, cass_data_type_type(datatype));
    ASSERT_EQ(2u, cass_data_type_sub_type_count(datatype));
    EXPECT_EQ(CASS_VALUE_TYPE_INT, cass_data_type_type(cass_data_type_sub_data_type(datatype, 0)));
    EXPECT_EQ(CASS_VALUE_TYPE_BIGINT,
              cass_data_type_type(cass_data_type_sub_data_type(datatype, 1)));
    const CassFunctionMeta* function_meta = cass_aggregate_meta_state_func(aggregate_meta);
    cass_function_meta_name(function_meta, &data, &length);
    EXPECT_EQ("avg_state", std::string(data, length));
    function_meta = cass_aggregate_meta_final_func(aggregate_meta);
    cass_function_meta_name(function_meta, &data, &length);
    EXPECT_EQ("avg_final", std::string(data, length));
    const CassValue* initcond = cass_aggregate_meta_init_cond(aggregate_meta);
    EXPECT_EQ(CASS_VALUE_TYPE_VARCHAR, cass_value_type(initcond));
    EXPECT_EQ(Text("(0, 0)"), Text(initcond));
    ASSERT_TRUE(true);
  }

  cass_schema_meta_free(schema_meta);
}

/**
 * Ensure guardrails are enabled when performing a query against the DBaaS SNI single endpoint
 * docker image.
 *
 * This test will perform a connection and execute a simple insert statement query against the
 * server using a valid consistency level.DBaaS SNI single endpoint while validating the
 * insert occured.
 *
 * @jira_ticket CPP-813
 * @test_category dbaas
 * @test_category queries:guard_rails
 * @since 2.14.0
 * @expected_result Simple statement is executed and is validated.
 */
CASSANDRA_INTEGRATION_TEST_F(DbaasTests, ConsistencyGuardrails) {
  CHECK_FAILURE;

  Cluster cluster = default_cluster(false);
  ASSERT_EQ(CASS_OK, cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(
                         cluster.get(), creds_v1().c_str()));
  connect(cluster);

  session_.execute(
      format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, default_table().c_str(), "int", "int"));
  CHECK_FAILURE;

  session_.execute(Statement(
      format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, default_table().c_str(), "0", "1")));
  Result result = session_.execute(
      Statement(format_string(CASSANDRA_SELECT_VALUE_FORMAT, default_table().c_str(), "0")));
  EXPECT_EQ(1u, result.row_count());
  ASSERT_EQ(1u, result.column_count());
  ASSERT_EQ(Integer(1), result.first_row().next().as<Integer>());
}

/**
 * Ensure guardrails are enabled when performing a query against the DBaaS SNI single endpoint
 * docker image.
 *
 * This test will perform a connection and execute a simple statement query against the
 * server using an invalid consistency level.DBaaS SNI single endpoint while validating the
 * error.
 *
 * @jira_ticket CPP-813
 * @test_category dbaas
 * @test_category queries:guard_rails
 * @since 2.14.0
 * @expected_result Simple statement is executed and guard rail error is validated.
 */
CASSANDRA_INTEGRATION_TEST_F(DbaasTests, ConsistencyGuardrailsInvalid) {
  CHECK_FAILURE;

  Cluster cluster = default_cluster(false);
  ASSERT_EQ(CASS_OK, cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(
                         cluster.get(), creds_v1().c_str()));
  connect(cluster);

  session_.execute(
      format_string(CASSANDRA_KEY_VALUE_TABLE_FORMAT, default_table().c_str(), "int", "int"));
  CHECK_FAILURE

  Statement statement(
      format_string(CASSANDRA_KEY_VALUE_INSERT_FORMAT, default_table().c_str(), "0", "1"));
  statement.set_consistency(
      CASS_CONSISTENCY_LOCAL_ONE); // Override default DBaaS configured consistency
  Result result = session_.execute(statement, false);
  EXPECT_TRUE(result.error_code() != CASS_OK)
      << "Statement execution succeeded; guardrails may not be enabled";
  EXPECT_TRUE(contains(result.error_message(),
                       "Provided value LOCAL_ONE is not allowed for Write Consistency Level"));
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
  ASSERT_EQ(CASS_OK, cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(
                         cluster.get(), creds_v1().c_str()));
  connect(cluster);

  for (std::vector<std::pair<int, int> >::iterator it = replicas.begin(), end = replicas.end();
       it != end; ++it) {
    Statement statement(SELECT_ALL_SYSTEM_LOCAL_CQL, 1);
    statement.set_consistency(CASS_CONSISTENCY_ONE);
    statement.add_key_index(0);
    statement.set_keyspace("system");
    statement.bind<Integer>(0, Integer(it->first));

    Result result = session_.execute(
        statement, false); // No bind variables exist so statement will return error
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
  ASSERT_EQ(CASS_OK, cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(
                         cluster.get(), creds_v1_no_creds().c_str()));
  cluster.with_credentials("cassandra", "cassandra");
  connect(cluster);
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
  ASSERT_EQ(CASS_OK, cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(
                         cluster.get(), creds_v1_no_creds().c_str()));
  try {
    connect(cluster);
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
  EXPECT_EQ(CASS_OK, cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(
                         cluster.get(), creds_v1_unreachable().c_str()));
  try {
    connect(cluster);
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
  EXPECT_EQ(CASS_ERROR_LIB_BAD_PARAMS,
            cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(
                cluster.get(), creds_v1_no_cert().c_str()));
  try {
    connect(cluster);
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
  ASSERT_EQ(CASS_OK, cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(
                         cluster.get(), creds_v1_invalid_ca().c_str()));
  try {
    connect(cluster);
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
  ASSERT_EQ(CASS_OK, cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(
                         cluster.get(), creds_v1().c_str()));
  connect(cluster);

  EXPECT_TRUE(stop_node(1));
  for (int i = 0; i < 8; ++i) {
    EXPECT_NE(server_names[1], session_.execute(SELECT_ALL_SYSTEM_LOCAL_CQL).server_name());
  }

  EXPECT_TRUE(stop_node(3));
  for (int i = 0; i < 8; ++i) {
    EXPECT_EQ(server_names[2], session_.execute(SELECT_ALL_SYSTEM_LOCAL_CQL).server_name());
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
  ASSERT_EQ(CASS_OK, cass_cluster_set_cloud_secure_connection_bundle_no_ssl_lib_init(
                         cluster.get(), creds_v1().c_str()));
  connect(cluster);

  EXPECT_TRUE(stop_cluster());

  Statement statement(SELECT_ALL_SYSTEM_LOCAL_CQL);
  EXPECT_EQ(CASS_ERROR_LIB_NO_HOSTS_AVAILABLE, session_.execute(statement, false).error_code());

  EXPECT_TRUE(start_cluster());
  EXPECT_EQ(CASS_OK, session_.execute(statement).error_code());
}
