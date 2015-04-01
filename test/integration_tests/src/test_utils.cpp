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

#include "test_utils.hpp"

#include "cassandra.h"
#include "cql_ccm_bridge.hpp"
#include "testing.hpp"

#include <boost/test/test_tools.hpp>
#include <boost/test/debug.hpp>
#include <boost/thread.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/format.hpp>

#include <assert.h>
#include <fstream>
#include <vector>
#include <cstdlib>

namespace test_utils {
//-----------------------------------------------------------------------------------

const cass_duration_t ONE_MILLISECOND_IN_MICROS = 1000;
const cass_duration_t ONE_SECOND_IN_MICROS = 1000 * ONE_MILLISECOND_IN_MICROS;

const char* CREATE_TABLE_ALL_TYPES =
    "CREATE TABLE %s ("
    "id uuid PRIMARY KEY,"
    "text_sample text,"
    "int_sample int,"
    "bigint_sample bigint,"
    "float_sample float,"
    "double_sample double,"
    "decimal_sample decimal,"
    "blob_sample blob,"
    "boolean_sample boolean,"
    "timestamp_sample timestamp,"
    "inet_sample inet);";

const char* CREATE_TABLE_TIME_SERIES =
    "CREATE TABLE %s ("
    "id uuid,"
    "event_time timestamp,"
    "text_sample text,"
    "int_sample int,"
    "bigint_sample bigint,"
    "float_sample float,"
    "double_sample double,"
    "decimal_sample decimal,"
    "blob_sample blob,"
    "boolean_sample boolean,"
    "timestamp_sample timestamp,"
    "inet_sample inet,"
    "PRIMARY KEY(id, event_time));";

const char* CREATE_TABLE_SIMPLE =
    "CREATE TABLE %s ("
    "id int PRIMARY KEY,"
    "test_val text);";

CassLog::LogData CassLog::log_data_;

size_t CassLog::message_count() {
  while (!cass::is_log_flushed()) {
    boost::this_thread::sleep_for(boost::chrono::milliseconds(500));
  }
  return log_data_.message_count;
}

void CassLog::callback(const CassLogMessage* message, void* data) {
  LogData* log_data = reinterpret_cast<LogData*>(data);
  std::string str(message->message);
  if (message->severity <= log_data->output_log_level) {
    fprintf(stderr, "CassLog: %u.%03u [%s] (%s:%d:%s): %s\n",
            static_cast<unsigned int>(message->time_ms / 1000),
            static_cast<unsigned int>(message->time_ms % 1000),
            cass_log_level_string(message->severity),
            message->file, message->line, message->function,
            message->message);
  }
  boost::lock_guard<LogData> l(*log_data);
  if (log_data->message.empty()) return;
  if (str.find(log_data->message) != std::string::npos) {
    log_data->message_count++;
  }
}

const char* get_value_type(CassValueType type) {
  switch (type) {
    case CASS_VALUE_TYPE_CUSTOM: return "custom";
    case CASS_VALUE_TYPE_ASCII: return "ascii";
    case CASS_VALUE_TYPE_BIGINT: return "bigint";
    case CASS_VALUE_TYPE_BLOB: return "blob";
    case CASS_VALUE_TYPE_BOOLEAN: return "boolean";
    case CASS_VALUE_TYPE_COUNTER: return "counter";
    case CASS_VALUE_TYPE_DECIMAL: return "decimal";
    case CASS_VALUE_TYPE_DOUBLE: return "double";
    case CASS_VALUE_TYPE_FLOAT: return "float";
    case CASS_VALUE_TYPE_INT: return "int";
    case CASS_VALUE_TYPE_TEXT: return "text";
    case CASS_VALUE_TYPE_TIMESTAMP: return "timestamp";
    case CASS_VALUE_TYPE_UUID: return "uuid";
    case CASS_VALUE_TYPE_VARCHAR: return "varchar";
    case CASS_VALUE_TYPE_VARINT: return "varint";
    case CASS_VALUE_TYPE_TIMEUUID: return "timeuuid";
    case CASS_VALUE_TYPE_INET: return "inet";
    case CASS_VALUE_TYPE_LIST: return "list";
    case CASS_VALUE_TYPE_MAP: return "map";
    case CASS_VALUE_TYPE_SET: return "set";
    default:
      assert(false && "Invalid value type");
      return "";
  }
}

//-----------------------------------------------------------------------------------
const std::string CREATE_KEYSPACE_SIMPLE_FORMAT = "CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %s }";
const std::string CREATE_KEYSPACE_NETWORK_FORMAT = "CREATE KEYSPACE %s WITH replication = { 'class' : 'NetworkTopologyStrategy',  'dc1' : %d, 'dc2' : %d }";
const std::string CREATE_KEYSPACE_GENERIC_FORMAT = "CREATE KEYSPACE {0} WITH replication = { 'class' : '{1}', {2} }";
const std::string DROP_KEYSPACE_FORMAT = "DROP KEYSPACE %s";
const std::string DROP_KEYSPACE_IF_EXISTS_FORMAT = "DROP KEYSPACE IF EXISTS %s";
const std::string SIMPLE_KEYSPACE = "ks";
const std::string NUMERIC_KEYSPACE_FORMAT = "ks%d";
const std::string SIMPLE_TABLE = "test";
const std::string CREATE_TABLE_SIMPLE_FORMAT = "CREATE TABLE {0} (k text PRIMARY KEY, t text, i int, f float)";
const std::string INSERT_FORMAT = "INSERT INTO {0} (k, t, i, f) VALUES ('{1}', '{2}', {3}, {4})";
const std::string SELECT_ALL_FORMAT = "SELECT * FROM {0}";
const std::string SELECT_WHERE_FORMAT = "SELECT * FROM {0} WHERE {1}";
const std::string SELECT_VERSION = "SELECT release_version FROM system.local";

const std::string lorem_ipsum = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla porta turpis vel dui venenatis, quis viverra magna"
                                "suscipit. Praesent pharetra facilisis turpis, et fermentum leo sollicitudin sit amet. In hac habitasse platea dictumst. Donec mattis facilisis"
                                "diam, nec pulvinar ligula. Sed eget faucibus magna. Donec vitae fermentum augue. Ut nec accumsan ligula. Sed a viverra leo, sed semper augue."
                                "Pellentesque auctor nisl varius, imperdiet est non, porttitor risus. Donec aliquam elementum sollicitudin. Maecenas ultrices mattis mauris,"
                                "fringilla congue nunc sodales sed. Fusce ac neque quis erat hendrerit porta at nec massa. Maecenas blandit ut felis sed ultrices. Sed fermentum"
                                "pharetra lacus sodales cursus.";
const char ALPHA_NUMERIC[] = { "01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" };

//-----------------------------------------------------------------------------------

MultipleNodesTest::MultipleNodesTest(unsigned int num_nodes_dc1, unsigned int num_nodes_dc2, unsigned int protocol_version, bool isSSL /* = false */)
  : conf(cql::get_ccm_bridge_configuration()) {
  boost::debug::detect_memory_leaks(false);
  ccm = cql::cql_ccm_bridge_t::create_and_start(conf, "test", num_nodes_dc1, num_nodes_dc2, isSSL);

  uuid_gen = cass_uuid_gen_new();
  cluster = cass_cluster_new();
  initialize_contact_points(cluster, conf.ip_prefix(), num_nodes_dc1, num_nodes_dc2);

  cass_cluster_set_connect_timeout(cluster, 10 * ONE_SECOND_IN_MICROS);
  cass_cluster_set_request_timeout(cluster, 30 * ONE_SECOND_IN_MICROS);
  cass_cluster_set_core_connections_per_host(cluster, 2);
  cass_cluster_set_max_connections_per_host(cluster, 4);
  cass_cluster_set_num_threads_io(cluster, 4);
  cass_cluster_set_max_concurrent_creation(cluster, 8);
  cass_cluster_set_protocol_version(cluster, protocol_version);
}

MultipleNodesTest::~MultipleNodesTest() {
  cass_uuid_gen_free(uuid_gen);
  cass_cluster_free(cluster);
}

SingleSessionTest::SingleSessionTest(unsigned int num_nodes_dc1, unsigned int num_nodes_dc2, unsigned int protocol_version, bool isSSL /* = false */)
  : MultipleNodesTest(num_nodes_dc1, num_nodes_dc2, protocol_version, isSSL), session(NULL), ssl(NULL) {
  //SSL verification flags must be set before establishing session
  if (!isSSL) {
    create_session();
  } else {
    ssl = cass_ssl_new();
  }
}

void SingleSessionTest::create_session() {
  session = cass_session_new();
  test_utils::CassFuturePtr connect_future(cass_session_connect(session, cluster));
  test_utils::wait_and_check_error(connect_future.get());
  version = get_version(session);
}

SingleSessionTest::~SingleSessionTest() {
  if (session) {
    CassFuturePtr close_future(cass_session_close(session));
    cass_future_wait(close_future.get());
  }
  if (ssl) {
    cass_ssl_free(ssl);
  }
}

void initialize_contact_points(CassCluster* cluster, std::string prefix, unsigned int num_nodes_dc1, unsigned int num_nodes_dc2) {
  for (unsigned int i = 0; i < num_nodes_dc1; ++i) {
    std::string contact_point(prefix + boost::lexical_cast<std::string>(i + 1));
    cass_cluster_set_contact_points(cluster, contact_point.c_str());
  }
}

CassSessionPtr create_session(CassCluster* cluster, cass_duration_t timeout) {
  test_utils::CassSessionPtr session(cass_session_new());
  test_utils::CassFuturePtr connect_future(cass_session_connect(session.get(), cluster));
  test_utils::wait_and_check_error(connect_future.get(), timeout);
  return session;
}

CassSessionPtr create_session(CassCluster* cluster, CassError* code, cass_duration_t timeout) {
  test_utils::CassSessionPtr session(cass_session_new());
  test_utils::CassFuturePtr connect_future(cass_session_connect(session.get(), cluster));
  if (code) {
    *code = test_utils::wait_and_return_error(connect_future.get(), timeout);
  }
  return session;
}

void execute_query(CassSession* session,
                   const std::string& query,
                   CassResultPtr* result,
                   CassConsistency consistency,
                   cass_duration_t timeout) {
  CassStatementPtr statement(cass_statement_new(query.c_str(), 0));
  cass_statement_set_consistency(statement.get(), consistency);
  CassFuturePtr future(cass_session_execute(session, statement.get()));
  wait_and_check_error(future.get(), timeout);
  if (result != NULL) {
    *result = CassResultPtr(cass_future_get_result(future.get()));
  }
}

CassError execute_query_with_error(CassSession* session,
                                   const std::string& query,
                                   CassResultPtr* result,
                                   CassConsistency consistency,
                                   cass_duration_t timeout) {
  CassStatementPtr statement(cass_statement_new(query.c_str(), 0));
  cass_statement_set_consistency(statement.get(), consistency);
  CassFuturePtr future(cass_session_execute(session, statement.get()));
  CassError code = wait_and_return_error(future.get(), timeout);
  if(result != NULL) {
    *result = CassResultPtr(cass_future_get_result(future.get()));
  }
  return code;
}

CassError wait_and_return_error(CassFuture* future, cass_duration_t timeout) {
  if (!cass_future_wait_timed(future, timeout)) {
    BOOST_FAIL("Timed out waiting for result");
  }
  return cass_future_error_code(future);
}

void wait_and_check_error(CassFuture* future, cass_duration_t timeout) {
  CassError code = wait_and_return_error(future, timeout);
  if (code != CASS_OK) {
    CassString message;
    cass_future_error_message(future, &message.data, &message.length);
    BOOST_FAIL("Error occurred during query '" << std::string(message.data, message.length) << "' (" << boost::format("0x%08X") % code << ")");
  }
}

std::string string_from_time_point(boost::chrono::system_clock::time_point time) {
  std::time_t t = boost::chrono::system_clock::to_time_t(time);
  char buffer[26];
#ifdef WIN32
  ctime_s(buffer, sizeof(buffer), &t);
#else
  ctime_r(&t, buffer);
#endif
  return std::string(buffer, 24);
}

std::string string_from_uuid(CassUuid uuid) {
  char buffer[CASS_UUID_STRING_LENGTH];
  cass_uuid_string(uuid, buffer);
  return std::string(buffer);
}

CassVersion get_version(CassSession* session) {
  // Execute the version query
  CassResultPtr result;
  execute_query(session, SELECT_VERSION, &result);

  // Only one row should be returned; get the first row
  const CassRow *row = cass_result_first_row(result.get());

  // Convert the release_version value to a string
  const CassValue* value = cass_row_get_column_by_name(row, "release_version");
  CassString version_string;
  cass_value_get_string(value, &version_string.data, &version_string.length);

  // Parse the version string and return the Cassandra version
  CassVersion version;
  std::string str(version_string.data, version_string.length); // Needed for null termination
  sscanf(str.c_str(), "%hu.%hu.%hu-%s", &version.major, &version.minor, &version.patch, version.extra);
  return version;
}

std::string generate_random_string(unsigned int size /* = 1024 */) {
  std::string randomString;

  unsigned int stringLength = strlen(ALPHA_NUMERIC) - 1;
  for (unsigned int n = 0; n < size; ++n) {
    randomString += ALPHA_NUMERIC[rand() % stringLength];
  }

  return randomString;
}

std::string load_ssl_certificate(const std::string filename) {
  // Open the file
  std::ifstream file_stream(filename.c_str(), std::ios::in | std::ios::binary | std::ios::ate);

  BOOST_REQUIRE_MESSAGE(file_stream.is_open(), "Unable to load certificate file: " << filename);

  // Get the length of the file
  std::ifstream::pos_type file_size = file_stream.tellg();
  file_stream.seekg(0, std::ios::beg);

  BOOST_REQUIRE_MESSAGE(file_size > 0, "No data in certificate file: " << filename);

  // Read the file into memory
  std::vector<char> bytes(file_size);
  file_stream.read(&bytes[0], file_size);

  std::string certificate(&bytes[0], file_size);
  return certificate;
}

//-----------------------------------------------------------------------------------
} // End of namespace test_utils
