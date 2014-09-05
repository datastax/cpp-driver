/*
  Copyright (c) 2014 DataStax

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

#define BOOST_TEST_DYN_LINK

#include <assert.h>

#include <boost/test/test_tools.hpp>
#include <boost/test/debug.hpp>
#include <boost/thread.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/format.hpp>

#include "cassandra.h"
#include "cql_ccm_bridge.hpp"
#include "test_utils.hpp"

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

void count_message_log_callback(cass_uint64_t time,
                                CassLogLevel severity,
                                CassString message,
                                void* data) {
  LogData* log_data = reinterpret_cast<LogData*>(data);
  std::string str(message.data, message.length);
  if (log_data->output_messages) {
    fprintf(stderr, "Log: %s\n", str.c_str());
  }
  if (str.find(log_data->message) != std::string::npos) {
    log_data->message_count++;
  }
}

const char* get_value_type(CassValueType type) {
  switch(type) {
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
const std::string SIMPLE_KEYSPACE = "ks";
const std::string SIMPLE_TABLE = "test";
const std::string CREATE_TABLE_SIMPLE_FORMAT = "CREATE TABLE {0} (k text PRIMARY KEY, t text, i int, f float)";
const std::string INSERT_FORMAT = "INSERT INTO {0} (k, t, i, f) VALUES ('{1}', '{2}', {3}, {4})";
const std::string SELECT_ALL_FORMAT = "SELECT * FROM {0}";
const std::string SELECT_WHERE_FORMAT = "SELECT * FROM {0} WHERE {1}";

const std::string lorem_ipsum = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla porta turpis vel dui venenatis, quis viverra magna"
                                "suscipit. Praesent pharetra facilisis turpis, et fermentum leo sollicitudin sit amet. In hac habitasse platea dictumst. Donec mattis facilisis"
                                "diam, nec pulvinar ligula. Sed eget faucibus magna. Donec vitae fermentum augue. Ut nec accumsan ligula. Sed a viverra leo, sed semper augue."
                                "Pellentesque auctor nisl varius, imperdiet est non, porttitor risus. Donec aliquam elementum sollicitudin. Maecenas ultrices mattis mauris,"
                                "fringilla congue nunc sodales sed. Fusce ac neque quis erat hendrerit porta at nec massa. Maecenas blandit ut felis sed ultrices. Sed fermentum"
                                "pharetra lacus sodales cursus.";

//-----------------------------------------------------------------------------------

MultipleNodesTest::MultipleNodesTest(int num_nodes_dc1, int num_nodes_dc2, int protocol_version)
  : conf(cql::get_ccm_bridge_configuration()) {
  boost::debug::detect_memory_leaks(false);
  ccm = cql::cql_ccm_bridge_t::create(conf, "test", num_nodes_dc1, num_nodes_dc2);

  cluster = cass_cluster_new();
  initialize_contact_points(cluster, conf.ip_prefix(), num_nodes_dc1, num_nodes_dc2);

  cass_cluster_set_connect_timeout(cluster, 10000);
  cass_cluster_set_request_timeout(cluster, 10000);
  cass_cluster_set_num_threads_io(cluster, 2);
  cass_cluster_set_protocol_version(cluster, protocol_version);
}

MultipleNodesTest::~MultipleNodesTest() {
  cass_cluster_free(cluster);
}

SingleSessionTest::SingleSessionTest(int num_nodes_dc1, int num_nodes_dc2, int protocol_version)
  : MultipleNodesTest(num_nodes_dc1, num_nodes_dc2, protocol_version) {
  test_utils::CassFuturePtr connect_future(cass_cluster_connect(cluster));
  test_utils::wait_and_check_error(connect_future.get());
  session = cass_future_get_session(connect_future.get());
}

SingleSessionTest::~SingleSessionTest() {
  CassFuturePtr close_future(cass_session_close(session));
  cass_future_wait(close_future.get());
}

void initialize_contact_points(CassCluster* cluster, std::string prefix, int num_nodes_dc1, int num_nodes_dc2) {
  for(int i = 0; i < num_nodes_dc1; ++i) {
    std::string contact_point(prefix + boost::lexical_cast<std::string>(i + 1));
    cass_cluster_set_contact_points(cluster, contact_point.c_str());
  }
}

CassSessionPtr create_session(CassCluster* cluster) {
  test_utils::CassFuturePtr session_future(cass_cluster_connect(cluster));
  test_utils::wait_and_check_error(session_future.get());
  return test_utils::CassSessionPtr(cass_future_get_session(session_future.get()));
}

void execute_query(CassSession* session,
                   const std::string& query,
                   CassResultPtr* result,
                   CassConsistency consistency) {
  CassStatementPtr statement(cass_statement_new(cass_string_init(query.c_str()), 0));
  cass_statement_set_consistency(statement.get(), consistency);
  CassFuturePtr future(cass_session_execute(session, statement.get()));
  wait_and_check_error(future.get());
  if(result != NULL) {
    *result = CassResultPtr(cass_future_get_result(future.get()));
  }
}

CassError execute_query_with_error(CassSession* session,
                                   const std::string& query,
                                   CassResultPtr* result,
                                   CassConsistency consistency) {
  CassStatementPtr statement(cass_statement_new(cass_string_init(query.c_str()), 0));
  cass_statement_set_consistency(statement.get(), consistency);
  CassFuturePtr future(cass_session_execute(session, statement.get()));
  CassError code = wait_and_return_error(future.get());
  if(result != NULL) {
    *result = CassResultPtr(cass_future_get_result(future.get()));
  }
  return code;
}

CassError wait_and_return_error(CassFuture* future, cass_duration_t timeout) {
  if(!cass_future_wait_timed(future, timeout)) {
    BOOST_FAIL("Timed out waiting for result");
  }
  return cass_future_error_code(future);
}

void wait_and_check_error(CassFuture* future, cass_duration_t timeout) {
  CassError code = wait_and_return_error(future, timeout);
  if(code != CASS_OK) {
    CassString message = cass_future_error_message(future);
    BOOST_FAIL("Error occured during query '" << std::string(message.data, message.length) << "' (" << boost::format("0x%08X") % code << ")");
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

//-----------------------------------------------------------------------------------
} // End of namespace test_utils
