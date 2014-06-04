#include <assert.h>

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/thread.hpp>
#include <boost/lexical_cast.hpp>

#include "cassandra.h"
#include "cql_ccm_bridge.hpp"
#include "test_utils.hpp"

namespace test_utils {
//-----------------------------------------------------------------------------------

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
    default: assert(false && "Invalid value type");
  }
}

//-----------------------------------------------------------------------------------
const std::string CREATE_KEYSPACE_SIMPLE_FORMAT = "CREATE KEYSPACE %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %s }";
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

MultipleNodesTest::MultipleNodesTest(int numberOfNodesDC1, int numberOfNodesDC2) : conf(cql::get_ccm_bridge_configuration()) {
  boost::debug::detect_memory_leaks(true);
  ccm = cql::cql_ccm_bridge_t::create(conf, "test", numberOfNodesDC1, numberOfNodesDC2);

  cluster = cass_cluster_new();
  for(int i = 0; i < numberOfNodesDC1; ++i) {
    std::string contact_point(conf.ip_prefix() + boost::lexical_cast<std::string>(i + 1));
    cass_cluster_setopt(cluster, CASS_OPTION_CONTACT_POINT_ADD, contact_point.data(), contact_point.size());
  }

  if(conf.use_logger())	{
    // TODO(mpenick): Add logging
  }
}

MultipleNodesTest::~MultipleNodesTest() {
  cass_cluster_free(cluster);
}

SingleSessionTest::SingleSessionTest(int numberOfNodesDC1, int numberOfNodesDC2)
  : MultipleNodesTest(numberOfNodesDC1, numberOfNodesDC2) {
  test_utils::StackPtr<CassFuture> connect_future;
  session = cass_cluster_connect(cluster, connect_future.address_of());
  test_utils::wait_and_check_error(connect_future.get());
}

SingleSessionTest::~SingleSessionTest() {
   test_utils::StackPtr<CassFuture> close_future(cass_session_close(session));
   cass_future_wait(close_future.get());
   cass_session_free(session);
}

void execute_query(CassSession* session,
                   const std::string& query,
                   StackPtr<const CassResult>* result,
                   CassConsistency consistency) {
  StackPtr<CassStatement> statement(cass_statement_new(cass_string_init(query.c_str()), 0, consistency));
  StackPtr<CassFuture> future(cass_session_execute(session, statement.get()));
  wait_and_check_error(future.get());
  if(result != nullptr) {
    result->assign(cass_future_get_result(future.get()));
  }
}

void wait_and_check_error(CassFuture* future, cass_duration_t timeout) {
  if(!cass_future_wait_timed(future, timeout)) {
    BOOST_FAIL("Timed out waiting for result");
  }
  CassError code = cass_future_error_code(future);
  if(code != CASS_OK) {
    CassString message = cass_future_error_message(future);
    BOOST_FAIL("Error occured during query '" << std::string(message.data, message.length) << "' (" << code << ")");
  }
}

std::string string_from_time_point(std::chrono::system_clock::time_point time) {
  std::time_t t = std::chrono::system_clock::to_time_t(time);
  char buffer[26];
  ctime_r(&t, buffer);
  return std::string(buffer, 24);
}

//-----------------------------------------------------------------------------------
} // End of namespace test_utils
