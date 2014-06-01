#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "cassandra.h"
#include "test_utils.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/thread.hpp>

struct COLLECTIONS_CCM_SETUP : test_utils::CCM_SETUP {
    COLLECTIONS_CCM_SETUP() : CCM_SETUP(1,0) {}
};

BOOST_FIXTURE_TEST_SUITE(collections, COLLECTIONS_CCM_SETUP)

template<class T>
void test_simple_insert_collection(CassSession* session, CassValueType type, CassValueType primary_type, const std::vector<T> values) {
  std::string table_name = str(boost::format("%s_%s") % test_utils::SIMPLE_TABLE % test_utils::get_value_type(primary_type));
  std::string test_value_type = str(boost::format("%s<%s>") % test_utils::get_value_type(type) % test_utils::get_value_type(primary_type));

  test_utils::execute_query(session, str(boost::format("CREATE TABLE %s (tweet_id int PRIMARY KEY, test_val %s);")
                                         % table_name % test_value_type));

  test_utils::StackPtr<CassCollection> input(cass_collection_new(values.size()));

  for(T value : values) {
    test_utils::Value<T>::append(input.get(), value);
  }

  std::string query = str(boost::format("INSERT INTO %s (tweet_id, test_val) VALUES(0, ?);") % table_name);

  test_utils::StackPtr<CassStatement> statement(cass_statement_new(cass_string_init(query.c_str()), 1, CASS_CONSISTENCY_ONE));

  BOOST_REQUIRE(cass_statement_bind_collection(statement.get(), 0, input.get(), cass_false) == CASS_OK);

  test_utils::StackPtr<CassFuture> result_future(cass_session_execute(session, statement.get()));
  test_utils::wait_and_check_error(result_future.get());

  test_utils::StackPtr<const CassResult> result;
  test_utils::execute_query(session, str(boost::format("SELECT * FROM %s WHERE tweet_id = 0;") % table_name), &result);
  BOOST_REQUIRE(cass_result_row_count(result.get()) == 1);
  BOOST_REQUIRE(cass_result_column_count(result.get()) > 0);

  const CassRow* row = cass_result_first_row(result.get());

  const CassValue* output = cass_row_get_column(row, 1);
  BOOST_REQUIRE(cass_value_type(output) == type);

  test_utils::StackPtr<CassIterator> iterator(cass_iterator_from_collection(output));

  size_t i = 0;
  while(cass_iterator_next(iterator.get())) {
    T result_value;
    BOOST_REQUIRE(test_utils::Value<T>::get(cass_iterator_get_value(iterator.get()), &result_value) == CASS_OK);
    BOOST_REQUIRE(test_utils::Value<T>::compare(result_value, values[i++]));
  }
  BOOST_REQUIRE(i == values.size());
}

void test_simple_insert_collection_all_types(CassCluster* cluster, CassValueType type) {
  test_utils::StackPtr<CassFuture> session_future;
  test_utils::StackPtr<CassSession> session(cass_cluster_connect(cluster, session_future.address_of()));
  test_utils::wait_and_check_error(session_future.get());

  test_utils::execute_query(session.get(), str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                               % test_utils::SIMPLE_KEYSPACE % "1"));

  test_utils::execute_query(session.get(), str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));

  {
    std::vector<cass_int32_t> values = { 1, 2, 3 };
    test_simple_insert_collection<cass_int32_t>(session.get(), type, CASS_VALUE_TYPE_INT, values);
  }
  {
    std::vector<cass_int64_t> values = { 1, 2, 3 };
    test_simple_insert_collection<cass_int64_t>(session.get(), type, CASS_VALUE_TYPE_BIGINT, values);
  }
  {
    std::vector<cass_float_t> values = { 0.1, 0.2, 0.3 };
    test_simple_insert_collection<cass_float_t>(session.get(), type, CASS_VALUE_TYPE_FLOAT, values);
  }
  {
    std::vector<cass_double_t> values = { 0.000000000001, 0.000000000002, 0.000000000003 };
    test_simple_insert_collection<cass_double_t>(session.get(), type, CASS_VALUE_TYPE_DOUBLE, values);
  }
  {
    std::vector<CassString> values = { cass_string_init("abc"), cass_string_init("def"), cass_string_init("ghi") };
    test_simple_insert_collection<CassString>(session.get(), type, CASS_VALUE_TYPE_TEXT,  values);
  }
  {
    std::vector<CassBytes> values = { test_utils::bytes_from_string("123"),
                                      test_utils::bytes_from_string("456"),
                                      test_utils::bytes_from_string("789") };
    test_simple_insert_collection<CassBytes>(session.get(), type, CASS_VALUE_TYPE_BLOB,  values);
  }
  {
    std::vector<CassInet> values = { test_utils::inet_v4_from_int(16777343),
                                     test_utils::inet_v4_from_int(16777344),
                                     test_utils::inet_v4_from_int(16777345) };
    test_simple_insert_collection<CassInet>(session.get(), type, CASS_VALUE_TYPE_INET,  values);
  }
  {
    std::vector<test_utils::Uuid> values;
    for(int i = 0; i < 3; ++i) {
      test_utils::Uuid value;
      cass_uuid_generate_time(value);
      values.push_back(value);
    }
    test_simple_insert_collection<test_utils::Uuid>(session.get(), type, CASS_VALUE_TYPE_UUID,  values);
  }
  {
    const cass_uint8_t varint[] = { 57, 115, 235, 135, 229, 215, 8, 125, 13, 43, 1, 25, 32, 135, 129, 180,
                                    112, 176, 158, 120, 246, 235, 29, 145, 238, 50, 108, 239, 219, 100, 250,
                                    84, 6, 186, 148, 76, 230, 46, 181, 89, 239, 247 };
    std::vector<CassDecimal> values;
    for(int i = 0; i < 3; ++i) {
      CassDecimal value;
      value.scale = i;
      value.varint = cass_bytes_init(varint, sizeof(varint));
      values.push_back(value);
    }
    test_simple_insert_collection<CassDecimal>(session.get(), type, CASS_VALUE_TYPE_DECIMAL,  values);
  }
}

BOOST_AUTO_TEST_CASE(simple_insert_set)
{
  test_simple_insert_collection_all_types(cluster, CASS_VALUE_TYPE_SET);
}

BOOST_AUTO_TEST_CASE(simple_insert_list)
{
  test_simple_insert_collection_all_types(cluster, CASS_VALUE_TYPE_LIST);
}

BOOST_AUTO_TEST_CASE(simple_insert_map)
{
}

BOOST_AUTO_TEST_SUITE_END()
