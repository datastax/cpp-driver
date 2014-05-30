#define BOOST_TEST_DYN_LINK

#include "cassandra.h"
#include "test_utils.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/thread.hpp>

struct BASICS_CCM_SETUP : test_utils::CCM_SETUP {
    BASICS_CCM_SETUP() : CCM_SETUP(1,0) { }
};

BOOST_FIXTURE_TEST_SUITE(basics, BASICS_CCM_SETUP)

template <class T>
void simple_insert_test(CassCluster* cluster, CassValueType type, T value) {
  test_utils::StackPtr<CassFuture> session_future;
  test_utils::StackPtr<CassSession> session(cass_cluster_connect(cluster, session_future.address_of()));

  test_utils::wait_and_check_error(session_future.get());

  test_utils::execute_query(session.get(), str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                         % test_utils::SIMPLE_KEYSPACE % "1"));

  test_utils::execute_query(session.get(), str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));

  test_utils::execute_query(session.get(), str(boost::format("CREATE TABLE %s(tweet_id int PRIMARY KEY, test_val %s);")
                                         % test_utils::SIMPLE_TABLE % test_utils::get_value_type(type)));

  std::string query = str(boost::format("INSERT INTO %s(tweet_id, test_val) VALUES(0, ?);") % test_utils::SIMPLE_TABLE);

  test_utils::StackPtr<CassStatement> statement(cass_statement_new(cass_string_init(query.c_str()), 1, CASS_CONSISTENCY_ONE));

  BOOST_REQUIRE(test_utils::Value<T>::bind(statement.get(), 0, value) == CASS_OK);

  test_utils::StackPtr<CassFuture> result_future(cass_session_execute(session.get(), statement.get()));
  test_utils::wait_and_check_error(result_future.get());

  test_utils::StackPtr<const CassResult> result;
  test_utils::execute_query(session.get(), str(boost::format("SELECT * FROM %s WHERE tweet_id = 0;") % test_utils::SIMPLE_TABLE), &result);
  BOOST_REQUIRE(cass_result_row_count(result.get()) == 1);
  BOOST_REQUIRE(cass_result_column_count(result.get()) > 0);

  test_utils::StackPtr<CassIterator> iterator(cass_iterator_from_result(result.get()));
  BOOST_REQUIRE(cass_iterator_next(iterator.get()));

  const CassValue* column = cass_row_get_column(cass_iterator_get_row(iterator.get()), 1);
  T result_value;
  BOOST_REQUIRE(test_utils::Value<T>::get(column, &result_value) == CASS_OK);
  BOOST_REQUIRE(test_utils::Value<T>::compare(value, result_value));
}

BOOST_AUTO_TEST_CASE(simple_insert_int32)
{
  simple_insert_test<cass_int32_t>(cluster, CASS_VALUE_TYPE_INT, INT32_MAX);
}

BOOST_AUTO_TEST_CASE(simple_insert_int64)
{
  simple_insert_test<cass_int64_t>(cluster, CASS_VALUE_TYPE_BIGINT, INT64_MAX);
}

BOOST_AUTO_TEST_CASE(simple_insert_boolean)
{
  simple_insert_test<cass_bool_t>(cluster, CASS_VALUE_TYPE_BOOLEAN, cass_true);
}

BOOST_AUTO_TEST_CASE(simple_insert_float)
{
  simple_insert_test<cass_float_t>(cluster, CASS_VALUE_TYPE_FLOAT, 3.1415926f);
}

BOOST_AUTO_TEST_CASE(simple_insert_double)
{
  simple_insert_test<cass_double_t>(cluster, CASS_VALUE_TYPE_DOUBLE, 3.141592653589793);
}

BOOST_AUTO_TEST_CASE(simple_insert_string)
{
  CassString value = cass_string_init("Test Value.");
  simple_insert_test<CassString>(cluster, CASS_VALUE_TYPE_TEXT, value);
}

BOOST_AUTO_TEST_CASE(simple_insert_blob)
{
  const char* blob = "012345678900123456789001234567890012345678900123456789001234567890";
  CassBytes value = cass_bytes_init(reinterpret_cast<const cass_byte_t*>(blob), strlen(blob));
  simple_insert_test<CassBytes>(cluster, CASS_VALUE_TYPE_BLOB, value);
}

BOOST_AUTO_TEST_CASE(simple_insert_inet)
{
  cass_uint32_t inet = 2130706433; // 127.0.0.1
  CassInet value;
  value.address_length = 4;
  memcpy(value.address, &inet, sizeof(cass_uint32_t));
  simple_insert_test<CassInet>(cluster, CASS_VALUE_TYPE_INET, value);
}

BOOST_AUTO_TEST_CASE(simple_insert_uuid)
{
  CassUuid value;
  cass_uuid_generate_random(value);
  simple_insert_test<CassUuid>(cluster, CASS_VALUE_TYPE_UUID, value);
}

BOOST_AUTO_TEST_CASE(simple_insert_timeuuid)
{
  CassUuid value;
  cass_uuid_generate_time(value);
  simple_insert_test<CassUuid>(cluster, CASS_VALUE_TYPE_TIMEUUID, value);
}

BOOST_AUTO_TEST_CASE(simple_timestamp_test)
{
  test_utils::StackPtr<CassFuture> session_future;
  test_utils::StackPtr<CassSession> session(cass_cluster_connect(cluster, session_future.address_of()));

  test_utils::wait_and_check_error(session_future.get());

  test_utils::execute_query(session.get(), str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                         % test_utils::SIMPLE_KEYSPACE % "1"));

  test_utils::execute_query(session.get(), str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));

  test_utils::execute_query(session.get(), "CREATE TABLE test(tweet_id int PRIMARY KEY, test_val int);");

  test_utils::execute_query(session.get(), "INSERT INTO test(tweet_id, test_val) VALUES(0, 42);");
  test_utils::StackPtr<const CassResult> timestamp_result1;
  test_utils::execute_query(session.get(), "SELECT WRITETIME (test_val) FROM test;", &timestamp_result1);
  BOOST_REQUIRE(cass_result_row_count(timestamp_result1.get()) == 1);
  BOOST_REQUIRE(cass_result_column_count(timestamp_result1.get()) == 1);

  int pause_duration = 5 * test_utils::ONE_SECOND_IN_MICROS;
  boost::this_thread::sleep(boost::posix_time::microseconds(pause_duration));

  test_utils::execute_query(session.get(), "INSERT INTO test(tweet_id, test_val) VALUES(0, 43);");
  test_utils::StackPtr<const CassResult> timestamp_result2;
  test_utils::execute_query(session.get(), "SELECT WRITETIME (test_val) FROM test;", &timestamp_result2);
  BOOST_REQUIRE(cass_result_row_count(timestamp_result2.get()) == 1);
  BOOST_REQUIRE(cass_result_column_count(timestamp_result2.get()) == 1);

  cass_int64_t timestamp1 = 0;
  cass_value_get_int64(cass_row_get_column(cass_result_first_row(timestamp_result1.get()), 0), &timestamp1);

  cass_int64_t timestamp2 = 0;
  cass_value_get_int64(cass_row_get_column(cass_result_first_row(timestamp_result2.get()), 0), &timestamp2);

  BOOST_REQUIRE(timestamp1 != 0 && timestamp2 != 0);
  BOOST_REQUIRE(::abs(timestamp2 - timestamp1 - pause_duration) < 100000); // Tolerance
}

/////  --run_test="basics/rows_in_rows_out"
BOOST_AUTO_TEST_CASE(rows_in_rows_out)
{
  CassConsistency consistency = CASS_CONSISTENCY_ONE;

  test_utils::StackPtr<CassFuture> session_future;
  test_utils::StackPtr<CassSession> session(cass_cluster_connect(cluster, session_future.address_of()));

  test_utils::wait_and_check_error(session_future.get());

  test_utils::execute_query(session.get(), str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                         % test_utils::SIMPLE_KEYSPACE % "1"));

  test_utils::execute_query(session.get(), str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));

  // TODO(mpenick): In "master" were there more tests planned here?

  {
    test_utils::execute_query(session.get(),
                              str(boost::format("CREATE TABLE %s (tweet_id bigint PRIMARY KEY, t1 bigint, t2 bigint, t3 bigint);") % test_utils::SIMPLE_TABLE),
                              nullptr, 0, consistency);

    constexpr int num_rows = 100000;

    std::string insert_query(boost::str(boost::format("INSERT INTO %s (tweet_id, t1, t2, t3) VALUES (?, ?, ?, ?);") % test_utils::SIMPLE_TABLE));
    for(int i = 0; i < num_rows; ++i) {
      test_utils::StackPtr<CassStatement> statement(cass_statement_new(cass_string_init(insert_query.c_str()), 4, consistency));
      BOOST_REQUIRE(test_utils::Value<cass_int64_t>::bind(statement.get(), 0, i) == CASS_OK);
      BOOST_REQUIRE(test_utils::Value<cass_int64_t>::bind(statement.get(), 1, i + 1) == CASS_OK);
      BOOST_REQUIRE(test_utils::Value<cass_int64_t>::bind(statement.get(), 2, i + 2) == CASS_OK);
      BOOST_REQUIRE(test_utils::Value<cass_int64_t>::bind(statement.get(), 3, i + 3) == CASS_OK);
      test_utils::StackPtr<CassFuture> result_future(cass_session_execute(session.get(), statement.get()));
      test_utils::wait_and_check_error(result_future.get(), 30 * test_utils::ONE_SECOND_IN_MICROS);
    }


    std::string select_query(str(boost::format("SELECT tweet_id, t1, t2, t3 FROM %s LIMIT %d;") % test_utils::SIMPLE_TABLE % num_rows));
    test_utils::StackPtr<const CassResult> result;
    test_utils::execute_query(session.get(), select_query, &result, 0, consistency);
    BOOST_REQUIRE(cass_result_row_count(result.get()) == num_rows);
    BOOST_REQUIRE(cass_result_column_count(result.get()) == 4);

    test_utils::StackPtr<CassIterator> iterator(cass_iterator_from_result(result.get()));
    int row_count = 0;
    while(cass_iterator_next(iterator.get())) {
      cass_int64_t tweet_id = 0;
      cass_int64_t t1 = 0, t2 = 0, t3 = 0;
      BOOST_REQUIRE(test_utils::Value<cass_int64_t>::get(cass_row_get_column(cass_iterator_get_row(iterator.get()), 0), &tweet_id) == CASS_OK);
      BOOST_REQUIRE(test_utils::Value<cass_int64_t>::get(cass_row_get_column(cass_iterator_get_row(iterator.get()), 1), &t1) == CASS_OK);
      BOOST_REQUIRE(test_utils::Value<cass_int64_t>::get(cass_row_get_column(cass_iterator_get_row(iterator.get()), 2), &t2) == CASS_OK);
      BOOST_REQUIRE(test_utils::Value<cass_int64_t>::get(cass_row_get_column(cass_iterator_get_row(iterator.get()), 3), &t3) == CASS_OK);
      BOOST_REQUIRE(t1 == tweet_id + 1 && t2 == tweet_id + 2 && t3 == tweet_id + 3);
      row_count++;
    }

    BOOST_REQUIRE(row_count == num_rows);
  }
}

BOOST_AUTO_TEST_SUITE_END()
