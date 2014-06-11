#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include <future>

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/thread.hpp>

#include "cassandra.h"
#include "test_utils.hpp"

struct AllTypes {
  test_utils::Uuid id;
  CassString text_sample;
  cass_int32_t int_sample;
  cass_int64_t bigint_sample;
  cass_float_t float_sample;
  cass_double_t double_sample;
  CassDecimal decimal_sample;
  CassBytes blob_sample;
  cass_bool_t boolean_sample;
  cass_int64_t timestamp_sample;
  CassInet inet_sample;
};

struct PreparedTests : public test_utils::SingleSessionTest {
    static const char* ALL_TYPE_TABLE_NAME;

    PreparedTests() : test_utils::SingleSessionTest(2, 0) {
      test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                                   % test_utils::SIMPLE_KEYSPACE % "1"));
      test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
      test_utils::execute_query(session, str(boost::format(test_utils::CREATE_TABLE_ALL_TYPES) % ALL_TYPE_TABLE_NAME));
    }
};

const char* PreparedTests::ALL_TYPE_TABLE_NAME = "all_types_table_prepared";

BOOST_FIXTURE_TEST_SUITE(prepared, PreparedTests)

void insert_all_types(CassSession* session, const CassPrepared* prepared, const AllTypes& all_types) {
  test_utils::CassStatementPtr statement(cass_prepared_bind(prepared, 11, CASS_CONSISTENCY_ONE));

  cass_statement_bind_uuid(statement.get(), 0, all_types.id.uuid);
  cass_statement_bind_string(statement.get(), 1, all_types.text_sample);
  cass_statement_bind_int32(statement.get(), 2, all_types.int_sample);
  cass_statement_bind_int64(statement.get(), 3, all_types.bigint_sample);
  cass_statement_bind_float(statement.get(), 4, all_types.float_sample);
  cass_statement_bind_double(statement.get(), 5, all_types.double_sample);
  cass_statement_bind_decimal(statement.get(), 6, all_types.decimal_sample);
  cass_statement_bind_bytes(statement.get(), 7, all_types.blob_sample);
  cass_statement_bind_bool(statement.get(), 8, all_types.boolean_sample);
  cass_statement_bind_int64(statement.get(), 9, all_types.timestamp_sample);
  cass_statement_bind_inet(statement.get(), 10, all_types.inet_sample);

  test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));

  test_utils::wait_and_check_error(future.get());
}

void compare_all_types(const AllTypes& input, const CassRow* row) {
  AllTypes output;
  BOOST_REQUIRE(cass_value_get_string(cass_row_get_column(row, 1), &output.text_sample) == CASS_OK);
  BOOST_REQUIRE(test_utils::Value<decltype(input.text_sample)>::equal(input.text_sample, output.text_sample));

  BOOST_REQUIRE(cass_value_get_int32(cass_row_get_column(row, 2), &output.int_sample) == CASS_OK);
  BOOST_REQUIRE(test_utils::Value<decltype(input.int_sample)>::equal(input.int_sample, output.int_sample));

  BOOST_REQUIRE(cass_value_get_int64(cass_row_get_column(row, 3), &output.bigint_sample) == CASS_OK);
  BOOST_REQUIRE(test_utils::Value<decltype(input.bigint_sample)>::equal(input.bigint_sample, output.bigint_sample));

  BOOST_REQUIRE(cass_value_get_float(cass_row_get_column(row, 4), &output.float_sample) == CASS_OK);
  BOOST_REQUIRE(test_utils::Value<decltype(input.float_sample)>::equal(input.float_sample, output.float_sample));

  BOOST_REQUIRE(cass_value_get_double(cass_row_get_column(row, 5), &output.double_sample) == CASS_OK);
  BOOST_REQUIRE(test_utils::Value<decltype(input.double_sample)>::equal(input.double_sample, output.double_sample));

  BOOST_REQUIRE(cass_value_get_decimal(cass_row_get_column(row, 6), &output.decimal_sample) == CASS_OK);
  BOOST_REQUIRE(test_utils::Value<decltype(input.decimal_sample)>::equal(input.decimal_sample, output.decimal_sample));

  BOOST_REQUIRE(cass_value_get_bytes(cass_row_get_column(row, 7), &output.blob_sample) == CASS_OK);
  BOOST_REQUIRE(test_utils::Value<decltype(input.blob_sample)>::equal(input.blob_sample, output.blob_sample));

  BOOST_REQUIRE(cass_value_get_bool(cass_row_get_column(row, 8), &output.boolean_sample) == CASS_OK);
  BOOST_REQUIRE(test_utils::Value<decltype(input.boolean_sample)>::equal(input.boolean_sample, output.boolean_sample));

  BOOST_REQUIRE(cass_value_get_int64(cass_row_get_column(row, 9), &output.timestamp_sample) == CASS_OK);
  BOOST_REQUIRE(test_utils::Value<decltype(input.timestamp_sample)>::equal(input.timestamp_sample, output.timestamp_sample));

  BOOST_REQUIRE(cass_value_get_inet(cass_row_get_column(row, 10), &output.inet_sample) == CASS_OK);
  BOOST_REQUIRE(test_utils::Value<decltype(input.inet_sample)>::equal(input.inet_sample, output.inet_sample));
}

BOOST_AUTO_TEST_CASE(test_bound_all_types_different_values)
{
  std::string insert_query = str(boost::format("INSERT INTO %s "
                                               "(id, text_sample, int_sample, bigint_sample, float_sample, double_sample, decimal_sample, "
                                               "blob_sample, boolean_sample, timestamp_sample, inet_sample) "
                                               "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)") % ALL_TYPE_TABLE_NAME);

  test_utils::CassFuturePtr prepared_future(cass_session_prepare(session,
                                                                        cass_string_init2(insert_query.data(), insert_query.size())));
  test_utils::wait_and_check_error(prepared_future.get());
  test_utils::CassPreparedPtr prepared(cass_future_get_prepared(prepared_future.get()));

  uint8_t varint1[] = { 1, 2, 3 };
  uint8_t varint2[] = { 0, 0, 0 };
  uint8_t varint3[] = { 255, 255, 255, 255, 255 };
  uint8_t bytes1[] = { 255, 255 };
  uint8_t bytes2[] = { 0, 0 };
  uint8_t bytes3[] = { 1, 1 };
  uint8_t address1[CASS_INET_V4_LENGTH] = { 192, 168, 0, 100 };
  uint8_t address2[CASS_INET_V4_LENGTH] = { 0, 0, 0, 0 };
  uint8_t address3[CASS_INET_V6_LENGTH] = { 255, 128, 12, 1, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 };

  const size_t all_types_count = 3;
  AllTypes all_types[all_types_count];

  all_types[0] = { test_utils::generate_time_uuid(), cass_string_init("first"), 10, INT64_MAX - 1, 1.999f, 32.002,
                   cass_decimal_init(1, cass_bytes_init(varint1, sizeof(varint1))),
                   cass_bytes_init(bytes1, sizeof(bytes1)), cass_true, 1123200000, cass_inet_init_v4(address1) };

  all_types[1] = { test_utils::generate_time_uuid(), cass_string_init("second"), 0, 0, 0.0f, 0.0,
                   cass_decimal_init(2, cass_bytes_init(varint2, sizeof(varint2))),
                   cass_bytes_init(bytes2, sizeof(bytes2)), cass_false, 0, cass_inet_init_v4(address2) };

  all_types[2] = { test_utils::generate_time_uuid(), cass_string_init("third"), -100, INT64_MIN + 1, -150.111f, -5.12342,
                   cass_decimal_init(3, cass_bytes_init(varint3, sizeof(varint3))),
                   cass_bytes_init(bytes3, sizeof(bytes3)), cass_true, -13462502400, cass_inet_init_v6(address3) };

  for(size_t i = 0; i < all_types_count; ++i) {
    insert_all_types(session, prepared.get(), all_types[i]);
  }

  std::string select_query = str(boost::format("SELECT "
                                               "id, text_sample, int_sample, bigint_sample, float_sample, double_sample, decimal_sample, "
                                               "blob_sample, boolean_sample, timestamp_sample, inet_sample "
                                               "FROM %s WHERE id IN (%s, %s, %s)")
                                 % ALL_TYPE_TABLE_NAME
                                 % test_utils::string_from_uuid(all_types[0].id)
                                 % test_utils::string_from_uuid(all_types[1].id)
                                 % test_utils::string_from_uuid(all_types[2].id));

  test_utils::CassResultPtr result;
  test_utils::execute_query(session, select_query, &result);
  BOOST_REQUIRE(cass_result_row_count(result.get()) == all_types_count);
  BOOST_REQUIRE(cass_result_column_count(result.get()) == 11);

  test_utils::CassIteratorPtr iterator(cass_iterator_from_result(result.get()));

  while(cass_iterator_next(iterator.get())) {
    const CassRow* row = cass_iterator_get_row(iterator.get());
    CassUuid id;
    cass_value_get_uuid(cass_row_get_column(row, 0), id);
    for(size_t i = 0; i < all_types_count; ++i) {
      if(test_utils::Value<CassUuid>::equal(id, all_types[i].id)) {
        compare_all_types(all_types[i], row);
      }
    }
  }
}

BOOST_AUTO_TEST_CASE(test_bound_all_types_null_values)
{
  std::string insert_query = str(boost::format("INSERT INTO %s "
                                               "(id, text_sample, int_sample, bigint_sample, float_sample, double_sample, decimal_sample, "
                                               "blob_sample, boolean_sample, timestamp_sample, inet_sample) "
                                               "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)") % ALL_TYPE_TABLE_NAME);

  test_utils::CassFuturePtr prepared_future(cass_session_prepare(session,
                                                                        cass_string_init2(insert_query.data(), insert_query.size())));
  test_utils::wait_and_check_error(prepared_future.get());
  test_utils::CassPreparedPtr prepared(cass_future_get_prepared(prepared_future.get()));

  test_utils::CassStatementPtr statement(cass_prepared_bind(prepared.get(), 11, CASS_CONSISTENCY_ONE));

  test_utils::Uuid id = test_utils::generate_time_uuid();

  cass_statement_bind_uuid(statement.get(), 0, id);
  cass_statement_bind_null(statement.get(), 1);
  cass_statement_bind_null(statement.get(), 2);
  cass_statement_bind_null(statement.get(), 3);
  cass_statement_bind_null(statement.get(), 4);
  cass_statement_bind_null(statement.get(), 5);
  cass_statement_bind_null(statement.get(), 6);
  cass_statement_bind_null(statement.get(), 7);
  cass_statement_bind_null(statement.get(), 8);
  cass_statement_bind_null(statement.get(), 9);
  cass_statement_bind_null(statement.get(), 10);

  test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));

  test_utils::wait_and_check_error(future.get());

  std::string select_query = str(boost::format("SELECT "
                                               "id, text_sample, int_sample, bigint_sample, float_sample, double_sample, decimal_sample, "
                                               "blob_sample, boolean_sample, timestamp_sample, inet_sample "
                                               "FROM %s WHERE id IN (%s)")
                                 % ALL_TYPE_TABLE_NAME
                                 % test_utils::string_from_uuid(id));

  test_utils::CassResultPtr result;
  test_utils::execute_query(session, select_query, &result);
  BOOST_REQUIRE(cass_result_row_count(result.get()) == 1);
  BOOST_REQUIRE(cass_result_column_count(result.get()) == 11);

  const CassRow* row = cass_result_first_row(result.get());
  test_utils::Uuid result_id;
  BOOST_REQUIRE(cass_value_get_uuid(cass_row_get_column(row, 0), result_id) == CASS_OK);
  BOOST_REQUIRE(test_utils::Value<CassUuid>::equal(id, result_id));
  for(size_t i = 1; i < 11; ++i) {
    BOOST_REQUIRE(cass_value_is_null(cass_row_get_column(row, i)));
  }
}

BOOST_AUTO_TEST_CASE(test_select_one)
{
  std::string table_name = str(boost::format("table_%s") % test_utils::generate_unique_str());
  std::string create_table_query = str(boost::format("CREATE TABLE %s (tweet_id int PRIMARY KEY, numb double, label text);") % table_name);

  test_utils::execute_query(session, create_table_query);

  for(int i = 0; i < 10; ++i) {
    std::string insert_query = str(boost::format("INSERT INTO %s (tweet_id, numb, label) VALUES(%d, 0.01,'row%d')") % table_name % i % i);
    test_utils::execute_query(session, insert_query);
  }

  std::string select_query = str(boost::format("SELECT * FROM %s WHERE tweet_id = ?;") % table_name);

  test_utils::CassFuturePtr prepared_future(cass_session_prepare(session,
                                                                 cass_string_init2(select_query.data(), select_query.size())));
  test_utils::wait_and_check_error(prepared_future.get());
  test_utils::CassPreparedPtr prepared(cass_future_get_prepared(prepared_future.get()));

  int tweet_id = 5;
  test_utils::CassStatementPtr statement(cass_prepared_bind(prepared.get(), 1, CASS_CONSISTENCY_ONE));
  BOOST_REQUIRE(cass_statement_bind_int32(statement.get(), 0, tweet_id) == CASS_OK);

  test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
  test_utils::wait_and_check_error(future.get());

  test_utils::CassResultPtr result(cass_future_get_result(future.get()));
  BOOST_REQUIRE(cass_result_row_count(result.get()) == 1);
  BOOST_REQUIRE(cass_result_column_count(result.get()) == 3);

  const CassRow* row = cass_result_first_row(result.get());
  int result_tweet_id;
  CassString result_label;
  BOOST_REQUIRE(cass_value_get_int32(cass_row_get_column(row, 0), &result_tweet_id) == CASS_OK);
  BOOST_REQUIRE(test_utils::Value<cass_int32_t>::equal(tweet_id, result_tweet_id));
  BOOST_REQUIRE(cass_value_get_string(cass_row_get_column(row, 1), &result_label) == CASS_OK);
  BOOST_REQUIRE(test_utils::Value<CassString>::equal(cass_string_init("row5"), result_label));
}

const CassPrepared* prepare_statement(CassSession* session, std::string query) {
  test_utils::CassFuturePtr prepared_future(cass_session_prepare(session,
                                                                        cass_string_init2(query.data(), query.size())));
  test_utils::wait_and_check_error(prepared_future.get());
  return cass_future_get_prepared(prepared_future.get());
}

void execute_statement(CassSession* session, const CassPrepared* prepared, int value) {
  test_utils::CassStatementPtr statement(cass_prepared_bind(prepared, 2, CASS_CONSISTENCY_ONE));
  BOOST_REQUIRE(cass_statement_bind_double(statement.get(), 0, static_cast<double>(value)) == CASS_OK);
  BOOST_REQUIRE(cass_statement_bind_int32(statement.get(), 1, value) == CASS_OK);
  test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
  test_utils::wait_and_check_error(future.get());
}

BOOST_AUTO_TEST_CASE(test_massive_number_of_prepares)
{
  std::string table_name = str(boost::format("table_%s") % test_utils::generate_unique_str());
  std::string create_table_query = str(boost::format("CREATE TABLE %s (tweet_id uuid PRIMARY KEY, numb1 double, numb2 int);") % table_name);
  size_t number_of_prepares = 100;

  test_utils::execute_query(session, create_table_query);

  std::vector<test_utils::Uuid> tweet_ids;
  std::vector<std::future<const CassPrepared*>> prepare_tasks;
  for(size_t i = 0; i < number_of_prepares; ++i) {
    test_utils::Uuid tweet_id = test_utils::generate_time_uuid();
    std::string insert_query = str(boost::format("INSERT INTO %s (tweet_id, numb1, numb2) VALUES (%s, ?, ?);") % table_name % test_utils::string_from_uuid(tweet_id));
    prepare_tasks.push_back(std::async(std::launch::async, prepare_statement, session, insert_query));
    tweet_ids.push_back(tweet_id);
  }

  std::vector<test_utils::CassPreparedPtr> prepares;
  std::vector<std::future<void>> execute_tasks;
  for(size_t i = 0; i < number_of_prepares; ++i) {
    test_utils::CassPreparedPtr prepared(prepare_tasks[i].get());
    execute_tasks.push_back(std::async(std::launch::async, execute_statement, session, prepared.get(), i));
    prepares.emplace_back(std::move(prepared));
  }

  for(auto& task : execute_tasks) {
    task.wait();
  }

  std::string select_query = str(boost::format("SELECT * FROM %s;") % table_name);
  test_utils::CassResultPtr result;
  test_utils::execute_query(session, select_query, &result);
  BOOST_REQUIRE(cass_result_row_count(result.get()) == number_of_prepares);

  test_utils::CassIteratorPtr iterator(cass_iterator_from_result(result.get()));

  while(cass_iterator_next(iterator.get())) {
    const CassRow* row = cass_iterator_get_row(iterator.get());
    test_utils::Uuid result_tweet_id;
    cass_value_get_uuid(cass_row_get_column(row, 0), result_tweet_id.uuid);
    BOOST_REQUIRE(std::find(tweet_ids.begin(), tweet_ids.end(), result_tweet_id) != tweet_ids.end());
  }
}

BOOST_AUTO_TEST_SUITE_END()
