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

#include <boost/asio/io_service.hpp>
#include <boost/container/vector.hpp>
#include <boost/cstdint.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/make_shared.hpp>
#include <boost/move/move.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/test/debug.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/thread/future.hpp>
#include <boost/thread/thread.hpp>
#include <boost/version.hpp>

#include "cassandra.h"
#include "constants.hpp"
#include "test_utils.hpp"

#if (defined(WIN32) || defined(_WIN32))
#define CONTAINER std::vector
#else
#define CONTAINER boost::container::vector
#endif

struct AllTypes {
  CassUuid id;
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
  cass_int8_t tinyint_sample;
  cass_int16_t smallint_sample;
  CassDate date_sample;
  CassTime time_sample;
  CassDuration duration_sample;
};

struct PreparedTests : public test_utils::SingleSessionTest {
  static const char* ALL_TYPE_TABLE_NAME;
  std::string columns_;
  std::string values_;
  size_t column_size_;

  PreparedTests()
      : test_utils::SingleSessionTest(2, 0) {
    test_utils::execute_query(session,
                              str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) %
                                  test_utils::SIMPLE_KEYSPACE % "1"));
    test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));
    if ((version.major_version >= 3 && version.minor_version >= 10) || version.major_version >= 4) {
      test_utils::execute_query(
          session,
          str(boost::format(test_utils::CREATE_TABLE_ALL_TYPES_V4_1) % ALL_TYPE_TABLE_NAME));
      column_size_ = 16;
    } else if ((version.major_version >= 2 && version.minor_version >= 2) ||
               version.major_version >= 3) {
      test_utils::execute_query(
          session, str(boost::format(test_utils::CREATE_TABLE_ALL_TYPES_V4) % ALL_TYPE_TABLE_NAME));
      column_size_ = 15;
    } else {
      test_utils::execute_query(
          session, str(boost::format(test_utils::CREATE_TABLE_ALL_TYPES) % ALL_TYPE_TABLE_NAME));
      column_size_ = 11;
    }

    columns_ =
        "id, text_sample, int_sample, bigint_sample, float_sample, double_sample, decimal_sample, "
        "blob_sample, boolean_sample, timestamp_sample, inet_sample";
    values_ = "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?";
    if ((version.major_version >= 2 && version.minor_version >= 2) || version.major_version >= 3) {
      columns_ += ", tinyint_sample, smallint_sample, date_sample, time_sample";
      values_ += ", ?, ?, ?, ?";
    }
    if ((version.major_version >= 3 && version.minor_version >= 10) || version.major_version >= 4) {
      columns_ += ", duration_sample";
      values_ += ", ?";
    }
  }

  ~PreparedTests() {
    // Drop the keyspace (ignore any and all errors)
    test_utils::execute_query_with_error(
        session,
        str(boost::format(test_utils::DROP_KEYSPACE_FORMAT) % test_utils::SIMPLE_KEYSPACE));
  }

  void insert_all_types(CassSession* session, const CassPrepared* prepared,
                        const AllTypes& all_types) {
    test_utils::CassStatementPtr statement(cass_prepared_bind(prepared));

    cass_statement_bind_uuid(statement.get(), 0, all_types.id);
    cass_statement_bind_string_n(statement.get(), 1, all_types.text_sample.data,
                                 all_types.text_sample.length);
    cass_statement_bind_int32(statement.get(), 2, all_types.int_sample);
    cass_statement_bind_int64(statement.get(), 3, all_types.bigint_sample);
    cass_statement_bind_float(statement.get(), 4, all_types.float_sample);
    cass_statement_bind_double(statement.get(), 5, all_types.double_sample);
    cass_statement_bind_decimal(statement.get(), 6, all_types.decimal_sample.varint,
                                all_types.decimal_sample.varint_size,
                                all_types.decimal_sample.scale);
    cass_statement_bind_bytes(statement.get(), 7, all_types.blob_sample.data,
                              all_types.blob_sample.size);
    cass_statement_bind_bool(statement.get(), 8, all_types.boolean_sample);
    cass_statement_bind_int64(statement.get(), 9, all_types.timestamp_sample);
    cass_statement_bind_inet(statement.get(), 10, all_types.inet_sample);
    if ((version.major_version >= 2 && version.minor_version >= 2) || version.major_version >= 3) {
      cass_statement_bind_int8(statement.get(), 11, all_types.tinyint_sample);
      cass_statement_bind_int16(statement.get(), 12, all_types.smallint_sample);
      cass_statement_bind_uint32(statement.get(), 13, all_types.date_sample.date);
      cass_statement_bind_int64(statement.get(), 14, all_types.time_sample.time);
    }
    if ((version.major_version >= 3 && version.minor_version >= 10) || version.major_version >= 4) {
      cass_statement_bind_duration(statement.get(), 15, all_types.duration_sample.months,
                                   all_types.duration_sample.days, all_types.duration_sample.nanos);
    }

    test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));

    test_utils::wait_and_check_error(future.get());
  }

  void compare_all_types(const AllTypes& input, const CassRow* row) {
    AllTypes output;
    BOOST_REQUIRE(cass_value_get_string(cass_row_get_column(row, 1), &output.text_sample.data,
                                        &output.text_sample.length) == CASS_OK);
    BOOST_REQUIRE(test_utils::Value<CassString>::equal(input.text_sample, output.text_sample));

    BOOST_REQUIRE(cass_value_get_int32(cass_row_get_column(row, 2), &output.int_sample) == CASS_OK);
    BOOST_REQUIRE(test_utils::Value<cass_int32_t>::equal(input.int_sample, output.int_sample));

    BOOST_REQUIRE(cass_value_get_int64(cass_row_get_column(row, 3), &output.bigint_sample) ==
                  CASS_OK);
    BOOST_REQUIRE(
        test_utils::Value<cass_int64_t>::equal(input.bigint_sample, output.bigint_sample));

    BOOST_REQUIRE(cass_value_get_float(cass_row_get_column(row, 4), &output.float_sample) ==
                  CASS_OK);
    BOOST_REQUIRE(test_utils::Value<cass_float_t>::equal(input.float_sample, output.float_sample));

    BOOST_REQUIRE(cass_value_get_double(cass_row_get_column(row, 5), &output.double_sample) ==
                  CASS_OK);
    BOOST_REQUIRE(
        test_utils::Value<cass_double_t>::equal(input.double_sample, output.double_sample));

    BOOST_REQUIRE(cass_value_get_decimal(cass_row_get_column(row, 6), &output.decimal_sample.varint,
                                         &output.decimal_sample.varint_size,
                                         &output.decimal_sample.scale) == CASS_OK);
    BOOST_REQUIRE(
        test_utils::Value<CassDecimal>::equal(input.decimal_sample, output.decimal_sample));

    BOOST_REQUIRE(cass_value_get_bytes(cass_row_get_column(row, 7), &output.blob_sample.data,
                                       &output.blob_sample.size) == CASS_OK);
    BOOST_REQUIRE(test_utils::Value<CassBytes>::equal(input.blob_sample, output.blob_sample));

    BOOST_REQUIRE(cass_value_get_bool(cass_row_get_column(row, 8), &output.boolean_sample) ==
                  CASS_OK);
    BOOST_REQUIRE(
        test_utils::Value<cass_bool_t>::equal(input.boolean_sample, output.boolean_sample));

    BOOST_REQUIRE(cass_value_get_int64(cass_row_get_column(row, 9), &output.timestamp_sample) ==
                  CASS_OK);
    BOOST_REQUIRE(
        test_utils::Value<cass_int64_t>::equal(input.timestamp_sample, output.timestamp_sample));

    BOOST_REQUIRE(cass_value_get_inet(cass_row_get_column(row, 10), &output.inet_sample) ==
                  CASS_OK);
    BOOST_REQUIRE(test_utils::Value<CassInet>::equal(input.inet_sample, output.inet_sample));

    if ((version.major_version >= 2 && version.minor_version >= 2) || version.major_version >= 3) {
      BOOST_REQUIRE(cass_value_get_int8(cass_row_get_column(row, 11), &output.tinyint_sample) ==
                    CASS_OK);
      BOOST_REQUIRE(
          test_utils::Value<cass_int8_t>::equal(input.tinyint_sample, output.tinyint_sample));

      BOOST_REQUIRE(cass_value_get_int16(cass_row_get_column(row, 12), &output.smallint_sample) ==
                    CASS_OK);
      BOOST_REQUIRE(
          test_utils::Value<cass_int16_t>::equal(input.smallint_sample, output.smallint_sample));

      BOOST_REQUIRE(cass_value_get_uint32(cass_row_get_column(row, 13), &output.date_sample.date) ==
                    CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassDate>::equal(input.date_sample, output.date_sample));

      BOOST_REQUIRE(cass_value_get_int64(cass_row_get_column(row, 14), &output.time_sample.time) ==
                    CASS_OK);
      BOOST_REQUIRE(test_utils::Value<CassTime>::equal(input.time_sample, output.time_sample));
    }

    if ((version.major_version >= 3 && version.minor_version >= 10) || version.major_version >= 4) {
      BOOST_REQUIRE(cass_value_get_duration(
                        cass_row_get_column(row, 15), &output.duration_sample.months,
                        &output.duration_sample.days, &output.duration_sample.nanos) == CASS_OK);
      BOOST_REQUIRE(
          test_utils::Value<CassDuration>::equal(input.duration_sample, output.duration_sample));
    }
  }
};

const char* PreparedTests::ALL_TYPE_TABLE_NAME = "all_types_table_prepared";

BOOST_FIXTURE_TEST_SUITE(prepared, PreparedTests)

BOOST_AUTO_TEST_CASE(bound_all_types_different_values) {
  std::string insert_query = str(boost::format("INSERT INTO %s (%s) VALUES (%s)") %
                                 ALL_TYPE_TABLE_NAME % columns_ % values_);

  test_utils::CassFuturePtr prepared_future(
      cass_session_prepare_n(session, insert_query.data(), insert_query.size()));
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
  uint8_t address3[CASS_INET_V6_LENGTH] = { 255, 128, 12,  1,   255, 255, 255, 255,
                                            255, 255, 255, 255, 255, 255, 255, 255 };

  const size_t all_types_count = 3;
  AllTypes all_types[all_types_count];

  all_types[0].id = test_utils::generate_time_uuid(uuid_gen);
  all_types[0].text_sample = CassString("first");
  all_types[0].int_sample = 10;
  all_types[0].bigint_sample = CASS_INT64_MAX - 1L;
  all_types[0].float_sample = 1.999f;
  all_types[0].double_sample = 32.002;
  all_types[0].decimal_sample = CassDecimal(varint1, sizeof(varint1), 1);
  all_types[0].blob_sample = CassBytes(bytes1, sizeof(bytes1));
  all_types[0].boolean_sample = cass_true;
  all_types[0].timestamp_sample = 1123200000;
  all_types[0].inet_sample = cass_inet_init_v4(address1);
  if ((version.major_version >= 2 && version.minor_version >= 2) || version.major_version >= 3) {
    all_types[0].tinyint_sample = 37;
    all_types[0].smallint_sample = 456;
    all_types[0].date_sample = test_utils::Value<CassDate>::max_value();
    all_types[0].time_sample = test_utils::Value<CassTime>::max_value();
  }
  if ((version.major_version >= 3 && version.minor_version >= 10) || version.major_version >= 4) {
    all_types[0].duration_sample = CassDuration(1, 2, 3);
  }

  all_types[1].id = test_utils::generate_time_uuid(uuid_gen);
  all_types[1].text_sample = CassString("second");
  all_types[1].int_sample = 0;
  all_types[1].bigint_sample = 0L;
  all_types[1].float_sample = 0.0f;
  all_types[1].double_sample = 0.0;
  all_types[1].decimal_sample = CassDecimal(varint2, sizeof(varint2), 2);
  all_types[1].blob_sample = CassBytes(bytes2, sizeof(bytes2));
  all_types[1].boolean_sample = cass_false;
  all_types[1].timestamp_sample = 0;
  all_types[1].inet_sample = cass_inet_init_v4(address2);
  if ((version.major_version >= 2 && version.minor_version >= 2) || version.major_version >= 3) {
    all_types[1].tinyint_sample = 0;
    all_types[1].smallint_sample = 0;
    all_types[1].date_sample = 0;
    all_types[1].time_sample = 0;
  }
  if ((version.major_version >= 3 && version.minor_version >= 10) || version.major_version >= 4) {
    all_types[1].duration_sample = CassDuration(0, 0, 0);
  }

  all_types[2].id = test_utils::generate_time_uuid(uuid_gen);
  all_types[2].text_sample = CassString("third");
  all_types[2].int_sample = -100;
  all_types[2].bigint_sample = CASS_INT64_MIN + 1;
  all_types[2].float_sample = -150.111f;
  all_types[2].double_sample = -5.12342;
  all_types[2].decimal_sample = CassDecimal(varint3, sizeof(varint3), 3);
  all_types[2].blob_sample = CassBytes(bytes3, sizeof(bytes3));
  all_types[2].boolean_sample = cass_true;
  all_types[2].timestamp_sample = -13462502400;
  all_types[2].inet_sample = cass_inet_init_v4(address3);
  if ((version.major_version >= 2 && version.minor_version >= 2) || version.major_version >= 3) {
    all_types[2].tinyint_sample = 127;
    all_types[2].smallint_sample = 32767;
    all_types[2].date_sample = test_utils::Value<CassDate>::min_value();
    all_types[2].time_sample = 12345678;
  }
  if ((version.major_version >= 3 && version.minor_version >= 10) || version.major_version >= 4) {
    all_types[2].duration_sample = CassDuration(1, 2, 3);
  }

  for (size_t i = 0; i < all_types_count; ++i) {
    insert_all_types(session, prepared.get(), all_types[i]);
  }

  std::string select_query =
      str(boost::format("SELECT %s FROM %s WHERE id IN (%s, %s, %s)") % columns_ %
          ALL_TYPE_TABLE_NAME % test_utils::string_from_uuid(all_types[0].id) %
          test_utils::string_from_uuid(all_types[1].id) %
          test_utils::string_from_uuid(all_types[2].id));

  test_utils::CassResultPtr result;
  test_utils::execute_query(session, select_query, &result);
  BOOST_REQUIRE(cass_result_row_count(result.get()) == all_types_count);
  BOOST_REQUIRE(cass_result_column_count(result.get()) == column_size_);

  test_utils::CassIteratorPtr iterator(cass_iterator_from_result(result.get()));

  while (cass_iterator_next(iterator.get())) {
    const CassRow* row = cass_iterator_get_row(iterator.get());
    CassUuid id;
    cass_value_get_uuid(cass_row_get_column(row, 0), &id);
    for (size_t i = 0; i < all_types_count; ++i) {
      if (test_utils::Value<CassUuid>::equal(id, all_types[i].id)) {
        compare_all_types(all_types[i], row);
      }
    }
  }
}

BOOST_AUTO_TEST_CASE(bound_all_types_null_values) {
  std::string insert_query = str(boost::format("INSERT INTO %s (%s) VALUES (%s)") %
                                 ALL_TYPE_TABLE_NAME % columns_ % values_);

  test_utils::CassFuturePtr prepared_future(
      cass_session_prepare_n(session, insert_query.data(), insert_query.size()));
  test_utils::wait_and_check_error(prepared_future.get());
  test_utils::CassPreparedPtr prepared(cass_future_get_prepared(prepared_future.get()));

  test_utils::CassStatementPtr statement(cass_prepared_bind(prepared.get()));

  CassUuid id = test_utils::generate_time_uuid(uuid_gen);

  cass_statement_bind_uuid(statement.get(), 0, id);
  for (size_t i = 1; i < column_size_; ++i) {
    cass_statement_bind_null(statement.get(), i);
  }

  test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));

  test_utils::wait_and_check_error(future.get());

  std::string select_query = str(boost::format("SELECT %s FROM %s WHERE id IN (%s)") % columns_ %
                                 ALL_TYPE_TABLE_NAME % test_utils::string_from_uuid(id));

  test_utils::CassResultPtr result;
  test_utils::execute_query(session, select_query, &result);
  BOOST_REQUIRE(cass_result_row_count(result.get()) == 1);
  BOOST_REQUIRE(cass_result_column_count(result.get()) == column_size_);

  const CassRow* row = cass_result_first_row(result.get());
  CassUuid result_id;
  BOOST_REQUIRE(cass_value_get_uuid(cass_row_get_column(row, 0), &result_id) == CASS_OK);
  BOOST_REQUIRE(test_utils::Value<CassUuid>::equal(id, result_id));
  for (size_t i = 1; i < column_size_; ++i) {
    BOOST_REQUIRE(cass_value_is_null(cass_row_get_column(row, i)));
  }
}

BOOST_AUTO_TEST_CASE(select_one) {
  std::string table_name =
      str(boost::format("table_%s") % test_utils::generate_unique_str(uuid_gen));
  std::string create_table_query =
      str(boost::format("CREATE TABLE %s (tweet_id int PRIMARY KEY, numb double, label text);") %
          table_name);

  test_utils::execute_query(session, create_table_query);

  for (int i = 0; i < 10; ++i) {
    std::string insert_query =
        str(boost::format("INSERT INTO %s (tweet_id, numb, label) VALUES(%d, 0.01,'row%d')") %
            table_name % i % i);
    test_utils::execute_query(session, insert_query);
  }

  std::string select_query =
      str(boost::format("SELECT * FROM %s WHERE tweet_id = ?;") % table_name);

  test_utils::CassFuturePtr prepared_future(
      cass_session_prepare_n(session, select_query.data(), select_query.size()));
  test_utils::wait_and_check_error(prepared_future.get());
  test_utils::CassPreparedPtr prepared(cass_future_get_prepared(prepared_future.get()));

  int tweet_id = 5;
  test_utils::CassStatementPtr statement(cass_prepared_bind(prepared.get()));
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
  BOOST_REQUIRE(cass_value_get_string(cass_row_get_column(row, 1), &result_label.data,
                                      &result_label.length) == CASS_OK);
  BOOST_REQUIRE(test_utils::Value<CassString>::equal(CassString("row5"), result_label));
}

#if BOOST_VERSION < 105700
CassPreparedMovable prepare_statement(CassSession* session, std::string query) {
#else
test_utils::CassPreparedPtr prepare_statement(CassSession* session, std::string query) {
#endif
  test_utils::CassFuturePtr prepared_future(
      cass_session_prepare_n(session, query.data(), query.size()));
  test_utils::wait_and_check_error(prepared_future.get());
#if BOOST_VERSION < 105700
  return CassPreparedMovable(cass_future_get_prepared(prepared_future.get()));
#else
  return test_utils::CassPreparedPtr(cass_future_get_prepared(prepared_future.get()));
#endif
}

void execute_statement(CassSession* session, const CassPrepared* prepared, int value) {
  test_utils::CassStatementPtr statement(cass_prepared_bind(prepared));
  BOOST_REQUIRE(cass_statement_bind_double(statement.get(), 0, static_cast<double>(value)) ==
                CASS_OK);
  BOOST_REQUIRE(cass_statement_bind_int32(statement.get(), 1, value) == CASS_OK);
  test_utils::CassFuturePtr future(cass_session_execute(session, statement.get()));
  test_utils::wait_and_check_error(future.get());
}

BOOST_AUTO_TEST_CASE(massive_number_of_prepares) {
  std::string table_name =
      str(boost::format("table_%s") % test_utils::generate_unique_str(uuid_gen));
  std::string create_table_query =
      str(boost::format("CREATE TABLE %s (tweet_id uuid PRIMARY KEY, numb1 double, numb2 int);") %
          table_name);
  size_t number_of_prepares = 100;

  test_utils::execute_query(session, create_table_query);

#if BOOST_VERSION < 105700
  CONTAINER<boost::BOOST_THREAD_FUTURE<CassPreparedMovable> > prepare_futures;
#else
  CONTAINER<boost::BOOST_THREAD_FUTURE<test_utils::CassPreparedPtr> > prepare_futures;
#endif

  std::vector<CassUuid> tweet_ids;
  for (size_t i = 0; i < number_of_prepares; ++i) {
    CassUuid tweet_id = test_utils::generate_time_uuid(uuid_gen);
    std::string insert_query =
        str(boost::format("INSERT INTO %s (tweet_id, numb1, numb2) VALUES (%s, ?, ?);") %
            table_name % test_utils::string_from_uuid(tweet_id));
    prepare_futures.push_back(
        boost::async(boost::launch::async, boost::bind(prepare_statement, session, insert_query)));
    tweet_ids.push_back(tweet_id);
  }

  std::vector<boost::shared_future<void> > execute_futures;
#if BOOST_VERSION < 105700
  CONTAINER<CassPreparedMovable> prepares;
#else
  CONTAINER<test_utils::CassPreparedPtr> prepares;
#endif
  for (size_t i = 0; i < prepare_futures.size(); ++i) {
#if BOOST_VERSION < 105700
    CassPreparedMovable prepared = prepare_futures[i].get();
#else
    test_utils::CassPreparedPtr prepared = prepare_futures[i].get();
#endif
    execute_futures.push_back(
        boost::async(boost::launch::async,
                     boost::bind(execute_statement, session, prepared.get(), i))
            .share());
    prepares.push_back(boost::move(prepared));
  }

  boost::wait_for_all(execute_futures.begin(), execute_futures.end());

  std::string select_query = str(boost::format("SELECT * FROM %s;") % table_name);
  test_utils::CassResultPtr result;
  test_utils::execute_query(session, select_query, &result);
  BOOST_REQUIRE(cass_result_row_count(result.get()) == number_of_prepares);

  test_utils::CassIteratorPtr iterator(cass_iterator_from_result(result.get()));

  while (cass_iterator_next(iterator.get())) {
    const CassRow* row = cass_iterator_get_row(iterator.get());
    CassUuid result_tweet_id;
    cass_value_get_uuid(cass_row_get_column(row, 0), &result_tweet_id);
    BOOST_REQUIRE(std::find(tweet_ids.begin(), tweet_ids.end(), result_tweet_id) !=
                  tweet_ids.end());
  }
}

BOOST_AUTO_TEST_SUITE_END()
