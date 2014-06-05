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

struct BatchTests : test_utils::MultipleNodesTest {
    BatchTests() : MultipleNodesTest(1,0) { }
};

BOOST_FIXTURE_TEST_SUITE(batch, BatchTests)

void setup_test_env(CassSession* session)
{
    test_utils::StackPtr<CassFuture> session_future;
    test_utils::wait_and_check_error(session_future.get());

    test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                           % test_utils::SIMPLE_KEYSPACE % "1"));

    test_utils::execute_query(session, str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));


    std::string table_name = str(boost::format("table_%s") % test_utils::generate_unique_str());
    test_utils::execute_query(session, str(boost::format("CREATE TABLE %s (tweet_id int PRIMARY KEY, test_val text);")
                                           % table_name));
}

BOOST_AUTO_TEST_CASE(test_bound)
{
    test_utils::StackPtr<CassFuture> session_future;
    test_utils::StackPtr<CassSession> session(cass_cluster_connect(cluster, session_future.address_of()));
    test_utils::wait_and_check_error(session_future.get());

    test_utils::execute_query(session.get(), str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                           % test_utils::SIMPLE_KEYSPACE % "1"));

    test_utils::execute_query(session.get(), str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));


    std::string table_name = str(boost::format("table_%s") % test_utils::generate_unique_str());
    test_utils::execute_query(session.get(), str(boost::format("CREATE TABLE %s (tweet_id int PRIMARY KEY, test_val text);")
                                           % table_name));
    CassBatch* batch = cass_batch_new(CASS_CONSISTENCY_ONE);
    std::string insert_query = str(boost::format("INSERT INTO %s (tweet_id, test_val) VALUES(?, ?);") % table_name);
    test_utils::StackPtr<CassStatement> insert_statement(cass_statement_new(cass_string_init(insert_query.c_str()), 1, CASS_CONSISTENCY_ONE));

    for (int x=0; x<4; x++)
    {
        BOOST_REQUIRE(cass_statement_bind_int32(insert_statement.get(), 0, x) == CASS_OK);
        BOOST_REQUIRE(cass_statement_bind_string(insert_statement.get(), 1, cass_string_init(str(boost::format("test data %s") % x).c_str())) == CASS_OK);
        cass_batch_add_statement(batch, insert_statement.get());
     }

    test_utils::StackPtr<CassFuture> insert_future(cass_session_execute_batch(session.get(), batch));
    test_utils::wait_and_check_error(insert_future.get());

    for (int y=0; y<4; y++)
    {
        std::string select_query = str(boost::format("SELECT * FROM %s WHERE tweet_id = ?;") % table_name);
        test_utils::StackPtr<CassStatement> select_statement(cass_statement_new(cass_string_init(select_query.c_str()), 1, CASS_CONSISTENCY_ONE));
        BOOST_REQUIRE(cass_statement_bind_int32(select_statement.get(), 0, y) == CASS_OK);
        test_utils::StackPtr<CassFuture> select_future(cass_session_execute(session.get(), select_statement.get()));
        test_utils::wait_and_check_error(select_future.get());
        test_utils::StackPtr<const CassResult> result(cass_future_get_result(select_future.get()));
        const CassValue* column = cass_row_get_column(cass_result_first_row(result.get()), 1);

        CassString result_value;
        BOOST_REQUIRE(cass_value_type(column) == CASS_VALUE_TYPE_TEXT);
        BOOST_REQUIRE(test_utils::Value<CassString>::get(column, &result_value) == CASS_OK);
        BOOST_REQUIRE(test_utils::Value<CassString>::equal(result_value, cass_string_init(str(boost::format("test data %s") % y).c_str())));
    }
    cass_batch_free(batch);
}

BOOST_AUTO_TEST_CASE(test_simple)
{
    test_utils::StackPtr<CassFuture> session_future;
    test_utils::StackPtr<CassSession> session(cass_cluster_connect(cluster, session_future.address_of()));
    test_utils::wait_and_check_error(session_future.get());

    test_utils::execute_query(session.get(), str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT)
                                           % test_utils::SIMPLE_KEYSPACE % "1"));

    test_utils::execute_query(session.get(), str(boost::format("USE %s") % test_utils::SIMPLE_KEYSPACE));


    std::string table_name = str(boost::format("table_%s") % test_utils::generate_unique_str());
    test_utils::execute_query(session.get(), str(boost::format("CREATE TABLE %s (tweet_id int PRIMARY KEY, test_val text);")
                                           % table_name));
    CassBatch* batch = cass_batch_new(CASS_CONSISTENCY_ONE);
    std::string insert_query = str(boost::format("INSERT INTO %s (tweet_id, test_val) VALUES(1, 'test data');") % table_name);
    test_utils::StackPtr<CassStatement> insert_statement(cass_statement_new(cass_string_init(insert_query.c_str()), 0, CASS_CONSISTENCY_ONE));
    cass_batch_add_statement(batch, insert_statement.get());
    test_utils::StackPtr<CassFuture> insert_future(cass_session_execute_batch(session.get(), batch));
    test_utils::wait_and_check_error(insert_future.get());

    std::string select_query = str(boost::format("SELECT * FROM %s;") % table_name);
    test_utils::StackPtr<CassStatement> select_statement(cass_statement_new(cass_string_init(select_query.c_str()), 1, CASS_CONSISTENCY_ONE));
    test_utils::StackPtr<CassFuture> select_future(cass_session_execute(session.get(), select_statement.get()));
    test_utils::wait_and_check_error(select_future.get());
    test_utils::StackPtr<const CassResult> result(cass_future_get_result(select_future.get()));
    const CassValue* column = cass_row_get_column(cass_result_first_row(result.get()), 1);
    CassString result_value;
    BOOST_REQUIRE(cass_value_type(column) == CASS_VALUE_TYPE_TEXT);
    BOOST_REQUIRE(test_utils::Value<CassString>::get(column, &result_value) == CASS_OK);
    BOOST_REQUIRE(test_utils::Value<CassString>::equal(result_value, cass_string_init(str(boost::format("test data")).c_str())));

    cass_batch_free(batch);
}

BOOST_AUTO_TEST_SUITE_END()
