#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "cql/cql.hpp"
#include "cql_ccm_bridge.hpp"
#include "test_utils.hpp"

#include "cql/cql_session.hpp"
#include "cql/cql_cluster.hpp"
#include "cql/cql_builder.hpp"

#include "cql/cql_list.hpp"
#include "cql/cql_set.hpp"
#include "cql/cql_map.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>

struct BASICS_CCM_SETUP : test_utils::CCM_SETUP {
    BASICS_CCM_SETUP() : CCM_SETUP(1,0) {}
};

BOOST_FIXTURE_TEST_SUITE( basics, BASICS_CCM_SETUP )

boost::shared_ptr<cql::cql_result_t> simple_insert_test(boost::shared_ptr<cql::cql_cluster_t> cluster, cql::cql_column_type_enum col_type, std::string value_to_insert )
{
    boost::shared_ptr<cql::cql_session_t> session(cluster->connect());
    if (!session) {
        BOOST_FAIL("Session creation failure.");
    }

    test_utils::query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % "1"));
    session->set_keyspace(test_utils::SIMPLE_KEYSPACE);

    test_utils::query(session, str(boost::format("CREATE TABLE %s(tweet_id int PRIMARY KEY, test_val %s);") % test_utils::SIMPLE_TABLE % test_utils::get_cql(col_type)));
    
    if(col_type == cql::CQL_COLUMN_TYPE_TEXT)
        value_to_insert = std::string("\'") + value_to_insert + "\'";   
    test_utils::query(session, str(boost::format("INSERT INTO %s(tweet_id, test_val) VALUES(0,%s);") % test_utils::SIMPLE_TABLE % value_to_insert));
    
    boost::shared_ptr<cql::cql_result_t> result = test_utils::query(session, str(boost::format("SELECT * FROM %s WHERE tweet_id = 0;") % test_utils::SIMPLE_TABLE));    

    if(result->next())
    {
        session->close();
        return result;
    }
    else BOOST_FAIL("Empty result received.");
}


BOOST_AUTO_TEST_CASE(simple_insert_int32)
{
    int test_val = 2147483647;
    std::string conv_val = boost::lexical_cast<std::string>(test_val);
    boost::shared_ptr<cql::cql_result_t> result = simple_insert_test(builder->build(), cql::CQL_COLUMN_TYPE_INT, conv_val);
    int res;
    result->get_int("test_val", res);
    BOOST_REQUIRE(test_val == res);
}

BOOST_AUTO_TEST_CASE(simple_insert_int64)
{
    boost::int64_t test_val = 2147483648;
    std::string conv_val = boost::lexical_cast<std::string>(test_val);
    boost::shared_ptr<cql::cql_result_t> result = simple_insert_test(builder->build(), cql::CQL_COLUMN_TYPE_BIGINT, conv_val);
    boost::int64_t res;
    result->get_bigint("test_val", res);
    BOOST_REQUIRE(test_val == res);
}

BOOST_AUTO_TEST_CASE(simple_insert_boolean)
{
    bool test_val = false;
    std::string conv_val = "false";
    boost::shared_ptr<cql::cql_result_t> result = simple_insert_test(builder->build(), cql::CQL_COLUMN_TYPE_BOOLEAN, conv_val);
    bool res;
    result->get_bool("test_val", res);
    BOOST_REQUIRE(test_val == res);
}

BOOST_AUTO_TEST_CASE(simple_insert_float)
{
    float test_val = 3.1415926f;
    std::string conv_val = boost::lexical_cast<std::string>(test_val);
    boost::shared_ptr<cql::cql_result_t> result = simple_insert_test(builder->build(), cql::CQL_COLUMN_TYPE_FLOAT, conv_val);
    float res;
    result->get_float("test_val", res);
    BOOST_REQUIRE(test_val == res);
}

BOOST_AUTO_TEST_CASE(simple_insert_double)
{
    double test_val = 3.141592653589793;
    std::string conv_val = boost::lexical_cast<std::string>(test_val);
    boost::shared_ptr<cql::cql_result_t> result = simple_insert_test(builder->build(), cql::CQL_COLUMN_TYPE_DOUBLE, conv_val);
    double res;
    result->get_double("test_val", res);
    BOOST_REQUIRE(test_val == res);
}

BOOST_AUTO_TEST_CASE(simple_insert_string)
{
    std::string test_val = "Test Value.";
    boost::shared_ptr<cql::cql_result_t> result = simple_insert_test(builder->build(), cql::CQL_COLUMN_TYPE_TEXT, test_val);
    std::string res;
    result->get_string("test_val", res);
    BOOST_REQUIRE(test_val == res);
}

BOOST_AUTO_TEST_CASE(simple_timestamp_test)
{
    boost::shared_ptr<cql::cql_cluster_t> cluster = builder->build();
    boost::shared_ptr<cql::cql_session_t> session(cluster->connect());
    if (!session) {
        BOOST_FAIL("Session creation failure.");
    }
    
    test_utils::query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % "1"));
    session->set_keyspace(test_utils::SIMPLE_KEYSPACE);
    
    test_utils::query(session, "CREATE TABLE test(tweet_id int PRIMARY KEY, test_val int);");
    test_utils::query(session, "INSERT INTO test(tweet_id, test_val) VALUES(0,42);");
    
    boost::shared_ptr<cql::cql_result_t> timestamp_result1 =
        test_utils::query(session, "SELECT WRITETIME (test_val) FROM test;");

    const int break_length = 5000000; // [microseconds]
    boost::this_thread::sleep(boost::posix_time::microseconds(break_length));
    
    test_utils::query(session, "INSERT INTO test(tweet_id, test_val) VALUES(0,43);");
    boost::shared_ptr<cql::cql_result_t> timestamp_result2 =
        test_utils::query(session, "SELECT WRITETIME (test_val) FROM test;");
    
    cql::cql_bigint_t timestamp1, timestamp2;
    
    if (timestamp_result1->next() && timestamp_result2->next()) {
        timestamp_result1->get_bigint(0, timestamp1);
        timestamp_result2->get_bigint(0, timestamp2);
        session->close();
    }
    else {
        BOOST_FAIL("Cannot iterate over result");
    }

    BOOST_REQUIRE(::abs(timestamp2-timestamp1-break_length) < 100000); // Tolerance
}



/////  --run_test=basics/rows_in_rows_out
BOOST_AUTO_TEST_CASE(rows_in_rows_out)
{
    srand( (unsigned)time(NULL) );
    
    cql::cql_consistency_enum consistency = cql::CQL_CONSISTENCY_ONE;
    
    builder->with_load_balancing_policy( boost::shared_ptr< cql::cql_load_balancing_policy_t >( new cql::cql_round_robin_policy_t() ) );
    boost::shared_ptr<cql::cql_cluster_t> cluster(builder->build());
    boost::shared_ptr<cql::cql_session_t> session(cluster->connect());
    
    test_utils::query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % "1"));
    session->set_keyspace(test_utils::SIMPLE_KEYSPACE);
    
    { //// 1. Check the 32-int.
        test_utils::query(session, str(boost::format("CREATE TABLE %s(tweet_id bigint PRIMARY KEY, t1 bigint, t2 bigint, t3 bigint );") % test_utils::SIMPLE_TABLE ),consistency);
        
        std::map< int, cql::cql_int_t > int_map;
        
        int const integer_number_of_rows = 100000;
        
        for( int i = 0; i < integer_number_of_rows; ++i )
        {
            //// int_map.insert( std::make_pair( i, ii ) );
            std::string query_string( boost::str(boost::format("INSERT INTO %s (tweet_id,t1,t2,t3) VALUES (%d,%d,%d,%d);") % test_utils::SIMPLE_TABLE % i % i % i % i ) );
            boost::shared_ptr<cql::cql_query_t> _query(new cql::cql_query_t(query_string,consistency));
            session->query(_query);
        }
        
        cql::cql_bigint_t row_count( 0 );
        
        boost::shared_ptr<cql::cql_result_t> result = test_utils::query(session, str(boost::format("SELECT t1, t2, t3 FROM %s;") % test_utils::SIMPLE_TABLE ), consistency );
        
        int number_of_rows_selected( 0 );
        while (result->next())
        {
            ++number_of_rows_selected;
            cql::cql_bigint_t t1( 0 ), t2( 0 ), t3( 0 ), t4( 0 );
            
            result->get_bigint( 0, t1 );
            result->get_bigint( 1, t2 );
            result->get_bigint( 2, t3 );
        }
        
        if( number_of_rows_selected != integer_number_of_rows )
        {
            std::cout << "Wrong number of records. It should be: " << integer_number_of_rows << std::endl;
        }
        else
        {
            std::cout << "The number of rows is OK. " << std::endl;
        }
    }
}

BOOST_AUTO_TEST_SUITE_END()
