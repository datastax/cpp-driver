#ifdef _DEBUG
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
#include "cql/internal/cql_session_impl.hpp"

#include <set>
#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/thread/barrier.hpp>

struct ERROR_INJECTION_CCM_SETUP : test_utils::CCM_SETUP {
    ERROR_INJECTION_CCM_SETUP() : CCM_SETUP(4,0) {}
};

BOOST_FIXTURE_TEST_SUITE( error_injection_test_suite, ERROR_INJECTION_CCM_SETUP )

struct stress_thread_func_ctx
{
	int idx;
	boost::shared_ptr<cql::cql_session_t> session;
	boost::mutex* ar_guard;
	boost::barrier* barier;
	std::vector<boost::shared_future<cql::cql_future_result_t> >* ar;
};

static void stress_thread_func(stress_thread_func_ctx& ctx)
{
    try
    {
		ctx.barier->wait();
		std::string query_string(boost::str(boost::format("INSERT INTO %s (tweet_id,author,body)VALUES (%d,'author%d','body%d');") % test_utils::SIMPLE_TABLE%ctx.idx%ctx.idx%ctx.idx));
		boost::shared_ptr<cql::cql_query_t> _query(new cql::cql_query_t(query_string,cql::CQL_CONSISTENCY_ANY));
		boost::mutex::scoped_lock lock(*ctx.ar_guard);
		(*ctx.ar)[ctx.idx]= ctx.session->query(_query);
    }
	catch(std::exception& ex)
    {
		BOOST_WARN_MESSAGE(true,"@");
    }
}

BOOST_AUTO_TEST_CASE(error_injection_parallel_insert_test)
{
	boost::shared_ptr<cql::cql_cluster_t> cluster = builder->build();
	boost::shared_ptr<cql::cql_session_t> session = cluster->connect();
	if (!session) {
		BOOST_FAIL("Session creation failure.");
	}

	test_utils::query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % "1"));
	session->set_keyspace(test_utils::SIMPLE_KEYSPACE);

    for (int KK = 0; KK < 1; KK++)
    {
		test_utils::query(session, str(boost::format("CREATE TABLE %s(tweet_id int PRIMARY KEY, author text, body text);") % test_utils::SIMPLE_TABLE ));

        int RowsNo = 1000;
		boost::barrier barier(RowsNo);

		std::vector<boost::shared_future<cql::cql_future_result_t> > ar(RowsNo);
		boost::mutex ar_guard;

		std::vector<boost::shared_ptr<boost::thread> > threads(RowsNo); 

		for (int idx = 0; idx < RowsNo; idx++)
		{
			stress_thread_func_ctx  ctx;
			ctx.ar=&ar;
			ctx.barier=&barier;
			ctx.session=session;
			ctx.idx=idx;
			ctx.ar_guard = &ar_guard;
			threads[idx]=boost::shared_ptr<boost::thread>(new boost::thread(boost::bind(stress_thread_func,ctx)));
		};

		std::set<int> done; 

		int itercnt = 1;
        while (done.size() < RowsNo)
        {
			if(itercnt++%1000==0 && itercnt<1100)
			{
				session.get()->inject_random_connection_lowest_layer_shutdown();
				std::cout.put('^');

			}
			for (int i = 0; i < RowsNo; i++)
			{
				if(!(done.find(i)!=done.end()))
					if(ar[i].is_ready())
					{
						if(ar[i].timed_wait(boost::posix_time::milliseconds(10)))
						{
							done.insert(i);
							std::cout.put('+');
						}
						else
						{
							std::cout.put('-');
						}
					}
			}
		}

		boost::shared_ptr<cql::cql_result_t> result = test_utils::query(session, str(boost::format("SELECT count(*) FROM %s LIMIT %d;")%test_utils::SIMPLE_TABLE%(RowsNo+100)));			
		if (result->next())
		{
			cql::cql_bigint_t cnt;
			if(result->get_bigint(0,cnt))
			{
				BOOST_REQUIRE(cnt==RowsNo);
				return;
			}
        }
        BOOST_ERROR("Failture");
	}
}

BOOST_AUTO_TEST_SUITE_END()
#endif