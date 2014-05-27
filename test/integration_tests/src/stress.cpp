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

#include <pthread.h>

#include <set>
#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/cstdint.hpp>
#include <boost/thread/barrier.hpp>

struct STRESS_CCM_SETUP : test_utils::CCM_SETUP {
    STRESS_CCM_SETUP() : CCM_SETUP(4,0) {}
};

BOOST_FIXTURE_TEST_SUITE( stress_test_suite, STRESS_CCM_SETUP )

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

BOOST_AUTO_TEST_CASE(parallel_insert_test)
{
	builder->set_thread_pool_size(10);
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
        
        while (done.size() < RowsNo)
        {
			for (int i = 0; i < RowsNo; i++)
			{
				if(!(done.find(i)!=done.end()))
					if(ar[i].is_ready())
					{
						if(ar[i].timed_wait(boost::posix_time::milliseconds(10)))
						{
							done.insert(i);
							//std::cout.put('+');
						}
						else
						{
							//std::cout.put('-');
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
//--------------------------------------------------------------------------------------------
static bool terminate = false;
static int64_t count;
static pthread_mutex_t mutex;
static const int MIN_KEY = 10000;
static const int MAX_KEY = 19999;
const char* TEST_PREFIX = "LOADTEST_";
const char* TEST_VALUE = "some payload but not too much";

static void* writeThread( void* args ) {
    cql::cql_session_t* session = (cql::cql_session_t*) args;
    
    while ( !terminate ) {
        pthread_mutex_lock( &mutex );
        count++;
        pthread_mutex_unlock( &mutex );
        
        char buf[256];
        int key = (rand() % ( MAX_KEY - MIN_KEY ) + MIN_KEY);
        
        snprintf(buf, sizeof(buf), "INSERT INTO test.loadtest(mykey, mytext) VALUES ('%s%d', '%s') USING TTL 60;", TEST_PREFIX, key, TEST_VALUE);
        
        // execute a query, select all rows from the keyspace
        boost::shared_ptr<cql::cql_query_t> insert(
                                                   new cql::cql_query_t(buf, cql::CQL_CONSISTENCY_ONE));
        
        boost::shared_future<cql::cql_future_result_t> future = session->query(insert);
        
        future.wait();
        
        if(future.get().error.is_err()){
            //std::cout << "insert failed: " << future.get().error.message << std::endl;
            continue;
        }
    }
    return 0;
}

static void* readThread( void* args ) {
    cql::cql_session_t* session = (cql::cql_session_t*) args;
    
    while ( !terminate ) {
        pthread_mutex_lock( &mutex );
        count++;
        pthread_mutex_unlock( &mutex );
        
        char buf[256];
        int key = (rand() % ( MAX_KEY - MIN_KEY ) + MIN_KEY);
        
        snprintf(buf, sizeof(buf), "SELECT * FROM test.loadtest WHERE mykey='%s%d';", TEST_PREFIX, key);
        
        // execute a query, select all rows from the keyspace
        boost::shared_ptr<cql::cql_query_t> insert(
                                                   new cql::cql_query_t(buf, cql::CQL_CONSISTENCY_ONE));
        
        boost::shared_future<cql::cql_future_result_t> future = session->query(insert);
        
        future.wait();
        
        if(future.get().error.is_err()){
            std::cout << "read failed: " << future.get().error.message << std::endl;
            continue;
        }
        
        boost::shared_ptr<cql::cql_result_t> result(future.get().result);
        if (result->row_count() > 1) {
            std::cout << "supicious number of results for key " << TEST_PREFIX << key << ": " << result->row_count() << std::endl;
            continue;
        }
        
        while (result->next()) {
            std::string mytext;
            
            if ( !result->get_string( "mytext", mytext )) {
                std::cout << "problem parsing value for key " << TEST_PREFIX << key << std::endl;
                continue;
            }
            
            if (mytext != TEST_VALUE) {
                std::cout << "unexpected value for key " << TEST_PREFIX << key << ": " << mytext << std::endl;
                continue;
            }
        }
    }
    return 0;
}

BOOST_AUTO_TEST_CASE(dieckmann_stress_test)
{
    int runtime = 20; // [seconds]
    bool use_ssl = false;
    int numthreads = 50;
    
    builder->with_log_callback(0); // std::cout may not be synchronized an will likely crash
    
    std::vector<pthread_t> pids;
    
    try
    {
        pthread_mutex_init( &mutex, NULL );
        if (use_ssl) {
            builder->with_ssl();
        }
        
        boost::shared_ptr<cql::cql_cluster_t> cluster(builder->build());
        boost::shared_ptr<cql::cql_session_t> session(cluster->connect());
        
        if (session) {
            
            // Step 1: set up structure
            // Note: Don't check for errors - we'll find out soon enough.
            test_utils::query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % "test" % "1"));
            
            boost::shared_future<cql::cql_future_result_t> future;
            
            boost::shared_ptr<cql::cql_query_t> use_stmt(
                                                         new cql::cql_query_t("USE test;", cql::CQL_CONSISTENCY_ONE));
            future = session->query(use_stmt);
            future.wait();
            
            boost::shared_ptr<cql::cql_query_t> drop_stmt(
                                                          new cql::cql_query_t("DROP TABLE loadtest;", cql::CQL_CONSISTENCY_ONE));
            future = session->query(drop_stmt);
            future.wait();
            
            boost::shared_ptr<cql::cql_query_t> create_stmt(
                                                            new cql::cql_query_t("CREATE TABLE loadtest (mykey text, myblob blob, mytext text, PRIMARY KEY (mykey)) WITH caching='ALL'", cql::CQL_CONSISTENCY_ONE));
            future = session->query(create_stmt);
            future.wait();
            
            // Step 2: write test
            count = 0;
            terminate = false;
            pids.clear();
            
            for (int i = 0; i < numthreads; i++) {
                pthread_t pid;
                pthread_create( &pid, NULL, writeThread, (void*) (session.get()) );
                pids.push_back(pid);
            }
            
            // Let the threads run for a bit.
            sleep (runtime);
            
            terminate = true;
            std::cout << "wrapping up write test" << std::endl;
            
            // give all threads the chance to finish
            while (pids.size() > 0) {
                void *status;
                pthread_join(pids.back(), &status);
                pids.pop_back();
            }
            
            std::cout << "writes from " << numthreads << " threads for " << runtime << " secs:\t"
            << count << " total,\t " << count/runtime << " per sec" << std::endl;
            
            // Step 3: read test
            count = 0;
            terminate = false;
            pids.clear();
            
            for (int i = 0; i < numthreads; i++) {
                pthread_t pid;
                pthread_create( &pid, NULL, readThread, (void*) (session.get()) );
                pids[i] = pid;
            }
            
            // Let the threads run for a bit.
            sleep (runtime);
            
            terminate = true;
            std::cout << "wrapping up read test" << std::endl;
            
            // give all threads the chance to finish
            for (int i = 0; i < numthreads; i++) {
                void *status;
                pthread_join(pids[i], &status);
            }
            std::cout << "reads from " << numthreads << " threads for " << runtime << " secs:\t"
            << count << " total,\t " << count/runtime << " per sec" << std::endl;
            
            // close the connection session
            session->close();
        }
        
        pthread_mutex_destroy( &mutex );
        cluster->shutdown();
        BOOST_MESSAGE("THE END");
    }
    catch (std::exception& e)
    {
        BOOST_FAIL(e.what());
    }
    
}

BOOST_AUTO_TEST_SUITE_END()
