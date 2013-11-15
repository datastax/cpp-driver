/*
  Copyright (c) 2013 Matthew Stump

  This file is part of cassandra.

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

#include <cassert>
#include <pthread.h>

#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/program_options/option.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include <cql/cql.hpp>
#include <cql/cql_error.hpp>
#include <cql/cql_event.hpp>
#include <cql/cql_connection.hpp>
#include <cql/cql_connection_factory.hpp>
#include <cql/cql_session.hpp>
#include <cql/cql_cluster.hpp>
#include <cql/cql_builder.hpp>
#include <cql/cql_execute.hpp>
#include <cql/cql_result.hpp>

#define MIN_KEY 10000
#define MAX_KEY 19999

#define TEST_PREFIX "LOADTEST_"
#define TEST_VALUE "some payload but not too much"


static bool terminate = false;
static int64_t count;
static pthread_mutex_t mutex;


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
			std::cout << "insert failed: " << future.get().error.message << std::endl;
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



void
stress(
    const std::vector<std::string>& hosts,
    bool               use_ssl,
	int numthreads,
	int runtime)
{

	std::vector<pthread_t> pids;

    try
    {
		boost::shared_ptr<cql::cql_builder_t> builder = cql::cql_cluster_t::builder();
		// Logging callback is way to verbose so leave it off.
		// builder->with_log_callback(&log_callback);

		for (std::vector<std::string>::const_iterator it = hosts.begin(); it != hosts.end(); it++) {
			std::cout << "adding contact point " << *it << std::endl;
			builder->add_contact_point(boost::asio::ip::address::from_string(*it));
		}

		if (use_ssl) {
			builder->with_ssl();
		}

		boost::shared_ptr<cql::cql_cluster_t> cluster(builder->build());
		boost::shared_ptr<cql::cql_session_t> session(cluster->connect());

		if (session) {

			// Step 1: set up structure
			// Note: Don't check for errors - we'll find out soon enough.

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
	
			std::cout << "writes from " << numthreads << " threads for " << runtime << " secs:\t" << count << " total,\t " << count/runtime << " per sec" << std::endl;
		
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
			std::cout << "reads from " << numthreads << " threads for " << runtime << " secs:\t" << count << " total,\t " << count/runtime << " per sec" << std::endl;


			// close the connection session
			session->close();
		}		
		
		cluster->shutdown();
        std::cout << "THE END" << std::endl;
    }
    catch (std::exception& e)
    {
        std::cout << "Exception: " << e.what() << std::endl;
    }
}



int
main(
    int    argc,
    char** vargs)
{
    bool ssl = false;
    std::string hoststr;
	std::vector<std::string> hosts;
	int numthreads;
	int runtime;

    boost::program_options::options_description desc("Options");
    desc.add_options()
        ("help", "produce help message")
        ("ssl", boost::program_options::value<bool>(&ssl)->default_value(false), "use SSL")
        ("hosts", boost::program_options::value<std::string>(&hoststr)->default_value("127.0.0.1"), "comma separated list of notes to use as initial contact point")
		("threads", boost::program_options::value<int>(&numthreads)->default_value(1), "number of threads for stress test")
		("runtime", boost::program_options::value<int>(&runtime)->default_value(3), "number seconds each segment of the stress test is run");

    boost::program_options::variables_map variables_map;
    try {
        boost::program_options::store(boost::program_options::parse_command_line(argc, vargs, desc), variables_map);
        boost::program_options::notify(variables_map);
    }
    catch (boost::program_options::unknown_option ex) {
        std::cerr << desc << "\n";
        std::cerr << ex.what() << "\n";
        return 1;
    }

    if (variables_map.count("help")) {
        std::cerr << desc << "\n";
        return 0;
    }

	std::stringstream ss(hoststr);
	std::string s;
	while (getline(ss, s, ',')) {
		hosts.push_back(s);
	} 

	std::cout << "start" << std::endl;
    cql::cql_initialize();
	
	pthread_mutex_init( &mutex, NULL );

	std::cout << "stress start" << std::endl;
    stress(hosts, ssl, numthreads, runtime);

	pthread_mutex_destroy( &mutex );

    cql::cql_terminate();
    return 0;
}
