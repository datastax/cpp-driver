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

#include <boost/bind.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>

#include <cassandra/cql.hpp>
#include <cassandra/cql_error.hpp>
#include <cassandra/cql_event.hpp>
#include <cassandra/cql_client.hpp>
#include <cassandra/cql_client_factory.hpp>
#include <cassandra/cql_session.hpp>
#include <cassandra/cql_cluster.hpp>
#include <cassandra/cql_execute.hpp>
#include <cassandra/cql_result.hpp>

// helper function to print query results
void
print_rows(
    cql::cql_result_t& result)
{
    while (result.next()) {
        for (size_t i = 0; i < result.column_count(); ++i) {
            cql::cql_byte_t* data = NULL;
            cql::cql_int_t size = 0;
            result.get_data(i, &data, size);
            std::cout.write(reinterpret_cast<char*>(data), size);
            std::cout << " | ";
        }
        std::cout << std::endl;
    }
}


// This function is called asynchronously every time an event is logged
void
log_callback(
    const cql::cql_short_t,
    const std::string& message)
{
    std::cout << "LOG: " << message << std::endl;
}


int
main(int argc,
     char**)
{
    try
    {

	    // Initialize the IO service, this allows us to perform network operations asyncronously
        boost::asio::io_service io_service;

        // Typically async operations are performed in the thread performing the request, because we want synchronous behavior
        // we're going to spawn a thread whose sole purpose is to perform network communication, and we'll use this thread to
        // initiate and check the status of requests.
        //
        // Also, typically the boost::asio::io_service::run will exit as soon as it's work is done, which we want to prevent
        // because it's in it's own thread.  Using boost::asio::io_service::work prevents the thread from exiting.
        boost::scoped_ptr<boost::asio::io_service::work> work(new boost::asio::io_service::work(io_service));
        boost::thread thread(boost::bind(static_cast<size_t(boost::asio::io_service::*)()>(&boost::asio::io_service::run), &io_service));

		cql::cql_builder_t builder = cql::cql_cluster_t::builder();

		builder.with_log_callback(&log_callback);

		builder.add_contact_point("192.168.13.1");

		boost::shared_ptr<boost::asio::ssl::context> ctx(new boost::asio::ssl::context(boost::asio::ssl::context::sslv23));
		// decide which client factory we want, SSL or non-SSL.  This is a hack, if you pass any commandline arg to the
        // binary it will use the SSL factory, non-SSL by default
		if (argc > 1) {
			builder.with_ssl(ctx);
		}

		boost::shared_ptr<cql::cql_cluster_t> cluster (builder.build());

		boost::shared_ptr<cql::cql_session_t> session (cluster->connect(io_service));

		if(session.get()!=0){
			
            // The connection succeeded
            std::cout << "TRUE" << std::endl;

            // execute a query, switch keyspaces
            boost::shared_future<cql::cql_future_result_t> future = session->query("USE system;", cql::CQL_CONSISTENCY_ONE);

            // wait for the query to execute
            future.wait();

            // check whether the query succeeded
            std::cout << "switch keyspace successfull? " << (!future.get().error.is_err() ? "true" : "false") << std::endl;

            // execute a query, select all rows from the keyspace
            future = session->query("SELECT * from schema_keyspaces;", cql::CQL_CONSISTENCY_ONE);

            // wait for the query to execute
            future.wait();

            // check whether the query succeeded
            std::cout << "select successfull? " << (!future.get().error.is_err() ? "true" : "false") << std::endl;
            if (future.get().result) {
                // print the rows return by the successful query
                print_rows(*future.get().result);
            }

            // close the connection session
            session->close();

		}

        work.reset();
        thread.join();

        std::cout << "THE END" << std::endl;
    }
    catch (std::exception& e)
    {
        std::cout << "Exception: " << e.what() << std::endl;
    }
    return 0;
}
