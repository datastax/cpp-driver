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
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>

#include <cql/cql.hpp>
#include <cql/cql_error.hpp>
#include <cql/cql_event.hpp>
#include <cql/cql_client.hpp>
#include <cql/cql_client_factory.hpp>
#include <cql/cql_session.hpp>
#include <cql/cql_cluster.hpp>
#include <cql/cql_builder.hpp>
#include <cql/cql_execute.hpp>
#include <cql/cql_result.hpp>

#include <cql_ccm_bridge.hpp>

using namespace cql;

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
    cql::cql_initialize();
    
    try
    {
        cql::cql_thread_infrastructure_t cql_ti;
		int numberOfNodes = 1;

        const cql_ccm_bridge_configuration_t& conf = cql::get_ccm_bridge_configuration();
        // cql_ccm_bridge_t::create(conf, "test", numberOfNodes, true);

		boost::shared_ptr<cql::cql_builder_t> builder = cql::cql_cluster_t::builder();
		builder->with_log_callback(&log_callback);

		for(int i=1;i<=numberOfNodes;i++)
			builder->add_contact_point(conf.ip_prefix() + boost::lexical_cast<std::string>(i));

		// decide which client factory we want, SSL or non-SSL.  This is a hack, if you pass any commandline arg to the
		// binary it will use the SSL factory, non-SSL by default
		if (argc > 1) {
			builder->with_ssl();
		}

		boost::shared_ptr<cql::cql_cluster_t> cluster (builder->build());
		boost::shared_ptr<cql::cql_session_t> session (cluster->connect());

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

		cluster->shutdown();
        std::cout << "THE END" << std::endl;
    }
    catch (std::exception& e)
    {
        std::cout << "Exception: " << e.what() << std::endl;
    }
    
    cql::cql_terminate();
    return 0;
}
