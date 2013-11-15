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
            for (int i = size; i < 25; i++) {
                std::cout << ' ' ;
            }
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

void
demo(
    const std::vector<std::string>& hosts,
    bool               use_ssl)
{
    try
    {
		boost::shared_ptr<cql::cql_builder_t> builder = cql::cql_cluster_t::builder();
		builder->with_log_callback(&log_callback);

		for (std::vector<std::string>::const_iterator it = hosts.begin(); it != hosts.end(); it++) {
			builder->add_contact_point(boost::asio::ip::address::from_string(*it));
		}

		if (use_ssl) {
			builder->with_ssl();
		}

		boost::shared_ptr<cql::cql_cluster_t> cluster(builder->build());
		boost::shared_ptr<cql::cql_session_t> session(cluster->connect());

		if (session) {

            // execute a query, switch keyspaces
            boost::shared_ptr<cql::cql_query_t> use_system(
                new cql::cql_query_t("USE system;", cql::CQL_CONSISTENCY_ONE));


            boost::shared_future<cql::cql_future_result_t> future = session->query(use_system);

            // wait for the query to execute
            future.wait();

            // check whether the query succeeded
            std::cout << "switch keyspace successfull? " << (!future.get().error.is_err() ? "true" : "false") << std::endl;

            // execute a query, select all rows from the keyspace
            boost::shared_ptr<cql::cql_query_t> select(
                new cql::cql_query_t("SELECT * from schema_keyspaces;", cql::CQL_CONSISTENCY_ONE));
            future = session->query(select);

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
}

int
main(
    int    argc,
    char** vargs)
{
    bool ssl = false;
    std::string hoststr;
	std::vector<std::string> hosts;

    boost::program_options::options_description desc("Options");
    desc.add_options()
        ("help", "produce help message")
        ("ssl", boost::program_options::value<bool>(&ssl)->default_value(false), "use SSL")
        ("hosts", boost::program_options::value<std::string>(&hoststr)->default_value("127.0.0.1"), "comma separated list of notes to use as initial contact point");

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

    cql::cql_initialize();
    demo(hosts, ssl);

    cql::cql_terminate();
    return 0;
}
