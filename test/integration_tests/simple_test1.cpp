#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "cql/cql.hpp"
#include "cql_ccm_bridge.hpp"

#include <cql/cql_session.hpp>
#include <cql/cql_cluster.hpp>
#include <cql/cql_builder.hpp>

#include <cql/cql_list.hpp>
#include <cql/cql_set.hpp>
#include <cql/cql_map.hpp>

#include <boost/test/unit_test.hpp>
#include <boost/test/debug.hpp>

struct CCM_SETUP {
    CCM_SETUP() :conf(cql::get_ccm_bridge_configuration())
	{
		boost::debug::detect_memory_leaks(true);
		int numberOfNodes = 1;
		ccm = cql::cql_ccm_bridge_t::create(conf, "test", numberOfNodes, true);
		ccm_contact_seed = boost::asio::ip::address::from_string(conf.ip_prefix() + "1");
		use_ssl=false;
	}

    ~CCM_SETUP()         
	{ 
		ccm->remove();
	}

	boost::shared_ptr<cql::cql_ccm_bridge_t> ccm;
	const cql::cql_ccm_bridge_configuration_t& conf;
	boost::asio::ip::address ccm_contact_seed;
	bool use_ssl;
};

BOOST_FIXTURE_TEST_SUITE( simple_test1, CCM_SETUP )

// This function is called asynchronously every time an event is logged
void
log_callback(
    const cql::cql_short_t,
    const std::string& message)
{
    std::cout << "LOG: " << message << std::endl;
}

BOOST_AUTO_TEST_CASE(collections_set)
{
		boost::shared_ptr<cql::cql_builder_t> builder = cql::cql_cluster_t::builder();
		builder->with_log_callback(&log_callback);
		
    builder->add_contact_point(ccm_contact_seed);

		if (use_ssl) {
			builder->with_ssl();
		}

		boost::shared_ptr<cql::cql_cluster_t> cluster(builder->build());
		boost::shared_ptr<cql::cql_session_t> session(cluster->connect());

	if (!session) {
		BOOST_FAIL("Session creation failture.");
	}
			std::string keyspace_name = "test_ks";
			boost::shared_ptr<cql::cql_query_t> create_keyspace(
				new cql::cql_query_t(str(boost::format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};") % keyspace_name)));
		    boost::shared_future<cql::cql_future_result_t> create_keyspace_future = session->query(create_keyspace);
			create_keyspace_future.wait();

			session->set_keyspace(keyspace_name);
			
			std::string tablename = "test_table";			
			boost::shared_ptr<cql::cql_query_t> create_table(
				new cql::cql_query_t(str(boost::format("CREATE TABLE %s(tweet_id int PRIMARY KEY, some_collection set<int>);") % tablename), cql::CQL_CONSISTENCY_ONE));            
            boost::shared_future<cql::cql_future_result_t> create_table_future = session->query(create_table);
            create_table_future .wait();

					    
			boost::shared_ptr<cql::cql_query_t> insert_values(
				new cql::cql_query_t(str(boost::format("INSERT INTO %s(tweet_id,some_collection) VALUES ( 0 , %s);") % tablename % "<0>")));
			boost::shared_future<cql::cql_future_result_t> insert_values_result = session->query(insert_values);
			insert_values_result.wait();

			
			const unsigned int number_of_updates = 100;
			for( int i = 0; i < number_of_updates; i++)
			{
				boost::shared_ptr<cql::cql_query_t> update_table(
					new cql::cql_query_t(str(boost::format("UPDATE %s SET some_collection = some_collection + {%d} WHERE tweet_id = 0;") % tablename % i)));
				boost::shared_future<cql::cql_future_result_t> update_table_future = session->query(update_table);
				update_table_future.wait();
			}

			boost::shared_ptr<cql::cql_query_t> query1(
				new cql::cql_query_t(str(boost::format("SELECT * FROM %s WHERE tweet_id = 0;")%tablename)));
			boost::shared_future<cql::cql_future_result_t> query1_future = session->query(query1);
			query1_future.wait();
			cql::cql_future_result_t query1_result = query1_future.get();
			
			boost::shared_ptr<cql::cql_result_t> result = query1_result.result;			
			if (result->next())
            {
				cql::cql_set_t* set_row;
				result->get_set("some_collection", &set_row);
				assert( set_row->size() == number_of_updates);
				for( size_t i = 0u; i < set_row->size(); i++)
				{
					int val;
					set_row->get_int(i, val);
					assert(val == i);
				}				
			}
			else
				assert(false);

	session->close();
	cluster->shutdown();			
}

BOOST_AUTO_TEST_CASE(collections_map)
{
		boost::shared_ptr<cql::cql_builder_t> builder = cql::cql_cluster_t::builder();
		builder->with_log_callback(&log_callback);
		
    builder->add_contact_point(ccm_contact_seed);

		if (use_ssl) {
			builder->with_ssl();
		}

		boost::shared_ptr<cql::cql_cluster_t> cluster(builder->build());
		boost::shared_ptr<cql::cql_session_t> session(cluster->connect());

	if (!session) {
		BOOST_FAIL("Session creation failture.");
	}
			std::string keyspace_name = "test_ks";
			boost::shared_ptr<cql::cql_query_t> create_keyspace(
				new cql::cql_query_t(str(boost::format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};") % keyspace_name)));
		    boost::shared_future<cql::cql_future_result_t> create_keyspace_future = session->query(create_keyspace);
			create_keyspace_future.wait();

			session->set_keyspace(keyspace_name);
			
			std::string tablename = "test_table";			
			boost::shared_ptr<cql::cql_query_t> create_table(
				new cql::cql_query_t(str(boost::format("CREATE TABLE %s(tweet_id int PRIMARY KEY, some_collection map<int,int>);") % tablename), cql::CQL_CONSISTENCY_ONE));            
            boost::shared_future<cql::cql_future_result_t> create_table_future = session->query(create_table);
            create_table_future .wait();

					    
			boost::shared_ptr<cql::cql_query_t> insert_values(
				new cql::cql_query_t(str(boost::format("INSERT INTO %s(tweet_id,some_collection) VALUES ( 0 , %s);") % tablename % "{:}")));
			boost::shared_future<cql::cql_future_result_t> insert_values_result = session->query(insert_values);
			insert_values_result.wait();

			
			const unsigned int number_of_updates = 100;
			for( int i = 0; i < number_of_updates; i++)
			{
				boost::shared_ptr<cql::cql_query_t> update_table(
					new cql::cql_query_t(str(boost::format("UPDATE %s SET some_collection = some_collection + {%d:%d} WHERE tweet_id = 0;") % tablename % i % i)));
				boost::shared_future<cql::cql_future_result_t> update_table_future = session->query(update_table);
				update_table_future.wait();
			}

			boost::shared_ptr<cql::cql_query_t> query1(
				new cql::cql_query_t(str(boost::format("SELECT * FROM %s WHERE tweet_id = 0;")%tablename)));
			boost::shared_future<cql::cql_future_result_t> query1_future = session->query(query1);
			query1_future.wait();
			cql::cql_future_result_t query1_result = query1_future.get();
			
			boost::shared_ptr<cql::cql_result_t> result = query1_result.result;			
			if (result->next())
            {
				cql::cql_map_t* map_row;
				result->get_map("some_collection", &map_row);
				assert( map_row->size() == number_of_updates);
				for( size_t i = 0u; i < map_row->size(); i++)
				{
					int key_value;
					map_row->get_key_int(i, key_value);
					int val;
					map_row->get_value_int(i, val);
					
					assert(key_value == i);
					assert(val == i);
				}				
			}
			else
				assert(false);

	session->close();
	cluster->shutdown();			
}

BOOST_AUTO_TEST_CASE(collections_list)
{
		boost::shared_ptr<cql::cql_builder_t> builder = cql::cql_cluster_t::builder();
		builder->with_log_callback(&log_callback);
		
    builder->add_contact_point(ccm_contact_seed);

		if (use_ssl) {
			builder->with_ssl();
		}

		boost::shared_ptr<cql::cql_cluster_t> cluster(builder->build());
		boost::shared_ptr<cql::cql_session_t> session(cluster->connect());

	if (!session) {
		BOOST_FAIL("Session creation failure.");
	}
			std::string keyspace_name = "test_ks";
			boost::shared_ptr<cql::cql_query_t> create_keyspace(
				new cql::cql_query_t(str(boost::format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};") % keyspace_name)));
		    boost::shared_future<cql::cql_future_result_t> create_keyspace_future = session->query(create_keyspace);
			create_keyspace_future.wait();

			session->set_keyspace(keyspace_name);
			
			std::string tablename = "test_table";			
			boost::shared_ptr<cql::cql_query_t> create_table(
				new cql::cql_query_t(str(boost::format("CREATE TABLE %s(tweet_id int PRIMARY KEY, some_collection list<int>);") % tablename), cql::CQL_CONSISTENCY_ONE));            
            boost::shared_future<cql::cql_future_result_t> create_table_future = session->query(create_table);
            create_table_future .wait();

					    
			boost::shared_ptr<cql::cql_query_t> insert_values(
				new cql::cql_query_t(str(boost::format("INSERT INTO %s(tweet_id,some_collection) VALUES ( 0 , %s);") % tablename % "[]")));
			boost::shared_future<cql::cql_future_result_t> insert_values_result = session->query(insert_values);
			insert_values_result.wait();

			
			const unsigned int number_of_updates = 100;
			for( int i = 0; i < number_of_updates; i++)
			{
				boost::shared_ptr<cql::cql_query_t> update_table(
					new cql::cql_query_t(str(boost::format("UPDATE %s SET some_collection = some_collection + [%d] WHERE tweet_id = 0;") % tablename % i)));
				boost::shared_future<cql::cql_future_result_t> update_table_future = session->query(update_table);
				update_table_future.wait();
			}

			boost::shared_ptr<cql::cql_query_t> query1(
				new cql::cql_query_t(str(boost::format("SELECT * FROM %s WHERE tweet_id = 0;")%tablename)));
			boost::shared_future<cql::cql_future_result_t> query1_future = session->query(query1);
			query1_future.wait();
			cql::cql_future_result_t query1_result = query1_future.get();
			
			boost::shared_ptr<cql::cql_result_t> result = query1_result.result;			
			if (result->next())
            {
				cql::cql_list_t* list_row;
				result->get_list("some_collection", &list_row);
				assert( list_row->size() == number_of_updates);
				for( size_t i = 0u; i < list_row->size(); i++)
				{
					int val;
					list_row->get_int(i, val);
					assert(val == i);
				}				
			}
			else
				assert(false);

	session->close();
	cluster->shutdown();			
}

BOOST_AUTO_TEST_CASE(test1)
{
		boost::shared_ptr<cql::cql_builder_t> builder = cql::cql_cluster_t::builder();
		builder->with_log_callback(&log_callback);
		
    builder->add_contact_point(ccm_contact_seed);

		if (use_ssl) {
			builder->with_ssl();
		}

		boost::shared_ptr<cql::cql_cluster_t> cluster(builder->build());
		boost::shared_ptr<cql::cql_session_t> session(cluster->connect());

	if (!session) {
		BOOST_FAIL("Session creation failture.");
	}

            // write a query that switches current keyspace to "system"
            boost::shared_ptr<cql::cql_query_t> use_system(
				new cql::cql_query_t("USE system;", cql::CQL_CONSISTENCY_ONE));
            // send the query to Cassandra
            boost::shared_future<cql::cql_future_result_t> future = session->query(use_system);

            // wait for the query to execute
            future.wait();

	session->close();
	cluster->shutdown();			
}


BOOST_AUTO_TEST_SUITE_END()
