#include "cql/cql.hpp"
#include "cql_ccm_bridge.hpp"
#include "test_utils.hpp"

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

		builder = cql::cql_cluster_t::builder();
		builder->with_log_callback(& test_utils::log_callback);		
	    builder->add_contact_point(ccm_contact_seed);
		
		if (use_ssl) {
			builder->with_ssl();
		}
	}

    ~CCM_SETUP()         
	{ 
		ccm->remove();
	}

	boost::shared_ptr<cql::cql_ccm_bridge_t> ccm;
	const cql::cql_ccm_bridge_configuration_t& conf;
	boost::asio::ip::address ccm_contact_seed;
	bool use_ssl;
	boost::shared_ptr<cql::cql_builder_t> builder;
};

BOOST_FIXTURE_TEST_SUITE( collections, CCM_SETUP )


void collection_test(boost::shared_ptr<cql::cql_cluster_t> cluster, std::string collection_type, bool list_prepending = false)
{
		std::string open_bracket = collection_type == "list" ? "[" : "{";
        std::string close_bracket = collection_type == "list" ? "]" : "}";
        std::string map_syntax = collection_type == "map" ? "int," : "";
		
		boost::shared_ptr<cql::cql_session_t> session(cluster->connect());

		if (!session) {
			BOOST_FAIL("Session creation failture.");
		}		

		test_utils::query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_FORMAT) % test_utils::SIMPLE_KEYSPACE % "1"));
			session->set_keyspace(test_utils::SIMPLE_KEYSPACE);		

			test_utils::query(session, str(boost::format("CREATE TABLE %s(tweet_id int PRIMARY KEY, some_collection %s<%sint>);") % test_utils::SIMPLE_TABLE % collection_type % map_syntax ));
			
			test_utils::query(session, str(boost::format("INSERT INTO %s(tweet_id,some_collection) VALUES ( 0 , %s);") % test_utils::SIMPLE_TABLE % (open_bracket + (!map_syntax.empty()? "0:" : "") + "0" + close_bracket))); 		    
			
			const unsigned int number_of_updates = 100;
			for( int i = 1; i < number_of_updates +1; i++) //starts from 1 because SET and MAP need unique values ( zero is taken for the first element..)
			{
				std::string adding = open_bracket + (!map_syntax.empty() ? boost::lexical_cast<std::string>(i) +":" :"") + boost::lexical_cast<std::string>(i) + close_bracket;
				test_utils::query(session, str(boost::format("UPDATE %s SET some_collection = %s WHERE tweet_id = 0;") % test_utils::SIMPLE_TABLE % 
					( list_prepending ? (adding + "+ some_collection") : ("some_collection + " + adding ))));
			}
						
			boost::shared_ptr<cql::cql_result_t> result = test_utils::query(session, str(boost::format("SELECT * FROM %s WHERE tweet_id = 0;")%test_utils::SIMPLE_TABLE));			
			if (result->next())
            {								
				if(collection_type == "set")
				{
					cql::cql_set_t* collection_row;
					result->get_set("some_collection", &collection_row);
					assert( collection_row->size() == number_of_updates +1);
					
					for( size_t i = 0u; i < collection_row->size(); i++)
					{
						int val;
						collection_row->get_int(i, val);
						assert(val == i);
					}				
				}
				else if(collection_type == "list")
				{
					cql::cql_list_t* collection_row;
					result->get_list("some_collection", &collection_row);
					assert( collection_row->size() == number_of_updates +1);
					
					for( size_t i = 0u; i < collection_row->size(); i++)
					{
						int val;
						collection_row->get_int(i, val);
						assert(val == (list_prepending ? (number_of_updates - i) : i));
					}				
				}
				else
				{
					cql::cql_map_t* collection_row;
					result->get_map("some_collection", &collection_row);
					assert( collection_row->size() == number_of_updates +1);
					for( size_t i = 0u; i < collection_row->size(); i++)
					{
						int key_value;
						collection_row->get_key_int(i, key_value);
						int val;
						collection_row->get_value_int(i, val);
					
						assert(key_value == i);
						assert(val == i);
					}				
				}
			}
			else
				assert(false);			
	session->close();
	cluster->shutdown();				
}


BOOST_AUTO_TEST_CASE(collections_set)
{
	collection_test(builder->build(), "set");
}

BOOST_AUTO_TEST_CASE(collections_list_appending)
{
	collection_test(builder->build(), "list", true);
}

BOOST_AUTO_TEST_CASE(collections_list)
{
	collection_test(builder->build(), "list");
}

BOOST_AUTO_TEST_CASE(collections_map)
{
	collection_test(builder->build(), "map");
}

BOOST_AUTO_TEST_SUITE_END()