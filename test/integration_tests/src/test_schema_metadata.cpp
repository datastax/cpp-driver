/*
  Copyright (c) 2014 DataStax

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

#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include <algorithm>
#include <string>
#include <stdlib.h>

#include <boost/test/unit_test.hpp>
#include <boost/format.hpp>
#include <boost/thread.hpp>

#include "cassandra.h"
#include "test_utils.hpp"

#define SIMPLE_STRATEGY_KEYSPACE_NAME "simple"
#define SIMPLE_STRATEGY_CLASS_NAME  "org.apache.cassandra.locator.SimpleStrategy"
#define NETWORK_TOPOLOGY_KEYSPACE_NAME "network"
#define NETWORK_TOPOLOGY_STRATEGY_CLASS_NAME "org.apache.cassandra.locator.NetworkTopologyStrategy"
#define LOCAL_STRATEGY_KEYSPACE_NAME "local"
#define LOCAL_STRATEGY_CLASS_NAME "org.apache.cassandra.locator.LocalStrategy"

#define ALL_DATA_TYPES_TABLE_NAME "all"

#define WITH_OPTIONS_TABLE_NAME "with_options"
#define WITH_OPTIONS_VERSION_1_ID 12345
#define WITH_OPTIONS_VERSION_1_AND_VERSION_2_0_CACHING "ALL"
#define WITH_OPTIONS_VERSION_1_AND_VERSION_2_0_POPULATE_IO_CACHE_ON_FLUSH "true"
#define WITH_OPTIONS_VERSION_1_AND_VERSION_2_0_REPLICATE_ON_WRITE "false"
#define WITH_OPTIONS_VERSION_2_0_AND_GREATER_DEFAULT_TIME_TO_LIVE 3600
#define WITH_OPTIONS_VERSION_2_0_AND_GREATER_INDEX_INTERVAL 14
#define WITH_OPTIONS_VERSION_2_1_CACHING "{'keys':'ALL', 'rows_per_partition':'ALL'}"
#define WITH_OPTIONS_VERSION_2_1_MIN_INDEX_INTERVAL 3
#define WITH_OPTIONS_VERSION_2_1_MAX_INDEX_INTERVAL 987
#define WITH_OPTIONS_BLOOM_FILTER_FP_CHANCE 0.25
#define WITH_OPTIONS_COMMENT "This table was made with options"
#define WITH_OPTIONS_COMPACTION_CLASS "org.apache.cassandra.db.compaction.LeveledCompactionStrategy"
#define WITH_OPTIONS_COMPACTION_SUBPROPERTIES_ENABLED "false"
#define WITH_OPTIONS_COMPACTION_SUBPROPERTIES_SSTABLE_SIZE_IN_MB 37
#define WITH_OPTIONS_COMPACTION_SUBPROPERTIES_TOMBSTONE_COMPACTION_INTERVAL 43200
#define WITH_OPTIONS_COMPACTION_SUBPROPERTIES_TOMBSTONE_THRESHOLD 0.5
#define WITH_OPTIONS_COMPRESSION_SSTABLE_COMPRESSION "org.apache.cassandra.io.compress.SnappyCompressor"
#define WITH_OPTIONS_COMPRESSION_CHUNK_LENGTH_KB 128
#define WITH_OPTIONS_GC_GRACE_SECONDS 74
#define WITH_OPTIONS_READ_REPAIR_CHANCE 0.5
#define WITH_OPTIONS_ALL_VERSIONS "CREATE TABLE %s (artist text, album_year int, album text, track int, song_title text, PRIMARY KEY (artist, album_year, album)) WITH CLUSTERING ORDER BY (album_year DESC, album ASC) AND bloom_filter_fp_chance = %f AND caching = %s AND comment = '%s' AND compaction = {'class':'%s', 'enabled':'%s', 'sstable_size_in_mb':%d, 'tombstone_compaction_interval':%d, 'tombstone_threshold':%f} AND compression = {'sstable_compression':'%s', 'chunk_length_kb':%d} AND gc_grace_seconds = %d AND read_repair_chance = %f"
#define WITH_OPTIONS_VERSION_1_ADDITIONAL_PROPERTIES "AND id = %d AND populate_io_cache_on_flush = '%s' AND replicate_on_write = '%s'"
#define WITH_OPTIONS_VERSION_2_0_ADDITIONAL_PROPERTIES "AND populate_io_cache_on_flush = '%s' AND replicate_on_write = '%s' AND default_time_to_live = %d AND index_interval = %d"
#define WITH_OPTIONS_VERSION_2_1_ADDITIONAL_PROPERTIES "AND default_time_to_live = %d AND index_interval = %d AND min_index_interval = %d AND max_index_interval = %d"

#define NUMBER_OF_ITERATIONS 4

//typedef declarations for pretty messages
typedef std::map<CassColumnType, std::string> CassColumnTypeMap;
typedef std::pair<CassColumnType, std::string> CassColumnTypePair;
typedef std::map<CassValueType, std::string> CassValueTypeMap;
typedef std::pair<CassValueType, std::string> CassValueTypePair;

/**
 * Schema Metadata Test Class
 *
 * The purpose of this class is to setup a single session integration test
 * while initializing single node cluster through CCM in order to perform
 * schema metadata validation against.
 */
struct TestSchemaMetadata : public test_utils::SingleSessionTest {
	/**
	 * Schema metadata from the current session
	 */
	CassSchemaMetadata* schemaMetadata_;
	/**
	 * Driver string for the simple strategy keyspace
	 */
	CassString simpleStrategyKeyspaceName_;
	/**
	 * Driver string for the network topology strategy keyspace
	 */
	CassString networkTopologyStrategyKeyspaceName_;

	/**
	 * Constructor - Create a single session test with a single node cluster
	 */
	TestSchemaMetadata() : SingleSessionTest(1, 0), schemaMetadata_(NULL) {
		//Initialize the keyspace names
		simpleStrategyKeyspaceName_ = cass_string_init(SIMPLE_STRATEGY_KEYSPACE_NAME);
		networkTopologyStrategyKeyspaceName_ = cass_string_init(NETWORK_TOPOLOGY_KEYSPACE_NAME);

		//Update the session schema metadata
		updateSessionSchemaMetadata();
	}

	/**
	 * Destructor - Clean up any additional resources created by the
	 *              TestSchemaMetadata class
	 */
	~TestSchemaMetadata() {
		//Free up the resources from the schema metadata
		if (schemaMetadata_) {
			cass_meta_free(schemaMetadata_);
		}
	}

	/**
	 * Update the session schema metadata while ensuring the previous resources
	 * are freed up
	 */
	void updateSessionSchemaMetadata() {
		//Free up the resources from the schema metadata
		if (schemaMetadata_) {
			cass_meta_free(schemaMetadata_);
		}

		//Update the current session schema metadata
		schemaMetadata_ = cass_session_get_schema_meta(session);
	}

	/**
	 * Create a keyspace using a simple replica placement strategy and update
	 * the schema metadata.  The schema will be dropped before it is created in
	 * the event it already exists.
	 *
	 * @param replicationFactor Replication factor for the number of replicas of
	 *                          data on multiple nodes (default: 1)
	 * @param isDurableWrites If true writes are written to commit log; false
	 *                        otherwise
	 */
	void createSimpleStrategyKeyspace(unsigned int replicationFactor = 1, bool isDurableWrites = true) {
		//Drop the SimpleStrategy keyspace (ignore errors)
		test_utils::execute_query_with_error(session, str(boost::format(test_utils::DROP_KEYSPACE_FORMAT) % SIMPLE_STRATEGY_KEYSPACE_NAME));

		//Create the keyspace with SimpleStrategy replica strategy
		test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_SIMPLE_WITH_DURABLE_RIGHTS_FORMAT) % SIMPLE_STRATEGY_KEYSPACE_NAME % replicationFactor % (isDurableWrites ? "true" : "false")));
		
		//Take a nap to ensure the keyspace creation has been completed
		boost::this_thread::sleep_for(boost::chrono::milliseconds(10));

		//Update the current session schema metadata
		updateSessionSchemaMetadata();
	}

	/**
	 * Create a keyspace using a network topology replica placement strategy and
	 * update the schema metadata.  The schema will be dropped before it is
	 * created in the event it already exists.
	 *
	 * @param replicationFactorDataCenterOne Replication factor for the number of
	 *                                       replicas of data on multiple nodes
	 *                                       for data center 1 (default: 3)
	 * @param replicationFactorDataCenterTwo Replication factor for the number of
	 *                                       replicas of data on multiple nodes
	 *                                       for data center 2 (default: 2)
	  @param isDurableWrites If true writes are written to commit log; false
	 *                        otherwise
	 */
	void createNetworkTopologyStrategyKeyspace(unsigned int replicationFactorDataCenterOne = 3, unsigned int replicationFactorDataCenterTwo = 2, bool isDurableWrites = true) {
		//Drop the SimpleStrategy keyspace (ignore errors)
		test_utils::execute_query_with_error(session, str(boost::format(test_utils::DROP_KEYSPACE_FORMAT) % NETWORK_TOPOLOGY_KEYSPACE_NAME));

		//Create the keyspace with NetworkTopologyStrategy replica strategy
		test_utils::execute_query(session, str(boost::format(test_utils::CREATE_KEYSPACE_NETWORK_WITH_DURABLE_RIGHTS_FORMAT) % NETWORK_TOPOLOGY_KEYSPACE_NAME % replicationFactorDataCenterOne % replicationFactorDataCenterTwo % (isDurableWrites ? "true" : "false")));

		//Take a nap to ensure the keyspace creation has been completed
		boost::this_thread::sleep_for(boost::chrono::milliseconds(10));

		//Update the current session schema metadata
		updateSessionSchemaMetadata();
	}

	/**
	 * Create a table with varying options depening on the Cassandra version
	 * detected
	 */
	void createTableWithOptions() {
		//Ensure the simple keyspace exists
		createSimpleStrategyKeyspace();

		//Create the table name for the with options table
		std::string tableName = std::string(SIMPLE_STRATEGY_KEYSPACE_NAME) + "." + WITH_OPTIONS_TABLE_NAME;

		//Determine and create the table format for the appropiate cassandra version
		//TODO: Accommodate DSE versions that correlate with Cassandra versions
		std::string createTableFormat(str(boost::format(WITH_OPTIONS_ALL_VERSIONS) % tableName % WITH_OPTIONS_BLOOM_FILTER_FP_CHANCE % WITH_OPTIONS_VERSION_1_AND_VERSION_2_0_CACHING % WITH_OPTIONS_COMMENT % WITH_OPTIONS_COMPACTION_CLASS % WITH_OPTIONS_COMPACTION_SUBPROPERTIES_ENABLED % WITH_OPTIONS_COMPACTION_SUBPROPERTIES_SSTABLE_SIZE_IN_MB % WITH_OPTIONS_COMPACTION_SUBPROPERTIES_TOMBSTONE_COMPACTION_INTERVAL % WITH_OPTIONS_COMPACTION_SUBPROPERTIES_TOMBSTONE_THRESHOLD % WITH_OPTIONS_COMPRESSION_SSTABLE_COMPRESSION % WITH_OPTIONS_COMPRESSION_CHUNK_LENGTH_KB % WITH_OPTIONS_GC_GRACE_SECONDS % WITH_OPTIONS_READ_REPAIR_CHANCE));
		if (version.major == 1) {
			//Create the table format for version 1.X Cassandra
			createTableFormat += std::string(" ") + WITH_OPTIONS_VERSION_1_ADDITIONAL_PROPERTIES;
			test_utils::execute_query(session, str(boost::format(createTableFormat) % WITH_OPTIONS_READ_REPAIR_CHANCE % WITH_OPTIONS_VERSION_1_ID % WITH_OPTIONS_VERSION_1_AND_VERSION_2_0_POPULATE_IO_CACHE_ON_FLUSH % WITH_OPTIONS_VERSION_1_AND_VERSION_2_0_REPLICATE_ON_WRITE));
		} else if (version.major == 2 && version.minor == 0) {
			//Create the table format for version 2.0.X Cassandra
			createTableFormat +=  std::string(" ") + WITH_OPTIONS_VERSION_2_0_ADDITIONAL_PROPERTIES;
			test_utils::execute_query(session, str(boost::format(createTableFormat) % WITH_OPTIONS_VERSION_1_AND_VERSION_2_0_POPULATE_IO_CACHE_ON_FLUSH % WITH_OPTIONS_VERSION_1_AND_VERSION_2_0_REPLICATE_ON_WRITE % WITH_OPTIONS_VERSION_2_0_AND_GREATER_DEFAULT_TIME_TO_LIVE % WITH_OPTIONS_VERSION_2_0_AND_GREATER_INDEX_INTERVAL));
		} else {
			//Display a warning message indicating the test may need to be updated (weak 2.1 check)
			if (version.minor > 1) {				
				BOOST_TEST_MESSAGE("Cassandra Version is Greater Than v2.1: Ensure table properties are appropiate for this version");
			}
			//Create the table format for version 2.1.X Cassandra
			createTableFormat +=  std::string(" ") + WITH_OPTIONS_VERSION_2_1_ADDITIONAL_PROPERTIES;
			test_utils::execute_query(session, str(boost::format(createTableFormat) % WITH_OPTIONS_VERSION_2_0_AND_GREATER_DEFAULT_TIME_TO_LIVE % WITH_OPTIONS_VERSION_2_0_AND_GREATER_INDEX_INTERVAL % WITH_OPTIONS_VERSION_2_1_MIN_INDEX_INTERVAL % WITH_OPTIONS_VERSION_2_1_MAX_INDEX_INTERVAL));
		}

		//Take a nap to ensure the keyspace creation has been completed
		boost::this_thread::sleep_for(boost::chrono::milliseconds(10));

		//Update the current session schema metadata
		updateSessionSchemaMetadata();
	}

	/**
	 * Create a table with all the datatypes in the simple keyspace
	 */
	void createTableAllDataTypes() {
		//Ensure the simple keyspace exists
		createSimpleStrategyKeyspace();

		//Create the data types table in the simple keyspace
		std::string tableName = std::string(SIMPLE_STRATEGY_KEYSPACE_NAME) + "." + ALL_DATA_TYPES_TABLE_NAME;
		test_utils::execute_query(session, str(boost::format(test_utils::CREATE_TABLE_ALL_TYPES) % tableName));

		//Take a nap to ensure the keyspace creation has been completed
		boost::this_thread::sleep_for(boost::chrono::milliseconds(10));

		//Update the current session schema metadata
		updateSessionSchemaMetadata();
	}
};

//Create the schema metadata test suite to utilize the test class
BOOST_FIXTURE_TEST_SUITE(schema_metadata, TestSchemaMetadata)

//Run a test case against keyspace metadata
BOOST_AUTO_TEST_CASE(keyspace) {
	//Create a container for the keyspace metadata
	CassKeyspaceMeta keyspaceMetadata;

	/*
	 * Perform keyspace schema metadata tests against a simple strategy keyspace
	 */
	for (int n = 1; n < NUMBER_OF_ITERATIONS; ++n) {
		//Randomly generate a replication factor [1 - 100]
		int replicationFactor = rand() % 100 + 1;

		//Create and check the simple strategy keyspace with durable rights
		createSimpleStrategyKeyspace(n);
		BOOST_CHECK_EQUAL(cass_meta_get_keyspace(schemaMetadata_, simpleStrategyKeyspaceName_, &keyspaceMetadata), CASS_OK);
		BOOST_CHECK_EQUAL(keyspaceMetadata.name.data, SIMPLE_STRATEGY_KEYSPACE_NAME);
		BOOST_CHECK_EQUAL(keyspaceMetadata.replication_strategy.data, SIMPLE_STRATEGY_CLASS_NAME);
		BOOST_CHECK_EQUAL(keyspaceMetadata.strategy_options.data, str(boost::format("{\"replication_factor\":\"%d\"}") % replicationFactor));
		BOOST_CHECK_EQUAL(keyspaceMetadata.durable_writes, true);

		//Create and check the simple strategy keyspace without durable rights
		createSimpleStrategyKeyspace(n, false);
		BOOST_CHECK_EQUAL(cass_meta_get_keyspace(schemaMetadata_, simpleStrategyKeyspaceName_, &keyspaceMetadata), CASS_OK);
		BOOST_CHECK_EQUAL(keyspaceMetadata.name.data, SIMPLE_STRATEGY_KEYSPACE_NAME);
		BOOST_CHECK_EQUAL(keyspaceMetadata.replication_strategy.data, SIMPLE_STRATEGY_CLASS_NAME);
		BOOST_CHECK_EQUAL(keyspaceMetadata.strategy_options.data, str(boost::format("{\"replication_factor\":\"%d\"}") % replicationFactor));
		BOOST_CHECK_EQUAL(keyspaceMetadata.durable_writes, false);
	}

	/*
	 * Perform keyspace schema metadata tests against a network topology strategy
	 * keyspace
	 */
	for (int x = 1; x < NUMBER_OF_ITERATIONS; ++x) {
		for (int y = x; y < NUMBER_OF_ITERATIONS; ++y) {
			//Randomly generate a replication factor [1 - x]
			int replicationFactorDataCenterTwo = rand() % x + 1;

			//Create and check the network topology strategy keyspace with durable rights
			createNetworkTopologyStrategyKeyspace(x, replicationFactorDataCenterTwo);
			BOOST_REQUIRE_EQUAL(cass_meta_get_keyspace(schemaMetadata_, networkTopologyStrategyKeyspaceName_, &keyspaceMetadata), CASS_OK);
			BOOST_CHECK_EQUAL(keyspaceMetadata.name.data, NETWORK_TOPOLOGY_KEYSPACE_NAME);
			BOOST_CHECK_EQUAL(keyspaceMetadata.replication_strategy.data, NETWORK_TOPOLOGY_STRATEGY_CLASS_NAME);
			BOOST_CHECK_EQUAL(keyspaceMetadata.strategy_options.data, str(boost::format("{\"dc2\":\"%d\",\"dc1\":\"%d\"}") % replicationFactorDataCenterTwo % x));
			BOOST_CHECK_EQUAL(keyspaceMetadata.durable_writes, true);

			//Create and check the network topology strategy keyspace without durable rights
			createNetworkTopologyStrategyKeyspace(x, replicationFactorDataCenterTwo, false);
			BOOST_REQUIRE_EQUAL(cass_meta_get_keyspace(schemaMetadata_, networkTopologyStrategyKeyspaceName_, &keyspaceMetadata), CASS_OK);
			BOOST_CHECK_EQUAL(keyspaceMetadata.name.data, NETWORK_TOPOLOGY_KEYSPACE_NAME);
			BOOST_CHECK_EQUAL(keyspaceMetadata.replication_strategy.data, NETWORK_TOPOLOGY_STRATEGY_CLASS_NAME);
			BOOST_CHECK_EQUAL(keyspaceMetadata.strategy_options.data, str(boost::format("{\"dc2\":\"%d\",\"dc1\":\"%d\"}") % replicationFactorDataCenterTwo % x));
			BOOST_CHECK_EQUAL(keyspaceMetadata.durable_writes, false);
		}
	}
}

//Run a test case against the column family
BOOST_AUTO_TEST_CASE(column_family) {
	//Create a container for the column family metadata
	CassColumnFamilyMeta columnFamilyMetadata;

	//Create a table with options
	createTableWithOptions();

	//Get the column family metadata for the table with options
	BOOST_REQUIRE_EQUAL(cass_meta_get_column_family2(schemaMetadata_, SIMPLE_STRATEGY_KEYSPACE_NAME, WITH_OPTIONS_TABLE_NAME, &columnFamilyMetadata), CASS_OK);

	//Validate the common column family metadata between Cassandra versions
	BOOST_CHECK_EQUAL(columnFamilyMetadata.name.data, WITH_OPTIONS_TABLE_NAME);
	BOOST_CHECK_EQUAL(columnFamilyMetadata.bloom_filter_fp_chance, WITH_OPTIONS_BLOOM_FILTER_FP_CHANCE);
	BOOST_CHECK_EQUAL(columnFamilyMetadata.comment.data, WITH_OPTIONS_COMMENT);
	BOOST_CHECK_EQUAL(columnFamilyMetadata.compaction_strategy_class.data, WITH_OPTIONS_COMPACTION_CLASS);

	//TODO: Implement checks for compaction options and compression parameters
	//BOOST_CHECK_EQUAL(columnFamilyMetadata.compaction_strategy_options.data, );
	//BOOST_CHECK_EQUAL(columnFamilyMetadata.compression_parameters.data, );
	//Force the boost test messages to be displayed
	boost::unit_test::unit_test_log_t::instance().set_threshold_level(boost::unit_test::log_messages);
	BOOST_TEST_MESSAGE("\tcompaction_strategy_options: " << columnFamilyMetadata.compaction_strategy_options.data);
	BOOST_TEST_MESSAGE("\tcompression_parameters: " << columnFamilyMetadata.compression_parameters.data);
	//ENDTODO

	BOOST_CHECK_EQUAL(columnFamilyMetadata.gc_grace_seconds, WITH_OPTIONS_GC_GRACE_SECONDS);
	BOOST_CHECK_EQUAL(columnFamilyMetadata.read_repair_chance, WITH_OPTIONS_READ_REPAIR_CHANCE);

	//Validate column family metadata for Cassandra v1.x
//FIXME: Uncomment once it is implemented in CassColumnFamilyMeta
//	if (version.major == 1) {
//		BOOST_CHECK_EQUAL(columnFamilyMetadata.id, WITH_OPTIONS_VERSION_1_ID);
//	}

	//Validate column family metadata for Cassandra v1.x and 2.0.x
	if (version.major == 1 || (version.major == 2 && version.minor == 0)) {
		BOOST_CHECK_EQUAL(columnFamilyMetadata.caching.data, WITH_OPTIONS_VERSION_1_AND_VERSION_2_0_CACHING);
//FIXME: Uncomment once it is implemented in CassColumnFamilyMeta
//		BOOST_CHECK_EQUAL((columnFamilyMetadata.populate_io_cache_on_flush ? "true" : "false"), WITH_OPTIONS_VERSION_1_AND_VERSION_2_0_POPULATE_IO_CACHE_ON_FLUSH);
//		BOOST_CHECK_EQUAL((columnFamilyMetadata.replicate_on_write ? "true" : "false"), WITH_OPTIONS_VERSION_1_AND_VERSION_2_0_REPLICATE_ON_WRITE);
	}

	//Validate column family metadata for Cassandra v2.0.x and v2.1.x
	if (version.major >= 2) {
		BOOST_CHECK_EQUAL(columnFamilyMetadata.default_time_to_live, WITH_OPTIONS_VERSION_2_0_AND_GREATER_DEFAULT_TIME_TO_LIVE);

		//index_interval table property has been replaced by min/max in v2.1
		if (version.major >= 2 && version.minor >= 1) {
			BOOST_CHECK_EQUAL(columnFamilyMetadata.index_interval, -1);
		} else {
			BOOST_CHECK_EQUAL(columnFamilyMetadata.index_interval, WITH_OPTIONS_VERSION_2_0_AND_GREATER_INDEX_INTERVAL);
		}
	}

	//Validate colum family metadata for Cassandra v2.1.x
	if (version.major >= 2 && version.minor >= 1) {
		//Replace single quotes with double quotes for string comparision
		std::string caching(WITH_OPTIONS_VERSION_2_1_CACHING);
		std::replace(caching.begin(), caching.end(), '\'', '\"');
		BOOST_CHECK_EQUAL(columnFamilyMetadata.caching.data, caching);
		BOOST_CHECK_EQUAL(columnFamilyMetadata.min_index_interval, WITH_OPTIONS_VERSION_2_1_MIN_INDEX_INTERVAL);
		BOOST_CHECK_EQUAL(columnFamilyMetadata.max_index_interval, WITH_OPTIONS_VERSION_2_1_MAX_INDEX_INTERVAL);
	}
}

//Run a test case against the column metadata
BOOST_AUTO_TEST_CASE(column) {
	//Create a container for the column metadata
	CassColumnMeta columnMetadata;

	//Create a table with all the datatypes
	createTableAllDataTypes();

	//TODO: Implement checks for all column metadata
	//Force the boost test messages to be displayed
	boost::unit_test::unit_test_log_t::instance().set_threshold_level(boost::unit_test::log_messages);

	//Get the column metadata for the id column on the all data types table
	BOOST_REQUIRE_EQUAL(cass_meta_get_column2(schemaMetadata_, SIMPLE_STRATEGY_KEYSPACE_NAME, ALL_DATA_TYPES_TABLE_NAME, "id", &columnMetadata), CASS_OK);
	BOOST_TEST_MESSAGE("Column Metadata: " << columnMetadata.name.data);
	BOOST_TEST_MESSAGE("\tcomponent_index: " << (int) columnMetadata.component_index);
	BOOST_TEST_MESSAGE("\tindex_name: " << columnMetadata.index_name.data);
	BOOST_TEST_MESSAGE("\tindex_options: " << columnMetadata.index_options.data);
	BOOST_TEST_MESSAGE("\tindex_type: " << columnMetadata.index_type.data);
	BOOST_TEST_MESSAGE("\tkind: " << test_utils::get_column_type(columnMetadata.kind));
	BOOST_TEST_MESSAGE("\ttype: " << test_utils::get_value_type(columnMetadata.type));
	BOOST_TEST_MESSAGE("\tis_reversed: " << (columnMetadata.is_reversed ? "true" : "false"));
	BOOST_TEST_MESSAGE("\tvalidator: " << columnMetadata.validator.data);
	
	//Get the column metadata for the text column on the all data types table
	BOOST_REQUIRE_EQUAL(cass_meta_get_column2(schemaMetadata_, SIMPLE_STRATEGY_KEYSPACE_NAME, ALL_DATA_TYPES_TABLE_NAME, "text_sample", &columnMetadata), CASS_OK);
	BOOST_TEST_MESSAGE("Column Metadata: " << columnMetadata.name.data);
	BOOST_TEST_MESSAGE("\tcomponent_index: " << (int) columnMetadata.component_index);
	BOOST_TEST_MESSAGE("\tindex_name: " << columnMetadata.index_name.data);
	BOOST_TEST_MESSAGE("\tindex_options: " << columnMetadata.index_options.data);
	BOOST_TEST_MESSAGE("\tindex_type: " << columnMetadata.index_type.data);
	BOOST_TEST_MESSAGE("\tkind: " << test_utils::get_column_type(columnMetadata.kind));
	BOOST_TEST_MESSAGE("\ttype: " << test_utils::get_value_type(columnMetadata.type));
	BOOST_TEST_MESSAGE("\tis_reversed: " << (columnMetadata.is_reversed ? "true" : "false"));
	BOOST_TEST_MESSAGE("\tvalidator: " << columnMetadata.validator.data);
	
	//Get the column metadata for the int column on the all data types table
	BOOST_REQUIRE_EQUAL(cass_meta_get_column2(schemaMetadata_, SIMPLE_STRATEGY_KEYSPACE_NAME, ALL_DATA_TYPES_TABLE_NAME, "int_sample", &columnMetadata), CASS_OK);
	BOOST_TEST_MESSAGE("Column Metadata: " << columnMetadata.name.data);
	BOOST_TEST_MESSAGE("\tcomponent_index: " << (int) columnMetadata.component_index);
	BOOST_TEST_MESSAGE("\tindex_name: " << columnMetadata.index_name.data);
	BOOST_TEST_MESSAGE("\tindex_options: " << columnMetadata.index_options.data);
	BOOST_TEST_MESSAGE("\tindex_type: " << columnMetadata.index_type.data);
	BOOST_TEST_MESSAGE("\tkind: " << test_utils::get_column_type(columnMetadata.kind));
	BOOST_TEST_MESSAGE("\ttype: " << test_utils::get_value_type(columnMetadata.type));
	BOOST_TEST_MESSAGE("\tis_reversed: " << (columnMetadata.is_reversed ? "true" : "false"));
	BOOST_TEST_MESSAGE("\tvalidator: " << columnMetadata.validator.data);
	
	//Get the column metadata for the bigint column on the all data types table
	BOOST_REQUIRE_EQUAL(cass_meta_get_column2(schemaMetadata_, SIMPLE_STRATEGY_KEYSPACE_NAME, ALL_DATA_TYPES_TABLE_NAME, "bigint_sample", &columnMetadata), CASS_OK);
	BOOST_TEST_MESSAGE("Column Metadata: " << columnMetadata.name.data);
	BOOST_TEST_MESSAGE("\tcomponent_index: " << (int) columnMetadata.component_index);
	BOOST_TEST_MESSAGE("\tindex_name: " << columnMetadata.index_name.data);
	BOOST_TEST_MESSAGE("\tindex_options: " << columnMetadata.index_options.data);
	BOOST_TEST_MESSAGE("\tindex_type: " << columnMetadata.index_type.data);
	BOOST_TEST_MESSAGE("\tkind: " << test_utils::get_column_type(columnMetadata.kind));
	BOOST_TEST_MESSAGE("\ttype: " << test_utils::get_value_type(columnMetadata.type));
	BOOST_TEST_MESSAGE("\tis_reversed: " << (columnMetadata.is_reversed ? "true" : "false"));
	BOOST_TEST_MESSAGE("\tvalidator: " << columnMetadata.validator.data);
	
	//Get the column metadata for the float column on the all data types table
	BOOST_REQUIRE_EQUAL(cass_meta_get_column2(schemaMetadata_, SIMPLE_STRATEGY_KEYSPACE_NAME, ALL_DATA_TYPES_TABLE_NAME, "float_sample", &columnMetadata), CASS_OK);
	BOOST_TEST_MESSAGE("Column Metadata: " << columnMetadata.name.data);
	BOOST_TEST_MESSAGE("\tcomponent_index: " << (int) columnMetadata.component_index);
	BOOST_TEST_MESSAGE("\tindex_name: " << columnMetadata.index_name.data);
	BOOST_TEST_MESSAGE("\tindex_options: " << columnMetadata.index_options.data);
	BOOST_TEST_MESSAGE("\tindex_type: " << columnMetadata.index_type.data);
	BOOST_TEST_MESSAGE("\tkind: " << test_utils::get_column_type(columnMetadata.kind));
	BOOST_TEST_MESSAGE("\ttype: " << test_utils::get_value_type(columnMetadata.type));
	BOOST_TEST_MESSAGE("\tis_reversed: " << (columnMetadata.is_reversed ? "true" : "false"));
	BOOST_TEST_MESSAGE("\tvalidator: " << columnMetadata.validator.data);
	
	//Get the column metadata for the double column on the all data types table
	BOOST_REQUIRE_EQUAL(cass_meta_get_column2(schemaMetadata_, SIMPLE_STRATEGY_KEYSPACE_NAME, ALL_DATA_TYPES_TABLE_NAME, "double_sample", &columnMetadata), CASS_OK);
	BOOST_TEST_MESSAGE("Column Metadata: " << columnMetadata.name.data);
	BOOST_TEST_MESSAGE("\tcomponent_index: " << (int) columnMetadata.component_index);
	BOOST_TEST_MESSAGE("\tindex_name: " << columnMetadata.index_name.data);
	BOOST_TEST_MESSAGE("\tindex_options: " << columnMetadata.index_options.data);
	BOOST_TEST_MESSAGE("\tindex_type: " << columnMetadata.index_type.data);
	BOOST_TEST_MESSAGE("\tkind: " << test_utils::get_column_type(columnMetadata.kind));
	BOOST_TEST_MESSAGE("\ttype: " << test_utils::get_value_type(columnMetadata.type));
	BOOST_TEST_MESSAGE("\tis_reversed: " << (columnMetadata.is_reversed ? "true" : "false"));
	BOOST_TEST_MESSAGE("\tvalidator: " << columnMetadata.validator.data);
	
	//Get the column metadata for the real column on the all data types table
	BOOST_REQUIRE_EQUAL(cass_meta_get_column2(schemaMetadata_, SIMPLE_STRATEGY_KEYSPACE_NAME, ALL_DATA_TYPES_TABLE_NAME, "decimal_sample", &columnMetadata), CASS_OK);
	BOOST_TEST_MESSAGE("Column Metadata: " << columnMetadata.name.data);
	BOOST_TEST_MESSAGE("\tcomponent_index: " << (int) columnMetadata.component_index);
	BOOST_TEST_MESSAGE("\tindex_name: " << columnMetadata.index_name.data);
	BOOST_TEST_MESSAGE("\tindex_options: " << columnMetadata.index_options.data);
	BOOST_TEST_MESSAGE("\tindex_type: " << columnMetadata.index_type.data);
	BOOST_TEST_MESSAGE("\tkind: " << test_utils::get_column_type(columnMetadata.kind));
	BOOST_TEST_MESSAGE("\ttype: " << test_utils::get_value_type(columnMetadata.type));
	BOOST_TEST_MESSAGE("\tis_reversed: " << (columnMetadata.is_reversed ? "true" : "false"));
	BOOST_TEST_MESSAGE("\tvalidator: " << columnMetadata.validator.data);
	
	//Get the column metadata for the blob column on the all data types table
	BOOST_REQUIRE_EQUAL(cass_meta_get_column2(schemaMetadata_, SIMPLE_STRATEGY_KEYSPACE_NAME, ALL_DATA_TYPES_TABLE_NAME, "blob_sample", &columnMetadata), CASS_OK);
	BOOST_TEST_MESSAGE("Column Metadata: " << columnMetadata.name.data);
	BOOST_TEST_MESSAGE("\tcomponent_index: " << (int) columnMetadata.component_index);
	BOOST_TEST_MESSAGE("\tindex_name: " << columnMetadata.index_name.data);
	BOOST_TEST_MESSAGE("\tindex_options: " << columnMetadata.index_options.data);
	BOOST_TEST_MESSAGE("\tindex_type: " << columnMetadata.index_type.data);
	BOOST_TEST_MESSAGE("\tkind: " << test_utils::get_column_type(columnMetadata.kind));
	BOOST_TEST_MESSAGE("\ttype: " << test_utils::get_value_type(columnMetadata.type));
	BOOST_TEST_MESSAGE("\tis_reversed: " << (columnMetadata.is_reversed ? "true" : "false"));
	BOOST_TEST_MESSAGE("\tvalidator: " << columnMetadata.validator.data);
	
	//Get the column metadata for the boolean column on the all data types table
	BOOST_REQUIRE_EQUAL(cass_meta_get_column2(schemaMetadata_, SIMPLE_STRATEGY_KEYSPACE_NAME, ALL_DATA_TYPES_TABLE_NAME, "boolean_sample", &columnMetadata), CASS_OK);
	BOOST_TEST_MESSAGE("Column Metadata: " << columnMetadata.name.data);
	BOOST_TEST_MESSAGE("\tcomponent_index: " << (int) columnMetadata.component_index);
	BOOST_TEST_MESSAGE("\tindex_name: " << columnMetadata.index_name.data);
	BOOST_TEST_MESSAGE("\tindex_options: " << columnMetadata.index_options.data);
	BOOST_TEST_MESSAGE("\tindex_type: " << columnMetadata.index_type.data);
	BOOST_TEST_MESSAGE("\tkind: " << test_utils::get_column_type(columnMetadata.kind));
	BOOST_TEST_MESSAGE("\ttype: " << test_utils::get_value_type(columnMetadata.type));
	BOOST_TEST_MESSAGE("\tis_reversed: " << (columnMetadata.is_reversed ? "true" : "false"));
	BOOST_TEST_MESSAGE("\tvalidator: " << columnMetadata.validator.data);
	
	//Get the column metadata for the timestamp column on the all data types table
	BOOST_REQUIRE_EQUAL(cass_meta_get_column2(schemaMetadata_, SIMPLE_STRATEGY_KEYSPACE_NAME, ALL_DATA_TYPES_TABLE_NAME, "timestamp_sample", &columnMetadata), CASS_OK);
	BOOST_TEST_MESSAGE("Column Metadata: " << columnMetadata.name.data);
	BOOST_TEST_MESSAGE("\tcomponent_index: " << (int) columnMetadata.component_index);
	BOOST_TEST_MESSAGE("\tindex_name: " << columnMetadata.index_name.data);
	BOOST_TEST_MESSAGE("\tindex_options: " << columnMetadata.index_options.data);
	BOOST_TEST_MESSAGE("\tindex_type: " << columnMetadata.index_type.data);
	BOOST_TEST_MESSAGE("\tkind: " << test_utils::get_column_type(columnMetadata.kind));
	BOOST_TEST_MESSAGE("\ttype: " << test_utils::get_value_type(columnMetadata.type));
	BOOST_TEST_MESSAGE("\tis_reversed: " << (columnMetadata.is_reversed ? "true" : "false"));
	BOOST_TEST_MESSAGE("\tvalidator: " << columnMetadata.validator.data);
	
	//Get the column metadata for the inet column on the all data types table
	BOOST_REQUIRE_EQUAL(cass_meta_get_column2(schemaMetadata_, SIMPLE_STRATEGY_KEYSPACE_NAME, ALL_DATA_TYPES_TABLE_NAME, "inet_sample", &columnMetadata), CASS_OK);
	BOOST_TEST_MESSAGE("Column Metadata: " << columnMetadata.name.data);
	BOOST_TEST_MESSAGE("\tcomponent_index: " << (int) columnMetadata.component_index);
	BOOST_TEST_MESSAGE("\tindex_name: " << columnMetadata.index_name.data);
	BOOST_TEST_MESSAGE("\tindex_options: " << columnMetadata.index_options.data);
	BOOST_TEST_MESSAGE("\tindex_type: " << columnMetadata.index_type.data);
	BOOST_TEST_MESSAGE("\tkind: " << test_utils::get_column_type(columnMetadata.kind));
	BOOST_TEST_MESSAGE("\ttype: " << test_utils::get_value_type(columnMetadata.type));
	BOOST_TEST_MESSAGE("\tis_reversed: " << (columnMetadata.is_reversed ? "true" : "false"));
	BOOST_TEST_MESSAGE("\tvalidator: " << columnMetadata.validator.data);
}

//Run a test case against the schema metadata iterators
BOOST_AUTO_TEST_CASE(iterators) {
	//Go through each keyspace
	CassIterator* keyspaceIterator = cass_iterator_keyspaces(schemaMetadata_);
	while (cass_iterator_next(keyspaceIterator)) {
		//Ensure the keyspace metadata can be retrieved
		CassKeyspaceMeta keyspaceMetadata;
		BOOST_REQUIRE_EQUAL(cass_iterator_get_keyspace_meta(keyspaceIterator, &keyspaceMetadata), CASS_OK);

		//Go through each column family in the keyspace
		CassIterator* columnFamilyIterator = cass_iterator_column_families_from_keyspace(keyspaceIterator);
		while (cass_iterator_next(columnFamilyIterator)) {
			//Ensure the column family metadata can be retrieved
			CassColumnFamilyMeta columnFamilyMetadata;
			BOOST_REQUIRE_EQUAL(cass_iterator_get_column_family_meta(columnFamilyIterator, &columnFamilyMetadata), CASS_OK);

			//Go through each column in the column family
			CassIterator* columnIterator = cass_iterator_columns_from_column_family(columnFamilyIterator);
			while (cass_iterator_next(columnIterator)) {
				CassColumnMeta columnMetadata;
				BOOST_CHECK_EQUAL(cass_iterator_get_column_meta(columnIterator, &columnMetadata), CASS_OK);
			}
		}
	}
}


//Close the schema metadata test suite
BOOST_AUTO_TEST_SUITE_END()

