/*
  Copyright (c) DataStax, Inc.

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

#ifndef __INTEGRATION_HPP__
#define __INTEGRATION_HPP__

#include <gtest/gtest.h>

#include <iostream>
#include <string>
#include <vector>

#include "cassandra.h"

#include "bridge.hpp"
#include "logger.hpp"
#include "objects.hpp"
#include "options.hpp"
#include "policies.hpp"
#include "pretty_print.hpp"
#include "test_utils.hpp"
#include "tlog.hpp"
#include "values.hpp"

// Macros for grouping tests together
#define GROUP_TEST_F(group_name, test_case, test_name) TEST_F(test_case, group_name##_##test_name)
#define GROUP_TEST(group_name, test_case, test_name) TEST(test_case, group_name##_##test_name)
#define GROUP_TYPED_TEST_P(group_name, test_case, test_name) \
  TYPED_TEST_P(test_case, group_name##_##test_name)

// Macros to use for grouping integration tests together
#define INTEGRATION_TEST(server_type, test_case, test_name) \
  GROUP_TEST(Integration##_##server_type, test_case, test_name)
#define INTEGRATION_TEST_F(server_type, test_case, test_name) \
  GROUP_TEST_F(Integration##_##server_type, test_case, test_name)
#define INTEGRATION_TYPED_TEST_P(server_type, test_case, test_name) \
  GROUP_TYPED_TEST_P(Integration##_##server_type, test_case, test_name)
#define INTEGRATION_DISABLED_TEST_F(server_type, test_case, test_name) \
  GROUP_TEST_F(DISABLED##_##Integration##_##server_type, test_case, test_name)
#define INTEGRATION_DISABLED_TYPED_TEST_P(server_type, test_case, test_name) \
  GROUP_TYPED_TEST_P(DISABLED##_##Integration##_##server_type, test_case, est_name)

// Macros to use for grouping Cassandra integration tests together
#define CASSANDRA_INTEGRATION_TEST(test_case, test_name) \
  INTEGRATION_TEST(Cassandra, test_case, test_name)
#define CASSANDRA_INTEGRATION_TEST_F(test_case, test_name) \
  INTEGRATION_TEST_F(Cassandra, test_case, test_name)
#define CASSANDRA_INTEGRATION_TYPED_TEST_P(test_case, test_name) \
  INTEGRATION_TYPED_TEST_P(Cassandra, test_case, test_name)
#define CASSANDRA_INTEGRATION_DISABLED_TEST_F(test_case, test_name) \
  INTEGRATION_DISABLED_TEST_F(Cassandra, test_case, test_name)
#define CASSANDRA_INTEGRATION_DISABLED_TYPED_TEST_P(test_case, test_name) \
  INTEGRATION_DISABLED_TYPED_TEST_P(Cassandra, test_case, test_name)

// TODO: Create SKIP_SUITE macro; reduces noise and makes sense for certain suites

#define SKIP_TEST(message)                                \
  if (!Integration::skipped_message_displayed_) {         \
    std::cout << "[ SKIPPED  ] " << message << std::endl; \
    Integration::skipped_message_displayed_ = true;       \
  }                                                       \
  return;

#define CHECK_FAILURE       \
  if (this->HasFailure()) { \
    return;                 \
  }

#define SERVER_VERSION_SKIP_TEST_MESSAGE(server_version_string, comparison, version_string)                     \
  "Unsupported for Apache Cassandra Version " << server_version_string << ": Server version is " \
  << comparison << " the specified version " << version_string

#define PROTOCOL_VERSION_SKIP_TEST_MESSAGE(server_version_string, comparison, version_string)                     \
  "Unsupported for Apache Cassandra protocol version " << server_version_string << ": Server version is " \
  << comparison << " the specified protocol version " << version_string

/* Maintain existing behaviour; default message indicates server < specified */
#define SKIP_TEST_VERSION(server_version_string, version_string)           \
  SKIP_TEST(SERVER_VERSION_SKIP_TEST_MESSAGE(server_version_string, '<', version_string))

#define CHECK_VERSION(version)                                                      \
  do {                                                                              \
    CCM::CassVersion cass_version = this->server_version_;                          \
    if (!Options::is_cassandra()) {                                                 \
      cass_version = static_cast<CCM::DseVersion>(cass_version).get_cass_version(); \
    }                                                                               \
    if (cass_version < #version) {                                                  \
      SKIP_TEST_VERSION(cass_version.to_string(), #version)                         \
    }                                                                               \
  } while (0)

/* CPP-967 Skip test iff server version matches major.minor of specified version but
   patch version is >= the specified patch level of the specified version.  This
   enables the concise expression of a single constraint based on multiple Cassandra
   server versions.

   Also note that the sense of the comaprison is inverted from what we get in
   CHECK_VERSION.  There we want to make sure we have a version greater than what's
   specified in the constraint.  Here we want to make sure we have something less than
   what's specified (since the behaviour under test is fixed in later versions of
   Cassandra) */
#define CHECK_VERSIONS(version_string)                                              \
  do {                                                                              \
    CCM::CassVersion cass_version = this->server_version_;                          \
    if (!Options::is_cassandra()) {                                                 \
      cass_version = static_cast<CCM::DseVersion>(cass_version).get_cass_version(); \
    }                                                                               \
    std::vector<std::string> versions = Utils::explode(version_string,',');         \
    for (unsigned int i = 0; i < versions.size(); i++) {                            \
      CCM::CassVersion version = CCM::CassVersion(versions[i]);                     \
      if (cass_version.major_version == version.major_version &&                    \
          cass_version.minor_version == version.minor_version &&                    \
          cass_version.patch_version >= version.patch_version) {                    \
        SKIP_TEST(                                                                  \
          SERVER_VERSION_SKIP_TEST_MESSAGE(                                         \
            cass_version.to_string(), ">=", version.to_string()))                   \
      }                                                                             \
    }                                                                               \
  } while (0)

#define CHECK_PROTOCOL_VERSION(version)            \
  do {                                             \
    int proto_version = this->protocol_version_;   \
    if (proto_version < version) {                 \
      SKIP_TEST(                                   \
        PROTOCOL_VERSION_SKIP_TEST_MESSAGE(        \
          proto_version, '<', version));           \
    }                                              \
  } while (0)

#define CHECK_OPTIONS_VERSION(version)                                 \
  if (Options::server_version() < #version) {                          \
    SKIP_TEST_VERSION(Options::server_version().to_string(), #version) \
  }

#define CHECK_VALUE_TYPE_VERSION(type)                                            \
  CCM::CassVersion cass_version = this->server_version_;                          \
  if (!Options::is_cassandra()) {                                                 \
    cass_version = static_cast<CCM::DseVersion>(cass_version).get_cass_version(); \
  }                                                                               \
  if (cass_version < type::supported_server_version()) {                          \
    SKIP_TEST_VERSION(cass_version.to_string(), type::supported_server_version()) \
  }

#define CHECK_CONTINUE(flag, message) ASSERT_TRUE(flag) << message;

#define CASSANDRA_KEY_VALUE_TABLE_FORMAT \
  "CREATE TABLE IF NOT EXISTS %s (key %s PRIMARY KEY, value %s)"
#define CASSANDRA_KEY_VALUE_QUALIFIED_TABLE_FORMAT \
  "CREATE TABLE IF NOT EXISTS %s.%s (key %s PRIMARY KEY, value %s)"
#define CASSANDRA_KEY_VALUE_INSERT_FORMAT "INSERT INTO %s (key, value) VALUES(%s, %s)"
#define CASSANDRA_KEY_VALUE_QUALIFIED_INSERT_FORMAT "INSERT INTO %s.%s (key, value) VALUES(%s, %s)"
#define CASSANDRA_SELECT_VALUE_FORMAT "SELECT value FROM %s WHERE key=%s"
#define CASSANDRA_DELETE_ROW_FORMAT "DELETE FROM %s WHERE key=%s"
#define CASSANDRA_UPDATE_VALUE_FORMAT "UPDATE %s SET value=%s WHERE key=%s"
#define SELECT_ALL_SYSTEM_LOCAL_CQL "SELECT * FROM system.local"
#define SELECT_COUNT_FORMAT "SELECT COUNT(*) FROM %s LIMIT 1000000"

#define CASSANDRA_COMPOSITE_KEY_VALUE_TABLE_FORMAT                                         \
  "CREATE TABLE IF NOT EXISTS %s (primary_key %s, column_key timeuuid, value %s, PRIMARY " \
  "KEY(primary_key, column_key))"
#define CASSANDRA_COMPOSITE_KEY_VALUE_INSERT_FORMAT \
  "INSERT INTO %s (primary_key, column_key, value) VALUES(%s, %s, %s)"
#define CASSANDRA_COMPOSITE_SELECT_VALUE_FORMAT "SELECT value FROM %s WHERE primary_key=%s"

using namespace test;
using namespace test::driver;

/**
 * Statement type enumeration to use for specifying type of statement to use
 * when executing queries
 */
enum StatementType {
  /**
   * Batch statement
   */
  STATEMENT_TYPE_BATCH,
  /**
   * Prepared statement
   */
  STATEMENT_TYPE_PREPARED,
  /**
   * Simple statement
   */
  STATEMENT_TYPE_SIMPLE
};

/**
 * Base class to provide common integration test functionality
 */
class Integration : public testing::Test {
public:
  Integration();

  virtual ~Integration();

  virtual void SetUp();

  virtual void TearDown();

protected:
  /**
   * Flag to indicate the skipped message display state
   */
  static bool skipped_message_displayed_;
  /**
   * Handle for interacting with CCM
   */
  SharedPtr<CCM::Bridge, StdDeleter<CCM::Bridge> > ccm_;
  /**
   * Logger instance for handling driver log messages
   */
  driver::Logger logger_;
  /**
   * Cluster instance
   */
  Cluster cluster_;
  /**
   * Connected database session
   */
  Session session_;
  /**
   * Generated keyspace name for the integration test
   */
  std::string keyspace_name_;
  /**
   * Generated table name for the integration test
   */
  std::string table_name_;
  /**
   * Keyspaces schema table
   */
  std::string system_schema_keyspaces_;
  /**
   * UUID generator
   */
  UuidGen uuid_generator_;
  /**
   * Version of Cassandra/DSE the session is connected to
   */
  CCM::CassVersion server_version_;
  /**
   * Number of nodes in data center one
   * (DEFAULT: 1)
   */
  unsigned short number_dc1_nodes_;
  /**
   * Number of nodes in data center two
   * (DEFAULT: 0)
   */
  unsigned short number_dc2_nodes_;
  /**
   * Replication factor override; default is calculated based on number of data
   * center nodes; single data center is (nodes / 2) rounded up
   */
  unsigned short replication_factor_;
  /**
   * Replication configuration strategy
   */
  std::string replication_strategy_;
  /**
   * Default contact points generated based on the number of nodes requested
   */
  std::string contact_points_;
  /**
   * Setting for password authenticator. True if password authenticator should
   * be enabled; false otherwise.
   * (DEFAULT: false)
   *
   * NOTE: Username and password is 'cassandra'
   */
  bool is_password_authenticator_;
  /**
   * Setting for client authentication. True if client authentication should be
   * enabled; false otherwise.
   * (DEFAULT: false)
   */
  bool is_client_authentication_;
  /**
   * Setting for SSL authentication. True if SSL should be enabled; false
   * otherwise.
   * (DEFAULT: false)
   */
  bool is_ssl_;
  /**
   * Setting for v-nodes usage. True if v-nodes should be enabled; false
   * otherwise.
   * (DEFAULT: false)
   */
  bool is_with_vnodes_;
  /**
   * Setting for randomized contact points. True if randomized contact points
   * should be enabled; false otherwise.
   * (DEFAULT: false)
   */
  bool is_randomized_contact_points_;
  /**
   * Setting for schema metadata. True if schema metadata should be enabled;
   * false otherwise.
   * (DEFAULT: false)
   */
  bool is_schema_metadata_;
  /**
   * Setting to determine if CCM instance should be created. True if CCM instance
   * should be created; false otherwise.
   * (DEFAULT: true)
   */
  bool is_ccm_requested_;
  /**
   * Setting to determine if CCM cluster should be started. True if CCM cluster
   * should be started; false otherwise.
   * (DEFAULT: true)
   */
  bool is_ccm_start_requested_;
  /**
   * Setting to determine if CCM cluster should be started normally or nodes
   * should be started individually (and in order). True if CCM cluster should
   * be started by starting all node individually; false otherwise (start
   * cluster normally).
   * (DEFAULT: false)
   */
  bool is_ccm_start_node_individually_;
  /**
   * Setting to determine if session connection should be established. True if
   * session connection should be established; false otherwise.
   * (DEFAULT: true)
   */
  bool is_session_requested_;
  /**
   * Flag to indicate if the newly created keyspace should be set for the session connection.
   * (DEFAULT: true)
   */
  bool is_keyspace_change_requested_;
  /**
   * Flag to indicate if a test is chaotic and should have its CCM cluster
   * destroyed
   */
  bool is_test_chaotic_;
  /**
   * Workload to apply to the cluster
   */
  std::vector<CCM::DseWorkload> dse_workload_;
  /**
   * Execution profiles to associate with default cluster
   */
  ExecutionProfile::Map profiles_;
  /**
   * Protocol version to associate with default cluster
   */
  int protocol_version_;
  /**
   * Name of the test case/suite
   */
  std::string test_case_name_;
  /**
   * Name of the test
   */
  std::string test_name_;
  /**
   * Vector of nodes that have been stopped
   */
  std::vector<unsigned int> stopped_nodes_;
  /**
   * Vector of nodes that have been paused
   */
  std::vector<unsigned int> paused_nodes_;

  /**
   * Get the default keyspace name (based on the current test case and test
   * name)
   *
   * @return Default keyspace name
   */
  virtual std::string default_keyspace();

  /**
   * Get the default replication factor (based on the number of nodes in the
   * standard two data center configuration for the test harness)
   *
   * @return Default replication factor
   */
  virtual unsigned short default_replication_factor();

  /**
   * Get the default replication strategy for the keyspace (based on the
   * default replication factor or the overridden value assigned during the test
   * case setup process)
   *
   * @return  Default replication strategy
   */
  virtual std::string default_replication_strategy();

  /**
   * Get the default select all CQL statement
   *
   * @return Default CQL statement to select all elements in the row
   */
  virtual std::string default_select_all();

  /**
   * Get the row count using the default table name (based on the test name)
   *
   * @return The number of rows in the table
   */
  virtual int64_t default_select_count();

  /**
   * Get the default table name (based on the test name)
   *
   * @return  Default table name
   */
  virtual std::string default_table();

  /**
   * Drop a table from the current keyspace
   *
   * @param table_name Table to drop from the current keyspace
   */
  virtual void drop_table(const std::string& table_name);

  /**
   * Drop a type from the current keyspace
   *
   * @param type_name Table to drop from the current keyspace
   */
  virtual void drop_type(const std::string& type_name);

  /**
   * Update the current keyspace used by the session
   *
   * @param keyspace_name Keyspace to use
   * @return True if keyspace was changed; false otherwise
   */
  virtual bool use_keyspace(const std::string& keyspace_name);

  /**
   * Establish the session connection using provided cluster object.
   *
   * @param cluster Cluster object to use when creating session connection
   */
  virtual void connect(Cluster cluster);

  /**
   * Create the cluster configuration and establish the session connection using
   * provided cluster object.
   */
  virtual void connect();

  /**
   * Get the default cluster configuration
   *
   * @param is_with_default_contact_points True if default contact points
                                           should be added to the cluster
   * @return Cluster object (default)
   */
  virtual Cluster default_cluster(bool is_with_default_contact_points = true);

  /**
   * Enable/Disable tracing on the cluster
   *
   * @param enable True if tracing should be enabled on the cluster; false
   *               otherwise (default: true)
   */
  virtual void enable_cluster_tracing(bool enable = true);

  /**
   * Decommission a node and force the cluster to be removed after the test is
   * completed
   *
   * @param node Node that should be decommissioned
   * @oaram is_force True if decommission should be forced; false otherwise
   *                 (default: false)
   * @return True if node was decommissioned; false otherwise (the node is
   *         invalid or was already decommissioned)
   */
  virtual bool decommission_node(unsigned int node, bool is_force = false);

  /**
   * Decommission a node by force and ensure the cluster is set to be removed
   * after the test is completed
   *
   * NOTE: Alias for decommission_node(node, true)
   *
   * @param node Node that should be decommissioned forcefully
   * @return True if node was decommissioned; false otherwise (the node is
   *         invalid or was already decommissioned)
   */
  virtual bool force_decommission_node(unsigned int node);

  /**
   * Start a node that was previously stopped to ensure that it is not restarted after test is
   * completed; paused nodes are ignored
   *
   * @param node Node that should be started
   * @return True if node was started; false otherwise (the node is invalid or was already
   *         started or is paused)
   */
  virtual bool start_node(unsigned int node);

  /**
   * Stop a node that should be restarted after test is completed
   *
   * @param node Node that should be stopped
   * @param is_kill True if forced termination requested; false otherwise
   *                (default: false)
   * @return True if node was stopped; false otherwise (the node is invalid or
   *         was already stopped)
   */
  virtual bool stop_node(unsigned int node, bool is_kill = false);

  /**
   * Pause a node that should be resumed after test is completed
   *
   * @param node Node that should be paused
   * @return True if node was paused; false otherwise (the node is invalid or
   *         was already paused)
   */
  virtual bool pause_node(unsigned int node);

  /**
   * Resume a node that was previously paused to ensure that it is not resumed after test is
   * completed
   *
   * @param node Node that should be resumed
   * @return True if node was resumed; false otherwise
   */
  virtual bool resume_node(unsigned int node);

  /**
   * Generate the contact points for the cluster
   *
   * @param ip_address IP address prefix
   * @param number_of_nodes Total number of nodes in the cluster
   * @return Comma delimited IP address (e.g. contact points)
   */
  std::string generate_contact_points(const std::string& ip_prefix, size_t number_of_nodes);

  /**
   * Variable argument string formatter
   *
   * @param format Format string that follows printf specifications
   * @param ... Additional arguments; depends on the format string
   */
  std::string format_string(const char* format, ...) const;

  /**
   * Calculate the elapsed time in milliseconds
   *
   * @return Elapsed time in milliseconds
   */
  inline uint64_t elapsed_time() {
    if (start_time_ > 0) {
      return (uv_hrtime() - start_time_) / 1000000UL;
    }
    return 0;
  }

  /**
   * Start the timer to calculate the elapsed time
   */
  inline void start_timer() { start_time_ = uv_hrtime(); }

  /**
   * Stop the timer - Calculate the elapsed time and reset the timer
   *
   * @return Elapsed time in milliseconds
   */
  inline uint64_t stop_timer() {
    uint64_t duration = elapsed_time();
    start_time_ = 0ull;
    return duration;
  }

  /**
   * Get the current working directory
   *
   * @return Current working directory
   */
  inline static std::string cwd() { return Utils::cwd(); }

  /**
   * Determine if a string contains another string
   *
   * @param input String being evaluated
   * @param search String to find
   * @return True if string is contained in other string; false otherwise
   */
  inline static bool contains(const std::string& input, const std::string& search) {
    return Utils::contains(input, search);
  }

  /**
   * Split a string into an array/vector
   *
   * @param input String to convert to array/vector
   * @param delimiter Character to use split into elements (default: <space>)
   * @return An array/vector representation of the string
   */
  inline static std::vector<std::string> explode(const std::string& input,
                                                 const char delimiter = ' ') {
    return Utils::explode(input, delimiter);
  }

  /**
   * Check to see if a file exists
   *
   * @param filename Absolute/Relative filename
   * @return True if file exists; false otherwise
   */
  inline static bool file_exists(const std::string& filename) {
    return Utils::file_exists(filename);
  }

  /**
   * Concatenate or join a vector into a string
   *
   * @param elements Vector of strings to concatenate
   * @param delimiter Character to use between elements (default: <space>)
   * @return A string concatenating all the vector strings with delimiter
   *         separation
   */
  inline static std::string implode(const std::vector<std::string>& elements,
                                    const char delimiter = ' ') {
    return Utils::implode<std::string>(elements, delimiter);
  }

  /**
   * Create the directory from a path
   *
   * @param path Directory/Path to create
   */
  inline static void mkdir(const std::string& path) { Utils::mkdir(path); }

  /**
   * Cross platform millisecond granularity sleep
   *
   * @param milliseconds Time in milliseconds to sleep
   */
  inline static void msleep(unsigned int milliseconds) { Utils::msleep(milliseconds); }

  /**
   * Replace all occurrences of a string from the input string
   *
   * @param input String having occurrences being replaced
   * @param from String to find for replacement
   * @param to String to replace with
   * @return Input string with replacement
   */
  inline static std::string replace_all(const std::string& input, const std::string& from,
                                        const std::string& to) {
    return Utils::replace_all(input, from, to);
  }

  /**
   * Convert a string to lowercase
   *
   * @param input String to convert to lowercase
   */
  inline static std::string to_lower(const std::string& input) { return Utils::to_lower(input); }

  /**
   * Remove the leading and trailing whitespace from a string
   *
   * @param input String to trim
   * @return Trimmed string
   */
  inline static std::string trim(const std::string& input) { return Utils::trim(input); }

  /**
   * Shrink the given name if the name is longer than allowable by the server
   *
   * NOTE: This is for keyspaces, tables, and other misc server side items
   *
   * @param name Name to shrink if name too long
   */
  void maybe_shrink_name(std::string& name);

  /**
   * Wait for the logger count to reach expected count
   *
   * NOTE: This may wait up to LOGGER_MAXIMUM_WAIT_TIME_MS
   *
   * @param expected_count Expected logger count
   * @return True if expected count is equal to logger count; false otherwise
   */
  bool wait_for_logger(size_t expected_count);

  /**
   * Get the Murmur3 hash for a given value
   *
   * @param value Value to calculate Murmur3 hash for
   * @return Murmur3 hash for value
   */
  int64_t murmur3_hash(const std::string& value);

  /**
   * Get the time since epoch in microseconds
   *
   * @return Time since epoch in microseconds
   */
  uint64_t time_since_epoch_in_ms();

  /**
   * Get the time since epoch in microseconds
   *
   * @return Time since epoch in microseconds
   */
  uint64_t time_since_epoch_us();

  /**
   * Get the driver name as configured by the driver
   *
   * @return Driver name
   */
  std::string driver_name();

  /**
   * Get the driver version as configured by the driver
   *
   * @return Driver version
   */
  std::string driver_version();

private:
  /**
   * Keyspace creation query (generated via SetUp)
   */
  std::string create_keyspace_query_;
  /**
   * High-resolution real time when the timer was started (in nanoseconds)
   */
  uint64_t start_time_;
};

#endif //__INTEGRATION_HPP__
