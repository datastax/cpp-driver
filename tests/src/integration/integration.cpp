/*
  Copyright (c) 2014-2016 DataStax

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
#include "integration.hpp"
#include "options.hpp"

#include <algorithm>
#include <cstdarg>
#include <iostream>
#include <sstream>
#include <fcntl.h>
#include <sys/stat.h>
#ifndef _WIN32
# include <time.h>
#endif

#ifdef _WIN32
# define FILE_MODE 0
#else
# define FILE_MODE S_IRWXU | S_IRWXG | S_IROTH
#endif
#define FILE_PATH_SIZE 1024

#define TRIM_DELIMETERS " \f\n\r\t\v"
#define FORMAT_BUFFER_SIZE 10240
#define KEYSPACE_MAXIMUM_LENGTH 48
#define SIMPLE_KEYSPACE_FORMAT "CREATE KEYSPACE %s WITH replication = { 'class': %s }"
#define SELECT_SERVER_VERSION "SELECT release_version FROM system.local"

Integration::Integration()
  : ccm_(NULL)
  , session_(NULL)
  , keyspace_name_("")
  , table_name_("")
  , uuid_generator_(new UuidGen())
  , server_version_(Options::server_version())
  , replication_factor_(0)
  , number_dc1_nodes_(1)
  , number_dc2_nodes_(0)
  , contact_points_("")
  , is_client_authentication_(false)
  , is_ssl_(false)
  , is_schema_metadata_(false)
  , create_keyspace_query_("")
  , is_ccm_start_requested_(true)
  , is_session_requested_(true) {
  // Get the name of the test and the case/suite it belongs to
  const testing::TestInfo* test_information = testing::UnitTest::GetInstance()->current_test_info();
  test_case_name_ = test_information->test_case_name();
  test_name_ = test_information->name();

  // Determine if file logging should be enabled for the integration tests
  if (Options::log_tests()) {
    logger_ = Logger(test_case_name_, test_name_);
  }
}

Integration::~Integration() {
  if (session_.get()) {
    session_->close();
  }
}

void Integration::SetUp() {
  // Generate the keyspace name
  keyspace_name_ = replace_all(to_lower(test_case_name_), "integrationtest", "") +
                   "_" + to_lower(test_name_);
  if (keyspace_name_.size() > KEYSPACE_MAXIMUM_LENGTH) {
    // Update the keyspace name with a UUID (first portions of v4 UUID)
    std::vector<std::string> uuid_octets = explode(uuid_generator_->generate_timeuuid().str(), '-');
    std::string id = uuid_octets[0] + uuid_octets[3];
    keyspace_name_ = keyspace_name_.substr(0, KEYSPACE_MAXIMUM_LENGTH - id.size()) +
                     id;
  }

  // Generate the table name
  table_name_ = to_lower(test_name_);

  // Determine the replication strategy and generate the keyspace query
  std::stringstream replication_strategy;
  if (number_dc1_nodes_ > 0) {
    replication_strategy << "'NetworkTopologyStrategy', 'dc1': " <<
                            number_dc1_nodes_ << ", " <<
                            "'dc2': " << number_dc2_nodes_;
  } else {
    replication_strategy << "'SimpleStrategy', 'replication_factor': ";
    if (replication_factor_ == 0) {
      // Calculate the default replication factor
      replication_factor_ = (number_dc1_nodes_ % 2 == 0) ? number_dc1_nodes_ / 2 : (number_dc1_nodes_ + 1) / 2;
    }
    replication_strategy << replication_factor_;
  }
  create_keyspace_query_ = format_string(SIMPLE_KEYSPACE_FORMAT,
                                         keyspace_name_.c_str(),
                                         replication_strategy.str().c_str());

  try {
    //Create and start the CCM cluster (if not already created)
    ccm_ = new CCM::Bridge(server_version_, Options::use_git(),
      Options::is_dse(), Options::cluster_prefix(),
      Options::dse_credentials(),
      Options::dse_username(), Options::dse_password(),
      Options::deployment_type(), Options::authentication_type(),
      Options::host(), Options::port(),
      Options::username(), Options::password(),
      Options::public_key(), Options::private_key());
    if (ccm_->create_cluster(number_dc1_nodes_,
      number_dc2_nodes_,
      is_ssl_,
      is_client_authentication_)) {
      if (is_ccm_start_requested_) {
        ccm_->start_cluster();
      }
    }

    // Generate the default contact points
    contact_points_ = generate_contact_points(ccm_->get_ip_prefix(),
                      number_dc1_nodes_ + number_dc2_nodes_);

    // Determine if the session connection should be established
    if (is_session_requested_) {
      // Create the cluster configuration and establish the session connection
      cluster_ = Cluster::build()
        .with_contact_points(contact_points_)
        .with_schema_metadata(is_schema_metadata_ ? cass_true : cass_false);
      connect(cluster_);
    }
  } catch (CCM::BridgeException be) {
    // Issue creating the CCM bridge instance (force failure)
    FAIL() << be.what();
  }
}

void Integration::TearDown() {
  // Drop keyspace for integration test (may or may have not been created)
  if (session_.get()) {
    std::stringstream use_keyspace_query;
    use_keyspace_query << "DROP KEYSPACE " << keyspace_name_;
    session_->execute(use_keyspace_query.str(), CASS_CONSISTENCY_ANY, false);
  }
}

void Integration::connect(Cluster cluster) {
  // Establish the session connection
  session_ = cluster.connect();
  CHECK_FAILURE;

  // Create the keyspace for the integration test
  session_->execute(create_keyspace_query_);
  CHECK_FAILURE;

  // Update the session to use the new keyspace by default
  std::stringstream use_keyspace_query;
  use_keyspace_query << "USE " << keyspace_name_;
  session_->execute(use_keyspace_query.str());
}

std::string Integration::generate_contact_points(const std::string& ip_prefix,
                                                 size_t number_of_nodes) const {
  // Iterate over the total number of nodes to create the contact list
  std::vector<std::string> contact_points;
  for (size_t i = 1; i <= number_of_nodes; ++i) {
    std::stringstream contact_point;
    contact_point << ip_prefix << i;
    contact_points.push_back(contact_point.str());
  }
  return implode(contact_points, ',');
}

std::string Integration::format_string(const char* format, ...) const {
  // Create a buffer for the formatting of the string
  char buffer[FORMAT_BUFFER_SIZE] = { '\0' };

  // Parse the arguments into the buffer
  va_list args;
  va_start(args, format);
  vsnprintf(buffer, FORMAT_BUFFER_SIZE, format, args);
  va_end(args);

  // Return the formatted string
  return buffer;
}

std::string Integration::to_lower(const std::string& input) {
  std::string lowercase = input;
  std::transform(lowercase.begin(), lowercase.end(), lowercase.begin(), ::tolower);
  return lowercase;
}

std::string Integration::trim(const std::string& input) {
  std::string result;
  if (!input.empty()) {
    // Trim right
    result = input.substr(0, input.find_last_not_of(TRIM_DELIMETERS) + 1);
    if (!result.empty()) {
      // Trim left
      result = result.substr(result.find_first_not_of(TRIM_DELIMETERS));
    }
  }
  return result;
}

std::string Integration::implode(const std::vector<std::string>& elements,
  const char delimiter /*= ' '*/) const {
  // Iterate through each element in the vector and concatenate the string
  std::string result;
  for (std::vector<std::string>::const_iterator iterator = elements.begin(); iterator < elements.end(); ++iterator) {
    result += *iterator;
    if ((iterator + 1) != elements.end()) {
      result += delimiter;
    }
  }
  return result;
}

std::vector<std::string> Integration::explode(const std::string& input,
  const char delimiter /*= ' '*/) {
  // Iterate over the input line and parse the tokens
  std::vector<std::string> result;
  std::istringstream parser(input);
  for (std::string token; std::getline(parser, token, delimiter);) {
    if (!token.empty()) {
      result.push_back(trim(token));
    }
  }
  return result;
}

std::string Integration::replace_all(const std::string& input,
  const std::string& from,
  const std::string& to) const {
  size_t position = 0;
  std::string result = input;
  while((position = result.find(from, position)) != std::string::npos) {
    result.replace(position, from.length(), to);
    // Handle the case where 'to' is a substring of 'from'
    position += to.length();
  }
  return result;
}

void Integration::msleep(unsigned int milliseconds) {
#ifdef _WIN32
  Sleep(milliseconds);
#else
  //Convert the milliseconds into a proper timespec structure
  struct timespec requested = { 0 };
  time_t seconds = static_cast<int>(milliseconds / 1000);
  long int nanoseconds = static_cast<long int>((milliseconds - (seconds * 1000)) * 1000000);

  //Assign the requested time and perform sleep
  requested.tv_sec = seconds;
  requested.tv_nsec = nanoseconds;
  while (nanosleep(&requested, &requested) == -1) {
    continue;
  }
#endif
}

std::string Integration::cwd() {
  char cwd[FILE_PATH_SIZE] = { 0 };
  size_t cwd_length = sizeof(cwd);
  uv_cwd(cwd, &cwd_length);
  return std::string(cwd, cwd_length);
}

bool Integration::file_exists(const std::string& filename) {
  uv_fs_t request;
  int error_code = uv_fs_open(NULL, &request, filename.c_str(), O_RDONLY, 0, NULL);
  uv_fs_req_cleanup(&request);
  return error_code != UV_ENOENT;
}

void Integration::mkdir(const std::string& path) {
  // Create a synchronous libuv file system call to create the path
  uv_loop_t* loop = uv_default_loop();
  uv_fs_t request;
  uv_fs_mkdir(loop, &request, path.c_str(), FILE_MODE, NULL);
  uv_run(loop, UV_RUN_DEFAULT);
  uv_fs_req_cleanup(&request);
}

