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
#ifdef _WIN32
// Local execution command
#include <io.h>
#define FILENO _fileno
#define POPEN _popen
#define PCLOSE _pclose
#define READ _read

// Enable memory leak detection
# define _CRTDBG_MAP_ALLOC
# include <stdlib.h>
# include <crtdbg.h>

// Enable memory leak detection for new operator
# ifdef _DEBUG
#   ifndef DBG_NEW
#     define DBG_NEW new ( _NORMAL_BLOCK , __FILE__ , __LINE__ )
#     define new DBG_NEW
#   endif
# endif
#else
// Local execution command
#define FILENO fileno
#define POPEN popen
#define PCLOSE pclose
#define READ read

#include <unistd.h>
#endif
#include "bridge.hpp"

#ifdef CASS_USE_LIBSSH2
#include <libssh2.h>
#define LIBSSH2_INIT_ALL 0
#ifdef OPENSSL_CLEANUP
# define PID_UNKNOWN 0
# include <openssl/ssl.h>
# include <openssl/rand.h>
# include <openssl/engine.h>
# include <openssl/conf.h>
#endif
#else
#ifdef _MSC_VER
// ssize_t is defined in libssh2 and used by local command execution
typedef SSIZE_T ssize_t;
#endif
#endif

#include <errno.h>
#include <ctype.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <sstream>

// Create simple console logging functions
#define PREFIX_LOG std::cout
#define PREFIX_MESSAGE "CCM: "
#define SUFFIX_LOG std::endl
#ifdef CCM_VERBOSE_LOGGING
# define LOG(message) PREFIX_LOG << PREFIX_MESSAGE << message << SUFFIX_LOG
# define LOG_WARN(message) PREFIX_LOG << PREFIX_MESSAGE << "WARN: " << message << SUFFIX_LOG
#else
# define LOG_DISABLED do {} while (false)
# define LOG(message) LOG_DISABLED
# define LOG_WARN(message) LOG_DISABLED
#endif
#define LOG_ERROR(message) PREFIX_LOG << PREFIX_MESSAGE << "ERROR: " \
                           << __FILE__ << "(" << __LINE__ << "): " \
                           << message << SUFFIX_LOG

// Create FALSE/TRUE defines for easier code readability
#ifndef FALSE
# define FALSE 0
#endif
#ifndef TRUE
# define TRUE 1
#endif

#define TRIM_DELIMETERS " \f\n\r\t\v"
#define CASSANDRA_BINARY_PORT 9042
#define CASSANDRA_STORAGE_PORT 7000
#define CASSANDRA_THRIFT_PORT 9160
#define CCM_NAP 100
#define CCM_RETRIES 100 // Up to 10 seconds for retry based on CCM_NAP

// CCM node status
#define CCM_NODE_STATUS_DECOMMISSIONED "decommissioned"
#define CCM_NODE_STATUS_DOWN "down"
#define CCM_NODE_STATUS_UNINITIALIZED "(not initialized)"
#define CCM_NODE_STATUS_UP "up"

// Configuration file setting keys
#define CCM_CONFIGURATION_KEY_CASSANDRA_VERSION "cassandra_version"
#define CCM_CONFIGURATION_KEY_USE_GIT "use_git"
#define CCM_CONFIGURATION_KEY_BRANCH_TAG "branch_tag"
#define CCM_CONFIGURATION_KEY_USE_INSTALL_DIR "use_install_dir"
#define CCM_CONFIGURATION_KEY_INSTALL_DIR "install_dir"
#define CCM_CONFIGURATION_KEY_DEPLOYMENT_TYPE "deployment_type"
#define CCM_CONFIGURATION_KEY_USE_DSE "use_dse"
#define CCM_CONFIGURATION_KEY_DSE_VERSION "dse_version"
#define CCM_CONFIGURATION_KEY_DSE_CREDENTIALS_TYPE "dse_credentials_type"
#define CCM_CONFIGURATION_KEY_DSE_USERNAME "dse_username"
#define CCM_CONFIGURATION_KEY_DSE_PASSWORD "dse_password"
#define CCM_CONFIGURATION_KEY_AUTHENTICATION_TYPE "authentication_type"
#define CCM_CONFIGURATION_KEY_HOST "host"
#define CCM_CONFIGURATION_KEY_SSH_PORT "ssh_port"
#define CCM_CONFIGURATION_KEY_SSH_USERNAME "ssh_username"
#define CCM_CONFIGURATION_KEY_SSH_PASSWORD "ssh_password"
#define CCM_CONFIGURATION_KEY_SSH_PUBLIC_KEY "ssh_public_key"
#define CCM_CONFIGURATION_KEY_SSH_PRIVATE_KEY "ssh_private_key"

// Workload value initialization
const std::string DSE_WORKLOADS[] = {
  "cassandra",
  "cfs",
  "dsefs",
  "graph",
  "hadoop",
  "solr",
  "spark"
};
const std::vector<std::string> CCM::Bridge::dse_workloads_(DSE_WORKLOADS,
  DSE_WORKLOADS + sizeof(DSE_WORKLOADS) / sizeof(DSE_WORKLOADS[0]));
const CCM::DseWorkload DEFAULT_WORKLOAD[] = {
  CCM::DSE_WORKLOAD_CASSANDRA
};
const std::vector<CCM::DseWorkload> CCM::Bridge::DEFAULT_DSE_WORKLOAD(
  DEFAULT_WORKLOAD, DEFAULT_WORKLOAD +
  sizeof(DEFAULT_WORKLOAD) / sizeof(DEFAULT_WORKLOAD[0]));

using namespace CCM;

CCM::Bridge::Bridge(CassVersion server_version /*= DEFAULT_CASSANDRA_VERSION*/,
  bool use_git /*= DEFAULT_USE_GIT*/,
  const std::string& branch_tag /* ""*/,
  bool use_install_dir /*=DEFAULT_USE_INSTALL_DIR*/,
  const std::string& install_dir /*=""*/,
  bool use_dse /*= DEFAULT_USE_DSE*/,
  std::vector<DseWorkload> dse_workload /*= DEFAULT_DSE_WORKLOAD*/,
  const std::string& cluster_prefix /*= DEFAULT_CLUSTER_PREFIX*/,
  DseCredentialsType dse_credentials_type /*= DEFAULT_DSE_CREDENTIALS*/,
  const std::string& dse_username /*= ""*/,
  const std::string& dse_password /*= ""*/,
  DeploymentType deployment_type /*= DEFAULT_DEPLOYMENT*/,
  AuthenticationType authentication_type /*= DEFAULT_AUTHENTICATION*/,
  const std::string& host /*= DEFAULT_HOST*/,
  short port /*= DEFAULT_REMOTE_DEPLOYMENT_PORT*/,
  const std::string& username /*= DEFAULT_USERNAME*/,
  const std::string& password /*= DEFAULT_PASSWORD*/,
  const std::string& public_key /*= ""*/,
  const std::string& private_key /*= ""*/)
  : cassandra_version_(server_version)
  , dse_version_(DEFAULT_DSE_VERSION)
  , use_git_(use_git)
  , branch_tag_(branch_tag)
  , use_install_dir_(use_install_dir)
  , install_dir_(install_dir)
  , use_dse_(use_dse)
  , dse_workload_(dse_workload)
  , cluster_prefix_(cluster_prefix)
  , authentication_type_(authentication_type)
  , dse_credentials_type_(dse_credentials_type)
  , dse_username_(dse_username)
  , dse_password_(dse_password)
#ifdef CASS_USE_LIBSSH2
  , deployment_type_(deployment_type)
  , host_(host)
  , session_(NULL)
  , channel_(NULL)
  , socket_(NULL) {
#else
  // Force local deployment only
  , deployment_type_(DeploymentType::LOCAL)
  , host_("127.0.0.1") {
#endif
#ifdef _WIN32
# ifdef _DEBUG
  // Enable automatic execution of the memory leak detection upon exit
  _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
# endif
#endif
  // Determine if DSE is being used
  if (use_dse_) {
    dse_version_ = DseVersion(server_version.to_string());
    cassandra_version_ = dse_version_.get_cass_version();
  }

  // Determine if installation directory can be used
  if (use_install_dir_ && install_dir_.empty()) {
    throw BridgeException("Unable to use Installation Directory: Directory must not be blank");
  }
#ifdef CASS_USE_LIBSSH2
  // Determine if libssh2 needs to be initialized
  if (deployment_type_ == DeploymentType::REMOTE) {
    // Initialize the socket
    try {
      initialize_socket(host_, port);
    } catch (SocketException &se) {
      // Re-throw the exception as a BridgeException
      finalize_libssh2();
      throw BridgeException(se.what());
    }

    // Initialize the libssh2 connection
    initialize_libssh2();

    // Authenticate and establish the libssh2 connection
    establish_libssh2_connection(authentication_type_, username, password, public_key, private_key);
  }
#endif
}

CCM::Bridge::Bridge(const std::string& configuration_file)
  : cassandra_version_(DEFAULT_CASSANDRA_VERSION)
  , dse_version_(DEFAULT_DSE_VERSION)
  , use_git_(DEFAULT_USE_GIT)
  , use_install_dir_(DEFAULT_USE_INSTALL_DIR)
  , use_dse_(DEFAULT_USE_DSE)
  , dse_workload_(DEFAULT_DSE_WORKLOAD)
  , cluster_prefix_(DEFAULT_CLUSTER_PREFIX)
  , authentication_type_(DEFAULT_AUTHENTICATION)
  , dse_credentials_type_(DEFAULT_DSE_CREDENTIALS)
#ifdef CASS_USE_LIBSSH2
  , deployment_type_(DEFAULT_DEPLOYMENT)
  , host_(DEFAULT_HOST)
  , session_(NULL)
  , channel_(NULL)
  , socket_(NULL) {
#else
  // Force local
  , deployment_type_(DeploymentType::LOCAL)
  , host_("127.0.0.1") {
#endif
  // Initialize the default remote configuration settings
  short port = DEFAULT_REMOTE_DEPLOYMENT_PORT;
  std::string username = DEFAULT_REMOTE_DEPLOYMENT_USERNAME;
  std::string password = DEFAULT_REMOTE_DEPLOYMENT_PASSWORD;
  std::string public_key = "";
  std::string private_key = "";

  // Open the file and parse each configuration line
  std::ifstream file(configuration_file.c_str(), std::ifstream::in);
  if (file.is_open()) {
    std::string current_line;
    while (std::getline(file, current_line)) {
      // Ignore lines that start with '#'
      current_line = trim(current_line);
      if (!current_line.empty() && current_line.at(0) != '#') {
        std::vector<std::string> tokens = explode(current_line, '=');
        // Ensure the configuration is a key/value pair
        if (tokens.size() == 2) {
          std::string key(to_lower(trim(tokens[0])));
          std::string value(trim(tokens[1]));

          // Find and apply the configuration setting
          if (key.compare(CCM_CONFIGURATION_KEY_CASSANDRA_VERSION) == 0) {
            cassandra_version_ = CassVersion(value);
          } else if (key.compare(CCM_CONFIGURATION_KEY_DSE_VERSION) == 0) {
            dse_version_ = DseVersion(value);
          } else if (key.compare(CCM_CONFIGURATION_KEY_USE_GIT) == 0) {
            //Convert the value
            std::stringstream valueStream(value);
            if (!(valueStream >> std::boolalpha >> use_git_).fail()) {
              continue;
            } else {
              LOG_ERROR("Invalid Flag [" << value << "] for Use git: Using default [" << DEFAULT_USE_GIT << "]");
              use_git_ = DEFAULT_USE_GIT;
            }
          } else if (key.compare(CCM_CONFIGURATION_KEY_BRANCH_TAG) == 0) {
            branch_tag_ = value;
          } else if (key.compare(CCM_CONFIGURATION_KEY_USE_INSTALL_DIR) == 0) {
            //Convert the value
            std::stringstream valueStream(value);
            if (!(valueStream >> std::boolalpha >> use_install_dir_).fail()) {
              continue;
            } else {
              LOG_ERROR("Invalid Flag [" << value << "] for Use Install Directory: Using default [" << DEFAULT_USE_INSTALL_DIR << "]");
              use_install_dir_ = DEFAULT_USE_INSTALL_DIR;
            }
          } else if (key.compare(CCM_CONFIGURATION_KEY_INSTALL_DIR) == 0) {
            install_dir_ = value;
          } else if (key.compare(CCM_CONFIGURATION_KEY_USE_DSE) == 0) {
            //Convert the value
            std::stringstream valueStream(value);
            if (!(valueStream >> std::boolalpha >> use_dse_).fail()) {
              continue;
            } else {
              LOG_ERROR("Invalid Flag [" << value << "] for Use DSE: Using default [" << DEFAULT_USE_DSE << "]");
              use_dse_ = DEFAULT_USE_DSE;
            }
          } else if (key.compare(CCM_CONFIGURATION_KEY_DSE_CREDENTIALS_TYPE) == 0) {
            // Determine the DSE credentials type
            bool is_found = false;
            for (DseCredentialsType::iterator iterator = DseCredentialsType::begin(); iterator != DseCredentialsType::end(); ++iterator) {
              if (*iterator == value) {
                dse_credentials_type_ = *iterator;
                is_found = true;
                break;
              }
            }
            if (!is_found) {
              LOG_ERROR("Invalid DSE Credentials Type [" << value << "]: Using default " << DEFAULT_DSE_CREDENTIALS.to_string());
            }
          } else if (key.compare(CCM_CONFIGURATION_KEY_DSE_USERNAME) == 0) {
            dse_username_ = value;
          } else if (key.compare(CCM_CONFIGURATION_KEY_DSE_PASSWORD) == 0) {
            dse_password_ = value;
#ifdef CASS_USE_LIBSSH2
          } else if (key.compare(CCM_CONFIGURATION_KEY_DEPLOYMENT_TYPE) == 0) {
            // Determine the deployment type
            bool is_found = false;
            for (DeploymentType::iterator iterator = DeploymentType::begin(); iterator != DeploymentType::end(); ++iterator) {
              if (*iterator == value) {
                deployment_type_ = *iterator;
                is_found = true;
                break;
              }
            }
            if (!is_found) {
              LOG_ERROR("Invalid Deployment Type: Using default " << DEFAULT_DEPLOYMENT.to_string());
            }
#endif
          } else if (key.compare(CCM_CONFIGURATION_KEY_AUTHENTICATION_TYPE) == 0) {
            // Determine the authentication type
            bool is_found = false;
            for (AuthenticationType::iterator iterator = AuthenticationType::begin(); iterator != AuthenticationType::end(); ++iterator) {
              if (*iterator == value) {
                authentication_type_ = *iterator;
                is_found = true;
                break;
              }
            }
            if (!is_found) {
              LOG_ERROR("Invalid Authentication Type [" << value << "]: Using default " << DEFAULT_AUTHENTICATION.to_string());
            }
#ifdef CASS_USE_LIBSSH2
          } else if (key.compare(CCM_CONFIGURATION_KEY_HOST) == 0) {
            host_ = value;
#endif
          } else if (key.compare(CCM_CONFIGURATION_KEY_SSH_PORT) == 0) {
            //Convert the value
            std::stringstream valueStream(value);
            if (!(valueStream >> port).fail()) {
              continue;
            } else {
              LOG_ERROR("Invalid Port: Using default [" << DEFAULT_REMOTE_DEPLOYMENT_PORT << "]");
              port = DEFAULT_REMOTE_DEPLOYMENT_PORT;
            }
#ifdef CASS_USE_LIBSSH2
          } else if (key.compare(CCM_CONFIGURATION_KEY_SSH_USERNAME) == 0) {
            username = value;
          } else if (key.compare(CCM_CONFIGURATION_KEY_SSH_PASSWORD) == 0) {
            password = value;
          } else if (key.compare(CCM_CONFIGURATION_KEY_SSH_PUBLIC_KEY) == 0) {
            public_key = value;
          } else if (key.compare(CCM_CONFIGURATION_KEY_SSH_PRIVATE_KEY) == 0) {
            private_key = value;
#endif
          } else {
            LOG_ERROR("Invalid Configuration Option: Key " << key << " with value " << value);
          }
        } else {
          LOG_ERROR("Invalid Key/Value Pair [" << current_line << "]: Configuration item will be skipped");
        }
      }
    }
  } else {
    LOG_WARN("Unable to Open Configuration File [" << configuration_file << "]: Defaults will be used");
  }

  // Determine if DSE is being used
  if (use_dse_) {
    dse_version_ = DseVersion(dse_version_.to_string());
    cassandra_version_ = dse_version_.get_cass_version();
  }

  // Determine if installation directory can be used
  if (use_install_dir_ && install_dir_.empty()) {
    throw BridgeException("Unable to use Installation Directory: Directory must not be blank");
  }

  // Display the configuration settings being used
  LOG("Host: " << host_);
  LOG("Cassandra Version: " << cassandra_version_.to_string());
  if (use_dse_) {
    LOG("DSE Version: " << dse_version_.to_string());
  }
  if (use_git_ && !branch_tag_.empty()) {
    LOG("  Branch/Tag: " << branch_tag_);
  }
  if (use_install_dir_ && !install_dir_.empty()) {
    LOG("  Installation Directory: " << install_dir_);
  }
  LOG("Cluster Prefix: " << cluster_prefix_);
  LOG("Deployment Type: " << deployment_type_.to_string());
#ifdef CASS_USE_LIBSSH2
  if (deployment_type_ == DeploymentType::REMOTE) {
    LOG("Authentication Type: " << authentication_type_.to_string());
    LOG("Port: " << port);
    LOG("Username: " << username);
    if (authentication_type_ == AuthenticationType::USERNAME_PASSWORD) {
      LOG("Password: " << password);
    } else {
      LOG("Public Key: " << public_key);
      LOG("Private Key: " << private_key);
    }
  }
#endif

#ifdef _WIN32
# ifdef _DEBUG
  // Enable automatic execution of the memory leak detection upon exit
  _CrtSetDbgFlag(_CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF);
# endif
#endif
#ifdef CASS_USE_LIBSSH2
  // Determine if libssh2 needs to be initialized
  if (deployment_type_ == DeploymentType::REMOTE) {
    // Initialize the socket
    try {
      initialize_socket(host_, port);
    }
    catch (SocketException &se) {
      // Re-throw the exception as a BridgeException
      finalize_libssh2();
      throw BridgeException(se.what());
    }

    // Initialize the libssh2 connection
    initialize_libssh2();

    // Authenticate and establish the libssh2 connection
    establish_libssh2_connection(authentication_type_, username, password, public_key, private_key);
  }
#endif
}

CCM::Bridge::~Bridge() {
#ifdef CASS_USE_LIBSSH2
  if (deployment_type_ == DeploymentType::REMOTE) {
    close_libssh2_terminal();
    finalize_libssh2();
  }
#endif
}

void CCM::Bridge::clear_cluster_data() {
  // Create the cluster clear data command and execute
  std::vector<std::string> clear_cluster_data_command;
  clear_cluster_data_command.push_back("clear");
  execute_ccm_command(clear_cluster_data_command);
}

std::string CCM::Bridge::cluster_contact_points(bool is_all /*= true*/) {
  // Determine if all the nodes are being returned or just the available nodes
  if (!is_all) {
    // Create the cluster liveset command and execute
    std::vector<std::string> liveset_command;
    liveset_command.push_back("liveset");
    return execute_ccm_command(liveset_command);
  } else {
    ClusterStatus status = cluster_status();
    std::stringstream contact_points;
    std::string ip_prefix = get_ip_prefix();
    for (unsigned i = 1; i <= status.node_count; ++i) {
      if (i > 1) {
        contact_points << ",";
      }
      contact_points << ip_prefix << i;
    }
    return contact_points.str();
  }
}

std::vector<std::string> CCM::Bridge::cluster_ip_addresses(bool is_all /*= true*/) {
  // Get and sort the IPv4 addresses and return the array/vector
  std::vector <std::string> ip_addresses = explode(cluster_contact_points(is_all), ',');
  std::sort(ip_addresses.begin(), ip_addresses.end());
  return ip_addresses;
}

ClusterStatus CCM::Bridge::cluster_status() {
  // Create the cluster status command and execute
  std::vector<std::string> status_command;
  status_command.push_back("status");
  std::string ccm_output = execute_ccm_command(status_command);

  // Iterate over the output line and parse the tokens
  std::string ip_prefix = get_ip_prefix();
  ClusterStatus status;
  std::istringstream parser(ccm_output);
  for (std::string token; std::getline(parser, token);) {
    if (!token.empty()) {
      // Handle node lines only
      std::string current_line = to_lower(trim(token));
      if (current_line.compare(0, 4, "node") == 0) {
        // Remove node and colon
        current_line.replace(0, 4, "");
        size_t colon_index = current_line.find(":");
        if (colon_index != std::string::npos) {
          current_line.replace(colon_index, 1, "");
        }

        // Split into key and value (node and status) and update cluster status
        std::vector<std::string> tokens = explode(current_line);
        if (tokens.size() >= 2) {
          std::string node_ip_address = ip_prefix + tokens[0];
          std::string node_status(tokens[1]);
          ++status.node_count;

          // Determine the status being updated
          if (node_status.compare(CCM_NODE_STATUS_DECOMMISSIONED) == 0 ||
              node_status.compare("decommisionned") == 0) { // Handle misspelling of decommissioned for older CCM versions
            status.nodes_decommissioned.push_back(node_ip_address);
          } else if (node_status.compare(CCM_NODE_STATUS_DOWN) == 0) {
            // Determine if the node is actually uninitialized
            if (tokens.size() == 4 && (tokens[2] + " " + tokens[3]).compare(CCM_NODE_STATUS_UNINITIALIZED) == 0) {
              status.nodes_uninitialized.push_back(node_ip_address);
            } else {
              status.nodes_down.push_back(node_ip_address);
            }
          } else if (node_status.compare(CCM_NODE_STATUS_UP) == 0) {
            status.nodes_up.push_back(node_ip_address);
          } else {
            LOG_ERROR("Node Status Not Valid: Unknown status " << node_status);
          }
        } else {
          LOG_ERROR("Node Status Cannot be Determined: To many tokens in status line [" << current_line << "]");
        }
      }
    }
  }
  return status;
}

bool CCM::Bridge::create_cluster(std::vector<unsigned short> data_center_nodes,
  bool with_vnodes /*= false*/, bool is_ssl /*= false*/,
  bool is_client_authentication /*= false*/) {
  // Generate the cluster name and determine if it needs to be created
  std::string active_cluster_name = get_active_cluster();
  std::string cluster_name = generate_cluster_name(cassandra_version_, data_center_nodes,
    with_vnodes, is_ssl, is_client_authentication);
  for (std::vector<DseWorkload>::iterator iterator = dse_workload_.begin();
    iterator != dse_workload_.end(); ++iterator) {
    if (use_dse_ && *iterator != DSE_WORKLOAD_CASSANDRA) {
      cluster_name.append("-").append(dse_workloads_[*iterator]);
    }
  }
  if (!switch_cluster(cluster_name)) {
    // Ensure any active cluster is stopped
    if (!get_active_cluster().empty()) {
      stop_cluster();
    }

    // Create the cluster create command and execute
    std::vector<std::string> create_command;
    create_command.push_back("create");
    if (use_install_dir_ && !install_dir_.empty()) {
      create_command.push_back("--install-dir=" + install_dir_);
    } else {
      create_command.push_back("-v");
      if (use_dse_) {
        if (use_git_) {
          if (branch_tag_.empty()) {
            create_command.push_back("git:" + dse_version_.to_string());
          } else {
            create_command.push_back("git:" + branch_tag_);
          }
        } else {
          create_command.push_back(dse_version_.to_string());
        }
        create_command.push_back("--dse");
        if (dse_credentials_type_ == DseCredentialsType::USERNAME_PASSWORD) {
          create_command.push_back("--dse-username=" + dse_username_);
          create_command.push_back("--dse-password=" + dse_password_);
        }
      } else {
        if (use_git_) {
          if (branch_tag_.empty()) {
            create_command.push_back("git:cassandra-" + cassandra_version_.to_string());
          } else {
            create_command.push_back("git:" + branch_tag_);
          }
        } else {
          create_command.push_back(cassandra_version_.to_string());
        }
      }
    }
    create_command.push_back("-b");

    // Determine if SSL and client authentication should be enabled
    if (is_ssl) {
      create_command.push_back("--ssl=ssl");
      if (is_client_authentication) {
        create_command.push_back("--require_client_auth");
      }
    }

    // Add the name of the cluster to create
    create_command.push_back(cluster_name);

    // Execute the CCM create command
    execute_ccm_command(create_command);

    // Generate the cluster update configuration command and execute
    execute_ccm_command(generate_create_updateconf_command(cassandra_version_));

    // Create the cluster populate command and execute
    std::string cluster_nodes = generate_cluster_nodes(data_center_nodes);
    std::string cluster_ip_prefix = get_ip_prefix();
    std::vector<std::string> populate_command;
    populate_command.push_back("populate");
    populate_command.push_back("-n");
    populate_command.push_back(cluster_nodes);
    populate_command.push_back("-i");
    populate_command.push_back(cluster_ip_prefix);
    if (with_vnodes) {
      populate_command.push_back("--vnodes");
    }
    execute_ccm_command(populate_command);

    // Update the cluster configuration (set num_tokens)
    if (with_vnodes) {
      // Maximum number of tokens is 1536
      update_cluster_configuration("num_tokens", "1536");
    }

    // Set the DSE workload (if applicable)
    if (use_dse_ &&
      !(dse_workload_.size() == 1 && dse_workload_[0] == DSE_WORKLOAD_CASSANDRA)) {
      set_dse_workloads(dse_workload_);
    }
  }

  // Indicate if the cluster was created or switched
  return !(active_cluster_name.compare(cluster_name) == 0);
}

bool CCM::Bridge::create_cluster(unsigned short data_center_one_nodes /*= 1*/,
  unsigned short data_center_two_nodes /*= 0*/, bool with_vnodes /*= false*/,
  bool is_ssl /*= false*/, bool is_client_authentication /*= false*/) {
  // Create the data center nodes from the two data centers
  std::vector<unsigned short> data_center_nodes;
  data_center_nodes.push_back(data_center_one_nodes);
  data_center_nodes.push_back(data_center_two_nodes);

  return create_cluster(data_center_nodes, with_vnodes, is_ssl,
    is_client_authentication);
}

bool CCM::Bridge::is_cluster_down() {
  // Iterate over each node and ensure a connection cannot be made
  ClusterStatus status = cluster_status();
  for (unsigned int i = 1; i <= status.node_count; ++i) {
    // Ensure the node is down
    if (!is_node_down(i)) {
      return false;
    }
  }

  // Cluster is down
  return true;
}

bool CCM::Bridge::is_cluster_up() {
  // Iterate over each node and ensure a connection can be made
  ClusterStatus status = cluster_status();
  for (unsigned int i = 1; i <= status.node_count; ++i) {
    // Ensure the node is ready/up
    if (!is_node_up(i)) {
      return false;
    }
  }

  // Cluster is ready
  return true;
}

bool CCM::Bridge::hang_up_cluster() {
  // Create the cluster stop command and execute
  std::vector<std::string> stop_command;
  stop_command.push_back("stop");
  stop_command.push_back("--hang-up");
  execute_ccm_command(stop_command);

  // Ensure the cluster is down
  return is_cluster_down();
}

bool CCM::Bridge::kill_cluster() {
  return stop_cluster(true);
}

void CCM::Bridge::remove_cluster() {
  remove_cluster(get_active_cluster());
}

void CCM::Bridge::remove_cluster(const std::string& cluster_name) {
  // Create the cluster remove command and execute
  std::vector<std::string> remove_command;
  remove_command.push_back("remove");
  remove_command.push_back(cluster_name);
  execute_ccm_command(remove_command);
}

void CCM::Bridge::remove_all_clusters(bool is_all /*= false*/) {
  // Iterate through all the available clusters
  std::vector<std::string> clusters = get_available_clusters();
  for (std::vector<std::string>::const_iterator iterator = clusters.begin(); iterator != clusters.end(); ++iterator) {
    // Determine if the cluster should be removed
    bool is_valid_cluster = is_all;
    if (!is_valid_cluster &&
       (*iterator).compare(0, cluster_prefix_.size(), cluster_prefix_) != 0) {
      continue;
    }
    remove_cluster(*iterator);
  }
}

bool  CCM::Bridge::start_cluster(std::vector<std::string> jvm_arguments /*= DEFAULT_JVM_ARGUMENTS*/) {
  // Create the cluster start command and execute
  std::vector<std::string> start_command;
  start_command.push_back("start");
  start_command.push_back("--wait-other-notice");
  start_command.push_back("--wait-for-binary-proto");
#ifdef _WIN32
  if (cassandra_version_ >= "2.2.4") {
    start_command.push_back("--quiet-windows");
  }
#endif
  for (std::vector<std::string>::const_iterator iterator = jvm_arguments.begin(); iterator != jvm_arguments.end(); ++iterator) {
    std::string jvm_argument = trim(*iterator);
    if (!jvm_argument.empty()) {
      start_command.push_back("--jvm_arg=" + *iterator);
    }
  }
  execute_ccm_command(start_command);

  // Ensure the cluster is up
  return is_cluster_up();
}

bool CCM::Bridge::start_cluster(std::string jvm_argument /*= ""*/) {
  std::vector<std::string> jvm_arguments;
  if (!jvm_argument.empty()) {
    jvm_arguments.push_back(jvm_argument);
  }
  return start_cluster(jvm_arguments);
}

bool CCM::Bridge::stop_cluster(bool is_kill /*= false*/) {
  // Create the cluster stop command and execute
  std::vector<std::string> stop_command;
  stop_command.push_back("stop");
  if (is_kill) {
    stop_command.push_back("--not-gently");
  }
  execute_ccm_command(stop_command);

  // Ensure the cluster is down
  return is_cluster_down();
}

bool CCM::Bridge::switch_cluster(const std::string& cluster_name) {
  // Get the active cluster and the available clusters
  std::string active_cluster;
  std::vector<std::string> clusters = get_available_clusters(active_cluster);

  // Determine if the switch should be performed
  if (active_cluster.compare(trim(cluster_name)) != 0) {
    // Ensure the cluster is in the list
    if (std::find(clusters.begin(), clusters.end(), cluster_name) != clusters.end()) {
      // Kill and clear the data on the current active cluster
      kill_cluster();
      clear_cluster_data();

      // Create the cluster switch command and execute
      std::vector<std::string> switch_command;
      switch_command.push_back("switch");
      switch_command.push_back(cluster_name);
      execute_ccm_command(switch_command);
      return true;
    }
  } else {
    // Cluster requested is already active
    return true;
  }

  // Unable to switch the cluster
  return false;
}

void CCM::Bridge::update_cluster_configuration(std::vector<std::string> key_value_pairs, bool is_dse /*= false*/) {
  // Create the update configuration command
  key_value_pairs.insert(key_value_pairs.begin(), (is_dse ? "updatedseconf" : "updateconf"));
  execute_ccm_command(key_value_pairs);

}

void CCM::Bridge::update_cluster_configuration(const std::string& key, const std::string& value, bool is_dse /*= false*/) {
  // Create the configuration to be updated
  std::stringstream configuration;
  configuration << key << ":" << value;

  // Create the update configuration command
  std::vector<std::string> updateconf_command;
  updateconf_command.push_back(is_dse ? "updatedseconf" : "updateconf");
  updateconf_command.push_back(configuration.str());
  execute_ccm_command(updateconf_command);
}

void CCM::Bridge::update_node_configuration(unsigned int node, std::vector<std::string> key_value_pairs) {
  // Create the update configuration command
  key_value_pairs.insert(key_value_pairs.begin(), generate_node_name(node));
  key_value_pairs.insert(key_value_pairs.begin(), "updateconf");
  execute_ccm_command(key_value_pairs);
}

void CCM::Bridge::update_node_configuration(unsigned int node, const std::string& key, const std::string& value) {
  // Create the configuration to be updated
  std::stringstream configuration;
  configuration << key << ":" << value;

  // Create the update configuration command
  std::vector<std::string> updateconf_command;
  updateconf_command.push_back(generate_node_name(node));
  updateconf_command.push_back("updateconf");
  updateconf_command.push_back(configuration.str());
  execute_ccm_command(updateconf_command);
}

unsigned int CCM::Bridge::add_node(const std::string& data_center /*= ""*/) {
  // Generate the arguments for the add node command
  unsigned int node = get_next_available_node();
  std::stringstream node_ip_address;
  node_ip_address << get_ip_prefix() << node;
  std::stringstream jmx_port;
  std::stringstream jmx_remote_debug_port;
  jmx_port << (7000 + (100 * node));
  jmx_remote_debug_port << (2000 + (100 * node));

  // Create the add node command and execute
  std::vector<std::string> add_node_command;
  add_node_command.push_back("add");
  add_node_command.push_back("-b");
  add_node_command.push_back("-i");
  add_node_command.push_back(node_ip_address.str());
  add_node_command.push_back("-j");
  add_node_command.push_back(jmx_port.str());
  add_node_command.push_back("-r");
  add_node_command.push_back(jmx_remote_debug_port.str());
  if (!data_center.empty()) {
    add_node_command.push_back("-d");
    add_node_command.push_back(data_center);
  }
  if (use_dse_) {
    add_node_command.push_back("--dse");
  }
  add_node_command.push_back(generate_node_name(node));
  execute_ccm_command(add_node_command);

  // Return the node created
  return node;
}

unsigned int CCM::Bridge::bootstrap_node(const std::string& jvm_argument /*= ""*/, const std::string& data_center /*= ""*/) {
  unsigned int node = add_node(data_center);
  start_node(node, jvm_argument);
  return node;
}

bool CCM::Bridge::decommission_node(unsigned int node) {
  // Create the node decommission command and execute
  std::vector<std::string> decommission_node_command;
  decommission_node_command.push_back(generate_node_name(node));
  decommission_node_command.push_back("decommission");
  execute_ccm_command(decommission_node_command);

  // Ensure the node has been decommissioned
  return is_node_decommissioned(node);
}

void CCM::Bridge::disable_node_binary_protocol(unsigned int node) {
  // Create the disable node binary protocol command and execute
  std::vector<std::string> disable_node_binary_protocol_command;
  disable_node_binary_protocol_command.push_back(generate_node_name(node));
  disable_node_binary_protocol_command.push_back("nodetool");
  disable_node_binary_protocol_command.push_back("disablebinary");
  execute_ccm_command(disable_node_binary_protocol_command);
}

void CCM::Bridge::disable_node_gossip(unsigned int node) {
  // Create the disable node gossip command and execute
  std::vector<std::string> disable_node_gossip_command;
  disable_node_gossip_command.push_back(generate_node_name(node));
  disable_node_gossip_command.push_back("nodetool");
  disable_node_gossip_command.push_back("disablegossip");
  execute_ccm_command(disable_node_gossip_command);
}

void CCM::Bridge::disable_node_trace(unsigned int node) {
  // Create the disable node trace command and execute
  std::vector<std::string> disable_node_trace_command;
  disable_node_trace_command.push_back(generate_node_name(node));
  disable_node_trace_command.push_back("nodetool");
  disable_node_trace_command.push_back("settraceprobability");
  disable_node_trace_command.push_back("0");
  execute_ccm_command(disable_node_trace_command);
}

void CCM::Bridge::enable_node_binary_protocol(unsigned int node) {
  // Create the enable node binary protocol command and execute
  std::vector<std::string> enable_node_binary_protocol_command;
  enable_node_binary_protocol_command.push_back(generate_node_name(node));
  enable_node_binary_protocol_command.push_back("nodetool");
  enable_node_binary_protocol_command.push_back("enablebinary");
  execute_ccm_command(enable_node_binary_protocol_command);
}

void CCM::Bridge::enable_node_gossip(unsigned int node) {
  // Create the enable node gossip command and execute
  std::vector<std::string> disable_node_gossip_command;
  disable_node_gossip_command.push_back(generate_node_name(node));
  disable_node_gossip_command.push_back("nodetool");
  disable_node_gossip_command.push_back("enablegossip");
  execute_ccm_command(disable_node_gossip_command);
}

void CCM::Bridge::enable_node_trace(unsigned int node) {
  // Create the enable node trace command and execute
  std::vector<std::string> enable_node_trace_command;
  enable_node_trace_command.push_back(generate_node_name(node));
  enable_node_trace_command.push_back("nodetool");
  enable_node_trace_command.push_back("settraceprobability");
  enable_node_trace_command.push_back("1");
  execute_ccm_command(enable_node_trace_command);
}

void CCM::Bridge::execute_cql_on_node(unsigned int node, const std::string& cql) {
  // Update the CQL statement for the command line
  std::stringstream execute_statement;
  execute_statement << "\"" << cql << ";\"";

  // Create the CQLSH pass through command and execute
  std::vector<std::string> cqlsh_node_command;
  cqlsh_node_command.push_back(generate_node_name(node));
  cqlsh_node_command.push_back("cqlsh");
  cqlsh_node_command.push_back("-x");
  cqlsh_node_command.push_back(execute_statement.str());
  execute_ccm_command(cqlsh_node_command);
}

bool CCM::Bridge::hang_up_node(unsigned int node) {
  // Create the node stop command and execute
  std::vector<std::string> stop_node_command;
  stop_node_command.push_back(generate_node_name(node));
  stop_node_command.push_back("stop");
  stop_node_command.push_back("--hang-up");
  execute_ccm_command(stop_node_command);

  // Ensure the node is down
  return is_node_down(node);
}

bool CCM::Bridge::kill_node(unsigned int node) {
  return stop_node(node, true);
}

void CCM::Bridge::pause_node(unsigned int node) {
  // Create the node pause command and execute
  std::vector<std::string> pause_node_command;
  pause_node_command.push_back(generate_node_name(node));
  pause_node_command.push_back("pause");
  execute_ccm_command(pause_node_command);
}

void CCM::Bridge::resume_node(unsigned int node) {
  // Create the node resume command and execute
  std::vector<std::string> resume_node_command;
  resume_node_command.push_back(generate_node_name(node));
  resume_node_command.push_back("resume");
  execute_ccm_command(resume_node_command);
}

bool CCM::Bridge::start_node(unsigned int node, std::vector<std::string> jvm_arguments /*= DEFAULT_JVM_ARGUMENTS*/) {
  // Create the node start command and execute
  std::vector<std::string> start_node_command;
  start_node_command.push_back(generate_node_name(node));
  start_node_command.push_back("start");
  start_node_command.push_back("--wait-other-notice");
  start_node_command.push_back("--wait-for-binary-proto");
#ifdef _WIN32
  if (cassandra_version_ >= "2.2.4") {
    start_node_command.push_back("--quiet-windows");
  }
#endif
  for (std::vector<std::string>::const_iterator iterator = jvm_arguments.begin(); iterator != jvm_arguments.end(); ++iterator) {
    std::string jvm_argument = trim(*iterator);
    if (!jvm_argument.empty()) {
      start_node_command.push_back("--jvm_arg=" + *iterator);
    }
  }
  execute_ccm_command(start_node_command);

  // Ensure the node is up
  return is_node_up(node);
}

bool CCM::Bridge::start_node(unsigned int node, std::string jvm_argument) {
  // Create the JVM arguments array/vector
  std::vector<std::string> jvm_arguments;
  jvm_arguments.push_back(jvm_argument);
  return start_node(node, jvm_arguments);
}

bool CCM::Bridge::stop_node(unsigned int node, bool is_kill /*= false*/) {
  // Create the node stop command and execute
  std::vector<std::string> stop_node_command;
  stop_node_command.push_back(generate_node_name(node));
  stop_node_command.push_back("stop");
  if (is_kill) {
    stop_node_command.push_back("--not-gently");
  }
  execute_ccm_command(stop_node_command);

  // Ensure the node is down
  return is_node_down(node);
}

std::string CCM::Bridge::get_ip_prefix() {
  return host_.substr(0, host_.size() - 1);
}

std::string CCM::Bridge::get_ip_prefix(const std::string& configuration_file) {
  // Open the file and parse each configuration line looking for the version
  std::string ip_prefix = DEFAULT_HOST;
  std::ifstream file(configuration_file.c_str(), std::ifstream::in);
  if (file.is_open()) {
    std::string current_line;
    while (std::getline(file, current_line)) {
      // Ignore lines that start with '#'
      if (!current_line.empty() && current_line.at(0) != '#') {
        std::vector<std::string> tokens = explode(trim(current_line), '=');
        // Ensure the configuration is a key/value pair
        if (tokens.size() == 2) {
          std::string key = to_lower(trim(tokens[0]));
          std::string value = trim(tokens[1]);

          // Find and apply the configuration setting
          if (key.compare(CCM_CONFIGURATION_KEY_HOST) == 0) {
            ip_prefix = value;
          }
        }
      }
    }
  }

  // Return the IPv4 prefix
  return ip_prefix.substr(0, ip_prefix.size() - 1);;
}

CassVersion CCM::Bridge::get_cassandra_version() {
  // Get the version string from CCM
  std::vector<std::string> active_cluster_version_command;
  active_cluster_version_command.push_back(generate_node_name(1));
  active_cluster_version_command.push_back("version");
  std::string ccm_output = execute_ccm_command(active_cluster_version_command);

  // Ensure the version release information exists and return the version
  size_t version_index = ccm_output.find("ReleaseVersion:");
  if (version_index != std::string::npos) {
    ccm_output.replace(0, version_index + 15, "");
    return CassVersion(trim(ccm_output));
  }

  // Unable to determine version information from active cluster
  throw BridgeException("Unable to Determine Version Information from Active Cluster: " + get_active_cluster());
}

CassVersion CCM::Bridge::get_cassandra_version(const std::string& configuration_file) {
  // Open the file and parse each configuration line looking for the version
  CassVersion cassandra_version(DEFAULT_CASSANDRA_VERSION);
  DseVersion dse_version(DEFAULT_DSE_VERSION);
  bool use_dse = false;
  std::ifstream file(configuration_file.c_str(), std::ifstream::in);
  if (file.is_open()) {
    std::string current_line;
    while (std::getline(file, current_line)) {
      // Ignore lines that start with '#'
      if (!current_line.empty() && current_line.at(0) != '#') {
        std::vector<std::string> tokens = explode(trim(current_line), '=');
        // Ensure the configuration is a key/value pair
        if (tokens.size() == 2) {
          std::string key = to_lower(trim(tokens[0]));
          std::string value = trim(tokens[1]);

          // Find and apply the configuration setting
          if (key.compare(CCM_CONFIGURATION_KEY_CASSANDRA_VERSION) == 0) {
            cassandra_version = CassVersion(value);
          } else if (key.compare(CCM_CONFIGURATION_KEY_DSE_VERSION) == 0) {
            dse_version = DseVersion(value);
          } else if (key.compare(CCM_CONFIGURATION_KEY_USE_DSE) == 0) {
            //Convert the value
            std::stringstream valueStream(value);
            if (!(valueStream >> std::boolalpha >> use_dse).fail()) {
              continue;
            }
          }
        }
      }
    }
  }

  // Return the Cassandra version
  if (use_dse) {
    return dse_version.get_cass_version();
  }
  return cassandra_version;
}

DseVersion CCM::Bridge::get_dse_version() {
  // Get the version string from CCM
  std::vector<std::string> active_cluster_version_command;
  active_cluster_version_command.push_back(generate_node_name(1));
  active_cluster_version_command.push_back("dse");
  active_cluster_version_command.push_back("-v");
  std::string ccm_output = execute_ccm_command(active_cluster_version_command);

  // Ensure the version release information exists and return the version
  ccm_output = trim(ccm_output);
  if (!ccm_output.empty()) {
    return DseVersion(ccm_output);
  }

  // Unable to determine version information from active cluster
  throw BridgeException("Unable to Determine Version Information from Active Cluster: " + get_active_cluster());
}

DseVersion CCM::Bridge::get_dse_version(const std::string& configuration_file) {
  // Open the file and parse each configuration line looking for the version
  DseVersion dse_version(DEFAULT_DSE_VERSION);
  std::ifstream file(configuration_file.c_str(), std::ifstream::in);
  if (file.is_open()) {
    std::string current_line;
    while (std::getline(file, current_line)) {
      // Ignore lines that start with '#'
      if (!current_line.empty() && current_line.at(0) != '#') {
        std::vector<std::string> tokens = explode(trim(current_line), '=');
        // Ensure the configuration is a key/value pair
        if (tokens.size() == 2) {
          std::string key = to_lower(trim(tokens[0]));
          std::string value = trim(tokens[1]);

          // Find and apply the configuration setting
          if (key.compare(CCM_CONFIGURATION_KEY_DSE_VERSION) == 0) {
            dse_version = DseVersion(value);
          }
        }
      }
    }
  }

  // Return the DSE version
  return dse_version;
}

bool CCM::Bridge::set_dse_workload(unsigned int node, DseWorkload workload,
  bool is_kill /*= false */) {
  std::vector<DseWorkload> workloads;
  workloads.push_back(workload);
  return set_dse_workloads(1, workloads, is_kill);
}

bool CCM::Bridge::set_dse_workloads(unsigned int node,
  std::vector<DseWorkload> workloads, bool is_kill /*= false */) {
  // Ensure the workloads can be processed
  if (workloads.empty()) {
    throw BridgeException("No workloads to assign");
  }

  // Update the member variable with the workloads and generate workloads
  dse_workload_.clear();
  dse_workload_ = workloads;
  std::string dse_workloads = generate_dse_workloads(workloads);

  // Determine if the node is currently active/up
  bool was_node_active = false;
  if (!is_node_down(node)) {
    LOG("Stopping Active Node to Set Workload: " << dse_workloads
      << " workload on node " << node);
    stop_node(node, is_kill);
    was_node_active = true;
  }

  // Create the node DSE workload command and execute
  std::vector<std::string> dse_workload_command;
  dse_workload_command.push_back(generate_node_name(node));
  dse_workload_command.push_back("setworkload");
  dse_workload_command.push_back(dse_workloads);
  execute_ccm_command(dse_workload_command);

  // Determine if the node should be restarted
  if (was_node_active) {
    LOG("Restarting Node to Apply Workload: " << dse_workloads
      << " workload on node " << node);
    start_node(node);
  }

  return was_node_active;
}

bool CCM::Bridge::set_dse_workload(DseWorkload workload,
  bool is_kill /*= false */) {
  std::vector<DseWorkload> workloads;
  workloads.push_back(workload);
  return set_dse_workloads(workloads, is_kill);
}

bool CCM::Bridge::set_dse_workloads(std::vector<DseWorkload> workloads,
  bool is_kill /*= false */) {
  // Ensure the workloads can be processed
  if (workloads.empty()) {
    throw BridgeException("No workloads to assign");
  }

  // Determine if the cluster is currently active/up
  bool was_cluster_active = false;
  if (!is_cluster_down()) {
    LOG("Stopping Active Cluster to Set Workload: " <<
      generate_dse_workloads(workloads) << " workload");
    stop_cluster(is_kill);
    was_cluster_active = true;
  }

  // Iterate over each node and set the DSE workload
  ClusterStatus status = cluster_status();
  for (unsigned int i = 1; i <= status.node_count; ++i) {
    set_dse_workloads(i, workloads, false);
  }

  // Determine if the cluster should be restarted
  if (was_cluster_active) {
    LOG("Restarting Cluster to Apply Workload: " <<
      generate_dse_workloads(workloads) << " workload");
    start_cluster();
  }

  return was_cluster_active;
}

bool CCM::Bridge::is_node_decommissioned(unsigned int node) {
  // Iterate over the list of decommissioned nodes
  std::stringstream node_ip_address;
  node_ip_address << get_ip_prefix() << node;
  std::vector<std::string> nodes = cluster_status().nodes_decommissioned;
  for (std::vector<std::string>::const_iterator iterator = nodes.begin(); iterator < nodes.end(); ++iterator) {
    if (node_ip_address.str().compare(*iterator) == 0) {
      return true;
    }
  }

  // Node has not been decommissioned
  return false;
}

bool CCM::Bridge::is_node_down(unsigned int node) {
  unsigned int number_of_retries = 0;
  while (number_of_retries++ < CCM_RETRIES) {
    if (!is_node_availabe(node)) {
      return true;
    } else {
      LOG("Connected to Node " << node << " in Cluster " << get_active_cluster() << ": Rechecking node down status [" << number_of_retries << "]");
      msleep(CCM_NAP);
    }
  }

  // Connection can still being established on node
  return false;
}

bool CCM::Bridge::is_node_up(unsigned int node) {
  unsigned int number_of_retries = 0;
  while (number_of_retries++ < CCM_RETRIES) {
    if (is_node_availabe(node)) {
      return true;
    } else {
      LOG("Unable to Connect to Node " << node << " in Cluster " << get_active_cluster() << ": Rechecking node up status [" << number_of_retries << "]");
      msleep(CCM_NAP);
    }
  }

  // Connection cannot be established on node
  return false;
}

#ifdef CASS_USE_LIBSSH2
void CCM::Bridge::initialize_socket(const std::string& host, short port) {
  // Initialize the socket
  socket_ = new Socket();

  // Establish socket connection
  socket_->establish_connection(host, port);
}

void CCM::Bridge::synchronize_socket() {
  // Determine current read/write direction of the session
  bool is_read = false;
  bool is_write = false;
  int read_write_direction = libssh2_session_block_directions(session_);
  if (read_write_direction & LIBSSH2_SESSION_BLOCK_INBOUND) {
    is_read = true;
  }
  if (read_write_direction & LIBSSH2_SESSION_BLOCK_OUTBOUND) {
    is_write = true;
  }

  // Synchronize the socket
  socket_->synchronize(is_read, is_write);
}

void CCM::Bridge::initialize_libssh2() {
  // Initialize libssh2
  int error_code = libssh2_init(LIBSSH2_INIT_ALL);
  if (error_code) {
    finalize_libssh2();
    std::stringstream message;
    message << "libssh2 Initialization Failed: " << error_code;
    throw BridgeException(message.str());
  }

  // Initialize and create the libssh2 session
  session_ = libssh2_session_init();
  if (!session_) {
    finalize_libssh2();
    throw BridgeException("libssh2 Session Initialization Failed");
  }

  // Disable blocking on the session
  libssh2_session_set_blocking(session_, FALSE);

  // Perform the session handshake; trade banners, exchange keys, setup cyrpto
  while ((error_code = libssh2_session_handshake(session_, socket_->get_handle())) == LIBSSH2_ERROR_EAGAIN) {
    ; // no-op
  }
  if (error_code) {
    // Determine error that occurred
    std::stringstream message;
    message << "libssh2 Session Handshake Failed: ";
    switch (error_code) {
      case LIBSSH2_ERROR_SOCKET_NONE:
        message << "The socket is invalid";
        break;
      case LIBSSH2_ERROR_BANNER_SEND:
        message << "Unable to send banner to remote host";
        break;
      case LIBSSH2_ERROR_KEX_FAILURE:
        message << "Encryption key exchange with the remote host failed";
        break;
      case LIBSSH2_ERROR_SOCKET_SEND:
        message << "Unable to send data on socket";
        break;
      case LIBSSH2_ERROR_SOCKET_DISCONNECT:
        message << "The socket was disconnected";
        break;
      case LIBSSH2_ERROR_PROTO:
        message << "An invalid SSH protocol response was received on the socket";
        break;
      default:
        message << error_code;
        break;
    }
    finalize_libssh2();
    throw BridgeException(message.str());
  }
}

void CCM::Bridge::establish_libssh2_connection(AuthenticationType authentication_type,
  const std::string& username, const std::string& password,
  const std::string& public_key, const std::string& private_key) {
  int error_code = 0;

  // Determine authentication mechanism
  if (authentication_type == AuthenticationType::USERNAME_PASSWORD) {
    // Perform username and password authentication
    while ((error_code = libssh2_userauth_password(session_, username.c_str(), password.c_str())) == LIBSSH2_ERROR_EAGAIN) {
      ; // no-op
    }
  } else {
    while ((error_code = libssh2_userauth_publickey_fromfile(session_, username.c_str(), public_key.c_str(), private_key.c_str(), "")) == LIBSSH2_ERROR_EAGAIN) {
      ; //no-op
    }
  }

  if (error_code) {
    // Determine error that occurred
    std::stringstream message;
    message << "libssh2 Username and Password Authentication Failed: ";
    switch (error_code) {
    case LIBSSH2_ERROR_ALLOC:
      message << "An internal memory allocation call failed";
      break;
    case LIBSSH2_ERROR_SOCKET_SEND:
      message << "Unable to send data on socket";
      break;
    case LIBSSH2_ERROR_SOCKET_TIMEOUT:
      message << "Timed out waiting for response";
      break;
    case LIBSSH2_ERROR_PASSWORD_EXPIRED:
      message << "Password has expired";
      break;
    case LIBSSH2_ERROR_PUBLICKEY_UNVERIFIED:
      message << "The username/public key combination was invalid";
      break;
    case LIBSSH2_ERROR_AUTHENTICATION_FAILED:
      // Ensure error message is displayed for the authentication type
      if (authentication_type == AuthenticationType::USERNAME_PASSWORD) {
        message << "Invalid username/password";
      } else {
        message << "Authentication using the supplied public key was not accepted";
      }
      break;
    default:
      message << error_code;
      break;
    }
    finalize_libssh2();
    throw BridgeException(message.str());
  }
}

void CCM::Bridge::open_libssh2_terminal() {
  // Open a channel; request a shell
  while (session_ != NULL && (channel_ = libssh2_channel_open_session(session_)) == NULL
    && libssh2_session_last_error(session_, NULL, NULL, FALSE) == LIBSSH2_ERROR_EAGAIN) {
    synchronize_socket();
  }
  if (!channel_) {
    // Determine error that occurred
    int error_code = libssh2_session_last_error(session_, NULL, NULL, FALSE);
    std::string message("libssh2 Opening Session Channel Failed: ");
    switch (error_code) {
      case LIBSSH2_ERROR_ALLOC:
        message += "An internal memory allocation call failed";
        break;
      case LIBSSH2_ERROR_SOCKET_SEND:
        message += "Unable to send data on socket";
        break;
      case LIBSSH2_ERROR_CHANNEL_FAILURE:
        message += "Unable to open channel";
    }
    finalize_libssh2();
    throw BridgeException(message);
  }
}

void CCM::Bridge::close_libssh2_terminal() {
  if (channel_) {
    // Close the libssh2 channel/terminal
    int error_code = 0;
    while ((error_code = libssh2_channel_close(channel_)) == LIBSSH2_ERROR_EAGAIN) {
      synchronize_socket();
    }
    if (error_code == 0) {
      char* exit_signal = NULL;
      libssh2_channel_get_exit_status(channel_);
      libssh2_channel_get_exit_signal(channel_, &exit_signal, NULL, NULL, NULL, NULL, NULL);
      if (exit_signal) {
        LOG_ERROR("libssh2 Unable to Close Channel: " << exit_signal);
      }
    }
    if (error_code) {
      LOG_ERROR("libssh2 Unable to Close Channel: " << error_code);
    }

    // Free the channel/terminal resources
    while ((error_code = libssh2_channel_free(channel_)) == LIBSSH2_ERROR_EAGAIN) {
      ; // no-op
    }
    if (error_code) {
      LOG_ERROR("libssh2 Unable to Free Channel Resources: " << error_code);
    }
    channel_ = NULL;
  }
}

void CCM::Bridge::finalize_libssh2() {
  // Free the libssh2 session
  if (session_) {
    // Perform session disconnection
    int error_code = 0;
    while ((error_code = libssh2_session_disconnect(session_, "Shutting Down libssh2 CCM Bridge Session")) == LIBSSH2_ERROR_EAGAIN) {
      ; // no-op
    }
    if (error_code) {
      LOG_ERROR("Unable to Perform libssh2 Session Disconnect: " << error_code);
    }
    while ((error_code = libssh2_session_free(session_)) == LIBSSH2_ERROR_EAGAIN) {
      ; // no-op
    }
    if (error_code) {
      LOG_ERROR("Unable to Free libssh2 Session Resources: " << error_code);
    }
    session_ = NULL;
  }

  // Free the socket (closes connection)
  delete socket_;
  socket_ = NULL;

  // Free up remaining libssh2 memory
  libssh2_exit();

#ifdef OPENSSL_CLEANUP
  // Free OpenSSL resources
  RAND_cleanup();
  ENGINE_cleanup();
  CONF_modules_unload(TRUE);
  CONF_modules_free();
  EVP_cleanup();
  ERR_free_strings();
  ERR_remove_state(PID_UNKNOWN);
  CRYPTO_cleanup_all_ex_data();
#endif
}

std::string CCM::Bridge::execute_libssh2_command(const std::vector<std::string>& command) {
  // Make sure the libssh2 session wasn't terminated
  if (!session_) {
    throw BridgeException("Command Cannot be Executed: libssh2 session is invalid/terminated");
  }

  // Create/Open libssh2 terminal
  open_libssh2_terminal();

  // Execute the command
  int error_code = 0;
  std::string full_command = implode(command);
  while ((error_code = libssh2_channel_exec(channel_, full_command.c_str())) == LIBSSH2_ERROR_EAGAIN) {
    synchronize_socket();
  }
  if (error_code) {
    // Determine error that occurred
      std::stringstream message;
      message << "libssh2 Command Execute Failed: ";
      switch (error_code) {
        case LIBSSH2_ERROR_ALLOC:
          message << "An internal memory allocation call failed";
          break;
        case LIBSSH2_ERROR_SOCKET_SEND:
          message << "Unable to send data on socket";
          break;
        case LIBSSH2_ERROR_CHANNEL_REQUEST_DENIED:
          message << "Request denied";
          break;
        default:
          message << error_code;
          break;
      }
      finalize_libssh2();
      throw BridgeException(message.str());
  }

  // Get the terminal output, close the terminal and return the output
  std::string output = read_libssh2_terminal();
  close_libssh2_terminal();
  return output;
}
#endif

std::string CCM::Bridge::execute_local_command(const std::vector<std::string>& command) {
  // Execute the command locally
#ifdef _WIN32
  if (!use_dse_) {
#endif
    std::string output;
    std::string full_command = implode(command) + " 2>&1";
    FILE* pipe = POPEN(full_command.c_str(), "r"); // stdout only
    int file_descriptor = FILENO(pipe);

#ifndef _WIN32
  // Ensure the pipe is non-blocking
    fcntl(file_descriptor, F_SETFL, O_NONBLOCK);
#endif

  // Ensure the command was execute (ignoring error codes)
    if (pipe) {
      // Get the command output
      while (!feof(pipe)) {
        char buffer[128];
        memset(buffer, '\0', sizeof(char) * sizeof(buffer));
        ssize_t bytes_read = READ(file_descriptor, buffer, sizeof(buffer));
        if (bytes_read == -1 && errno == EAGAIN) {
          msleep(CCM_NAP);
          continue;
        } else if (bytes_read > 0) {
          output += std::string(buffer, bytes_read);
        } else {
          break;
        }
      }
    }

    // Close the pip and return the output
    PCLOSE(pipe);
    return output;
#ifdef _WIN32
  } else {
    LOG_ERROR("DSE v" << dse_version_.to_string() << " cannot be launched on Windows platform");
  }

  return "";
#endif
}

#ifdef CASS_USE_LIBSSH2
std::string CCM::Bridge::read_libssh2_terminal() {
  ssize_t bytes_read_error_code = 0;
  char buffer[512];
  memset(buffer, '\0', sizeof(char) * 512);
  std::string output;

  // Read stdout
  while (true) {
    while ((bytes_read_error_code = libssh2_channel_read(channel_, buffer, sizeof(buffer))) > 0) {
      if (bytes_read_error_code > 0) {
        output += std::string(buffer, bytes_read_error_code);
      }
    }
    if (bytes_read_error_code == LIBSSH2_ERROR_EAGAIN) {
      synchronize_socket();
      msleep(CCM_NAP);
    } else {
      break;
    }
  }

  // Read stderr
  while (true) {
    while ((bytes_read_error_code = libssh2_channel_read_stderr(channel_, buffer, sizeof(buffer))) > 0) {
      if (bytes_read_error_code > 0) {
        output += std::string(buffer, bytes_read_error_code);
      }
    }
    if (bytes_read_error_code == LIBSSH2_ERROR_EAGAIN) {
      synchronize_socket();
      msleep(CCM_NAP);
    } else {
      break;
    }
  }

  return output;
}
#endif

std::string CCM::Bridge::execute_ccm_command(const std::vector<std::string>& command) {
  // Create the CCM command
  std::vector<std::string> ccm_command;
  ccm_command.push_back("ccm");
  ccm_command.insert(ccm_command.end(), command.begin(), command.end());
  LOG(implode(ccm_command));

  // Determine how to execute the command
  std::string output;
  if (deployment_type_ == DeploymentType::LOCAL) {
    output = execute_local_command(ccm_command);
  }
#ifdef CASS_USE_LIBSSH2
  else if (deployment_type_ == DeploymentType::REMOTE) {
    output = execute_libssh2_command(ccm_command);
  }
#endif

  if (!output.empty()) LOG(trim(output));
  return output;
}

std::string CCM::Bridge::get_active_cluster() {
  std::string active_cluster;
  std::vector<std::string> clusters = get_available_clusters(active_cluster);
  return active_cluster;
}

std::vector<std::string> CCM::Bridge::get_available_clusters() {
  std::string active_cluster;
  return get_available_clusters(active_cluster);
}

std::vector<std::string> CCM::Bridge::get_available_clusters(std::string& active_cluster) {
  // Create the cluster list command and get the list of clusters
  std::vector<std::string> list_command;
  list_command.push_back("list");
  std::vector<std::string> clusters = explode(execute_ccm_command(list_command));

  // Determine the active cluster and correct the cluster array
  int index = 0;
  for (std::vector<std::string>::const_iterator iterator = clusters.begin(); iterator < clusters.end(); ++iterator) {
    std::string cluster = *iterator;
    if (cluster.compare(0, 1, "*") == 0) {
      cluster.erase(std::remove(cluster.begin(), cluster.end(), '*'), cluster.end());
      active_cluster = cluster;
      clusters.at(index) = cluster;
    }
    ++index;
  }
  return clusters;
}

std::string CCM::Bridge::generate_cluster_name(CassVersion cassandra_version,
  std::vector<unsigned short> data_center_nodes,
  bool with_vnodes, bool is_ssl, bool is_client_authentication) {
  std::stringstream cluster_name;
  cluster_name << cluster_prefix_ << "_"
               << (use_dse_ ? dse_version_.to_string(false) : cassandra_version.to_string(false))
               << "_" << generate_cluster_nodes(data_center_nodes, '-');
  if (with_vnodes) {
    cluster_name << "-vnodes";
  }
  if (is_ssl) {
    cluster_name << "-ssl";
    if (is_client_authentication) {
      cluster_name << "-client_authentication";
    }
  }
  return cluster_name.str();
}

std::string CCM::Bridge::generate_cluster_nodes(std::vector<unsigned short> data_center_nodes,
  char separator /* = ':'*/) {
  std::stringstream cluster_nodes;
  for (std::vector<unsigned short>::iterator iterator = data_center_nodes.begin();
    iterator != data_center_nodes.end(); ++iterator) {
    cluster_nodes << *iterator;
    if ((iterator + 1) != data_center_nodes.end()) {
      cluster_nodes << separator;
    }
  }
  return cluster_nodes.str();
}

std::vector<std::string> CCM::Bridge::generate_create_updateconf_command(CassVersion cassandra_version) {
  //TODO: Add SSL setup and client authentication
  // Create the update configuration command (common updates)
  std::vector<std::string> updateconf_command;
  updateconf_command.push_back("updateconf");
  // Disable optimizations (limits) when using DSE
  if (!use_dse_) {
    updateconf_command.push_back("--rt=10000");
    updateconf_command.push_back("read_request_timeout_in_ms:10000");
    updateconf_command.push_back("write_request_timeout_in_ms:10000");
    updateconf_command.push_back("request_timeout_in_ms:10000");
    updateconf_command.push_back("phi_convict_threshold:16");
    updateconf_command.push_back("hinted_handoff_enabled:false");
    updateconf_command.push_back("dynamic_snitch_update_interval_in_ms:1000");
    updateconf_command.push_back("native_transport_max_threads:1");
    updateconf_command.push_back("rpc_min_threads:1");
    updateconf_command.push_back("rpc_max_threads:1");
    updateconf_command.push_back("concurrent_reads:2");
    updateconf_command.push_back("concurrent_writes:2");
    updateconf_command.push_back("concurrent_compactors:1");
    updateconf_command.push_back("compaction_throughput_mb_per_sec:0");
    updateconf_command.push_back("key_cache_size_in_mb:0");
    updateconf_command.push_back("key_cache_save_period:0");
    updateconf_command.push_back("memtable_flush_writers:1");
    updateconf_command.push_back("max_hints_delivery_threads:1");

    // Create Cassandra version specific updates (C* v1.2.x)
    if (cassandra_version < "2.0.0") {
      updateconf_command.push_back("reduce_cache_sizes_at:0");
      updateconf_command.push_back("reduce_cache_capacity_to:0");
      updateconf_command.push_back("flush_largest_memtables_at:0");
      updateconf_command.push_back("index_interval:512");
    } else {
      updateconf_command.push_back("cas_contention_timeout_in_ms:10000");
      updateconf_command.push_back("file_cache_size_in_mb:0");
    }

    // Create Cassandra version specific updates (C* < v2.1)
    if (cassandra_version < "2.1.0") {
      updateconf_command.push_back("in_memory_compaction_limit_in_mb:1");
    }
  }

  // Create Cassandra version specific updated (C* 2.2+)
  if (cassandra_version >= "2.2.0") {
    updateconf_command.push_back("enable_user_defined_functions:true");
  }

  // Create Cassandra version specific updated (C* 3.0+)
  if (cassandra_version >= "3.0.0") {
    updateconf_command.push_back("enable_scripted_user_defined_functions:true");
  }

  return updateconf_command;
}

std::string CCM::Bridge::generate_dse_workloads(std::vector<DseWorkload> workloads) {
  std::string dse_workloads;
  for (std::vector<DseWorkload>::iterator iterator = workloads.begin();
    iterator != workloads.end(); ++iterator) {
    dse_workloads += dse_workloads_[*iterator];
    if ((iterator + 1) != workloads.end()) {
      dse_workloads += ",";
    }
  }
  return dse_workloads;
}

std::string CCM::Bridge::generate_node_name(unsigned int node) {
  std::stringstream node_name;
  node_name << "node" << node;
  return node_name.str();
}

unsigned int CCM::Bridge::get_next_available_node() {
  ClusterStatus status = cluster_status();
  unsigned int next_available_node = status.node_count + 1;
  if (next_available_node > CLUSTER_NODE_LIMIT) {
    throw BridgeException("No Nodes are Available: Limiting total nodes for CCM to " + CLUSTER_NODE_LIMIT);
  }

  return next_available_node;
}

bool CCM::Bridge::is_node_availabe(unsigned int node) {
  std::stringstream ip_address;
  ip_address << get_ip_prefix() << node;
  return is_node_availabe(ip_address.str());
}

bool CCM::Bridge::is_node_availabe(const std::string& ip_address) {
  Socket socket;
  try {
    socket.establish_connection(ip_address, CASSANDRA_BINARY_PORT);
    return true;
  } catch (...) {
    ; // No-op
  }

  return false;
}

std::string CCM::Bridge::to_lower(const std::string& input) {
  std::string lowercase = input;
  std::transform(lowercase.begin(), lowercase.end(), lowercase.begin(), ::tolower);
  return lowercase;
}

std::string CCM::Bridge::trim(const std::string& input) {
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

std::string CCM::Bridge::implode(const std::vector<std::string>& elements, const char delimiter /*= ' '*/) {
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

std::vector<std::string> CCM::Bridge::explode(const std::string& input, const char delimiter /*= ' '*/) {
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

void CCM::Bridge::msleep(unsigned int milliseconds) {
#ifdef _WIN32
  Sleep(milliseconds);
#else
  //Convert the milliseconds into a proper timespec structure
  time_t seconds = static_cast<int>(milliseconds / 1000);
  long int nanoseconds = static_cast<long int>((milliseconds - (seconds * 1000)) * 1000000);

  //Assign the requested time and perform sleep
  struct timespec requested;
  requested.tv_sec = seconds;
  requested.tv_nsec = nanoseconds;
  while (nanosleep(&requested, &requested) == -1) {
    continue;
  }
#endif
}
