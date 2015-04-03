/*
 Copyright (c) 2014-2015 DataStax

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

#ifndef CQL_CCM_BRIDGE_H_
#define CQL_CCM_BRIDGE_H_

#include <exception>
#include <deque>
#include <string.h>

#include <boost/smart_ptr.hpp>
#include <boost/noncopyable.hpp>

#include "cql_ccm_bridge_configuration.hpp"
#include "cql_escape_sequences_remover.hpp"

/**
 * Cassandra release version number
 */
struct CassVersion {
  /**
   * Major portion of version number
   */
  unsigned short major;
  /**
   * Minor portion of version number
   */
  unsigned short minor;
  /**
   * Patch portion of version number
   */
  unsigned short patch;
  /**
   * Extra portion of version number
   */
  char extra[64];

  /**
   * Initializing constructor for structure
   */
  CassVersion() {
    major = 0;
    minor = 0;
    patch = 0;
    memset(extra, '\0', sizeof(extra));
  };
};

namespace cql {
class cql_ccm_bridge_t : public boost::noncopyable {
 public:
  cql_ccm_bridge_t(const cql_ccm_bridge_configuration_t& settings);
  ~cql_ccm_bridge_t();

  // Executes command on remote host
  // Returns command stdout and stderr followed by
  // shell prompth.
  std::string execute_command(const std::string& command);

  void update_config(const std::string& name, const std::string& value);

  void start();
  void start(int node);
  void start(int node, const std::string& option);
  void stop();
  void stop(int node);
  void pause(int node);
  void resume(int node);
  void kill();
  void kill(int node);
  void binary(int node, bool enable);
  void gossip(int node, bool enable);

  void remove();
  void ring(int node);

  void populate(int n);

  void add_node(int node);
  void add_node(int node, const std::string& dc);
  void bootstrap(int node);
  void bootstrap(int node, const std::string& dc);

  void decommission(int node);

  /**
   * Get the configuration version of Cassandra
   *
   * @return CassVersion defining version information from configuration
   */
  CassVersion version();
  /**
   * Get the version of a Cassandra node
   *
   * @param node Node to get version from
   * @return CassVersion defining version information for Cassandra node
   */
  CassVersion version(int node);

  //TODO: Allow CCM create to keep instances if settings are the same
  static boost::shared_ptr<cql_ccm_bridge_t> create(const cql_ccm_bridge_configuration_t& settings,
                                                    const std::string& name, bool is_ssl = false,
                                                    bool is_client_authentication = false);

  static boost::shared_ptr<cql_ccm_bridge_t> create_and_start(
      const cql_ccm_bridge_configuration_t& settings, const std::string& name,
      unsigned nodes_count_dc1, unsigned nodes_count_dc2 = 0, bool is_ssl = false,
      bool is_client_authentication = false);

 private:
  /* CCM functionality */
  static const std::string CCM_COMMAND;
  const std::string _ip_prefix;
  const std::string _cassandra_version;

  void execute_ccm_command(const std::string& ccm_args);
  void execute_ccm_and_print(const std::string& ccm_args);

  /**
   * Get the version information from the configuration file and parse the
   * data into a CassVersion structure
   *
   * @return CassVersion defining version information for CCM configuration
   */
  CassVersion get_cassandra_version();
  /**
   * Execute CCM command to get version information from a node and parse the
   * data into a CassVersion structure
   *
   * @param node Node to get version information from
   * @return CassVersion defining version information for Cassandra node
   */
  CassVersion get_cassandra_version(int node);

  /* SSH connection functionality */

  void initialize_environment();
  void wait_for_shell_prompth();

  std::string terminal_read_stdout();
  std::string terminal_read_stderr();
  std::string terminal_read(cql_escape_sequences_remover_t& buffer, int stream);
  void terminal_read_stream(cql_escape_sequences_remover_t& buffer, int stream);

  void terminal_write(const std::string& command);

  void initialize_socket_library();
  void finalize_socket_library();

  void start_connection(const cql_ccm_bridge_configuration_t& settings);
  void close_socket();
  void start_ssh_connection(const cql_ccm_bridge_configuration_t& settings);
  void close_ssh_session();

  cql_escape_sequences_remover_t _esc_remover_stdout;
  cql_escape_sequences_remover_t _esc_remover_stderr;

  int _socket;
  struct ssh_internals;
  boost::scoped_ptr<ssh_internals> _ssh_internals;
};

class cql_ccm_bridge_exception_t : public std::exception {
 public:
  cql_ccm_bridge_exception_t(const char* message)
      : _message(message) {
  }

  virtual const char* what() const throw () {
    return _message;
  }
 private:
  const char* const _message;
};
}

#endif // CQL_CCM_BRIDGE_H_
