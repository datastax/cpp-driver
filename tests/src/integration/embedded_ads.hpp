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

#ifndef __TEST_EMBEDDED_ADS_HPP__
#define __TEST_EMBEDDED_ADS_HPP__
#include "exception.hpp"
#include "options.hpp"
#include "test_utils.hpp"
#include "tlog.hpp"

#include "scoped_lock.hpp"

#include <string>
#ifdef _WIN32
#define putenv _putenv
#endif

#include <uv.h>

// TODO: This should be broken out in the future if required by more than one test (currently
// Authentication tests)

// Defines for ADS configuration
#define EMBEDDED_ADS_JAR_FILENAME "embedded-ads.jar"
#define EMBEDDED_ADS_CONFIGURATION_DIRECTORY "ads_config"
#define EMBEDDED_ADS_CONFIGURATION_FILE "krb5.conf"
#define CASSANDRA_KEYTAB_ADS_CONFIGURATION_FILE "cassandra.keytab"
#define DSE_KEYTAB_ADS_CONFIGURATION_FILE "dse.keytab"
#define DSE_USER_KEYTAB_ADS_CONFIGURATION_FILE "dseuser.keytab"
#define UNKNOWN_KEYTAB_ADS_CONFIGURATION_FILE "unknown.keytab"
#define BILL_KEYTAB_ADS_CONFIGURATION_FILE "bill.keytab"
#define BOB_KEYTAB_ADS_CONFIGURATION_FILE "bob.keytab"
#define CHARLIE_KEYTAB_ADS_CONFIGURATION_FILE "charlie.keytab"
#define STEVE_KEYTAB_ADS_CONFIGURATION_FILE "steve.keytab"
#define REALM "DATASTAX.COM"
#define DSE_SERVICE_PRINCIPAL "dse/_HOST@DATASTAX.COM"
#define CASSANDRA_USER "cassandra"
#define CASSANDRA_PASSWORD "cassandra"
#define CASSANDRA_USER_PRINCIPAL "cassandra@DATASTAX.COM"
#define DSE_USER "dseuser"
#define DSE_USER_PRINCIPAL "dseuser@DATASTAX.COM"
#define UNKNOWN "unknown"
#define UNKNOWN_PRINCIPAL "unknown@DATASTAX.COM"
#define BILL_PRINCIPAL "bill@DATASTAX.COM"
#define BOB_PRINCIPAL "bob@DATASTAX.COM"
#define CHARLIE_PRINCIPAL "charlie@DATASTAX.COM"
#define STEVE_PRINCIPAL "steve@DATASTAX.COM"

// Output buffer size for spawn pipe(s)
#define OUTPUT_BUFFER_SIZE 10240

namespace test {

/**
 * Embedded ADS for easily authenticating with DSE using Kerberos
 */
class EmbeddedADS {
  /**
   * Result for command execution
   */
  typedef struct CommandResult_ {
    /**
     * Error code (e.g. exit status)
     */
    int error_code;
    /**
     * Standard output from executing command
     */
    std::string standard_output;
    /**
     * Standard error from executing command
     */
    std::string standard_error;

    CommandResult_()
        : error_code(-1) {}
  } CommandResult;

public:
  /**
   * @throws EmbeddedADS::Exception If applications are not available to operate the ADS
   *                        properly
   */
  EmbeddedADS() {
    // TODO: Update test to work with remote deployments
#ifdef _WIN32
    // Unable to execute ADS locally and use remote DSE cluster
    throw Exception("ADS Server will not be Created: Must run locally with DSE cluster");
#endif
#ifdef CASS_USE_LIBSSH2
    if (Options::deployment_type() == CCM::DeploymentType::REMOTE) {
      throw Exception("ADS Server will not be Created: Must run locally with DSE cluster");
    }
#endif

    // Initialize the mutex
    uv_mutex_init(&mutex_);

    // Check to see if all applications and files are available for ADS
    bool is_useable = true;
    std::string message;
    if (!is_java_available()) {
      is_useable = false;
      message += "Java";
    }
    if (!is_kerberos_client_available()) {
      is_useable = false;
      if (!message.empty()) {
        message += " and ";
      }
      message += "Kerberos clients (kinit/kdestroy)";
    }
    if (!Utils::file_exists(EMBEDDED_ADS_JAR_FILENAME)) {
      is_useable = false;
      if (!message.empty()) {
        message += " and ";
      }
      message += "embedded ADS JAR file";
    }

    if (!is_useable) {
      message = "Unable to Create ADS Server: Missing " + message;
      throw Exception(message);
    }
  }

  ~EmbeddedADS() {
    terminate_process();
    uv_mutex_destroy(&mutex_);
  }

  /**
   * Start the ADS process
   */
  void start_process() { uv_thread_create(&thread_, EmbeddedADS::process_start, NULL); }

  /**
   * Terminate the ADS process
   */
  void terminate_process() {
    uv_process_kill(&process_, SIGTERM);
    uv_thread_join(&thread_);

    // Reset the static variables
    configuration_directory_ = "";
    configuration_file_ = "";
    cassandra_keytab_file_ = "";
    dse_keytab_file_ = "";
    dseuser_keytab_file_ = "";
    unknown_keytab_file_ = "";
    bill_keytab_file_ = "";
    bob_keytab_file_ = "";
    charlie_keytab_file_ = "";
    steve_keytab_file_ = "";
    is_initialized_ = false;
  }

  /**
   * Flag to determine if the ADS process is fully initialized
   *
   * @return True is ADS is initialized; false otherwise
   */
  static bool is_initialized() {
    datastax::internal::ScopedMutex lock(&mutex_);
    return is_initialized_;
  }

  /**
   * Get the configuration director being used by the ADS process
   *
   * @return Absolute path to the ADS configuration directory; empty string
   *         indicates ADS was not started properly
   */
  static std::string get_configuration_directory() { return configuration_directory_; }

  /**
   * Get the configuration file being used by the ADS process
   *
   * @return Absolute path to the ADS configuration file; empty string indicates
   *         ADS was not started properly
   */
  static std::string get_configuration_file() { return configuration_file_; }

  /**
   * Get the Cassandra keytab configuration file being used by the ADS process
   *
   * @return Absolute path to the Cassandra keytab configuration file; empty
   *         string indicates ADS was not started properly
   */
  static std::string get_cassandra_keytab_file() { return cassandra_keytab_file_; }

  /**
   * Get the DSE keytab configuration file being used by the ADS process
   *
   * @return Absolute path to the DSE keytab configuration file; empty
   *         string indicates ADS was not started properly
   */
  static std::string get_dse_keytab_file() { return dse_keytab_file_; }

  /**
   * Get the DSE user keytab configuration file being used by the ADS process
   *
   * @return Absolute path to the DSE user keytab configuration file; empty
   *         string indicates ADS was not started properly
   */
  static std::string get_dseuser_keytab_file() { return dseuser_keytab_file_; }

  /**
   * Get the unknown keytab configuration file being used by the ADS process
   *
   * @return Absolute path to the unknown keytab configuration file; empty
   *         string indicates ADS was not started properly
   */
  static std::string get_unknown_keytab_file() { return unknown_keytab_file_; }

  /**
   * Get the Bill keytab configuration file being used by the ADS process
   *
   * @return Absolute path to the Bill keytab configuration file; empty string
   *         indicates ADS was not started properly
   */
  static std::string get_bill_keytab_file() { return bill_keytab_file_; }

  /**
   * Get the Bob keytab configuration file being used by the ADS process
   *
   * @return Absolute path to the Bob keytab configuration file; empty string
   *         indicates ADS was not started properly
   */
  static std::string get_bob_keytab_file() { return bob_keytab_file_; }

  /**
   * Get the Charlie keytab configuration file being used by the ADS process
   *
   * @return Absolute path to the Charlie keytab configuration file; empty
   *         string indicates ADS was not started properly
   */
  static std::string get_charlie_keytab_file() { return charlie_keytab_file_; }

  /**
   * Get the Steve keytab configuration file being used by the ADS process
   *
   * @return Absolute path to the Steve keytab configuration file; empty string
   *         string indicates ADS was not started properly
   */
  static std::string get_steve_keytab_file() { return steve_keytab_file_; }

  /**
   * Check to see if the Kerberos client binaries are Heimdal
   *
   * @return True if Kerberos implementation is Heimdal; false otherwise
   */
  static bool is_kerberos_client_heimdal() {
    if (is_kerberos_client_available()) {
      // kinit
      char* kinit_args[3];
      kinit_args[0] = const_cast<char*>("kinit");
      kinit_args[1] = const_cast<char*>("--version");
      kinit_args[2] = NULL;

      // Check the output of the kinit command for Heimdal
      CommandResult result = execute_command(kinit_args);
      if (result.error_code == 0) {
        // Check both outputs
        bool is_in_standard_output = Utils::contains(result.standard_output, "Heimdal");
        bool is_in_standard_error = Utils::contains(result.standard_error, "Heimdal");
        return (is_in_standard_output || is_in_standard_error);
      }
    }

    return false;
  }

  /**
   * Acquire a ticket into the cache of the ADS for a given principal and keytab
   * file
   *
   * @param principal Principal identity
   * @param keytab_file Filename of keytab to use
   */
  void acquire_ticket(const std::string& principal, const std::string& keytab_file) {
    char* args[6];
    args[0] = const_cast<char*>("kinit");
    args[1] = const_cast<char*>("-k");
    args[2] = const_cast<char*>("-t");
    args[3] = const_cast<char*>(keytab_file.c_str());
    args[4] = const_cast<char*>(principal.c_str());
    args[5] = NULL;
    execute_command(args);
  }

  /**
   * Destroy all tickets in the cache
   */
  void destroy_tickets() {
    char* args[3];
    args[0] = const_cast<char*>("kdestroy");
    args[1] = const_cast<char*>("-A");
    args[2] = NULL;
    execute_command(args);
  }

  /**
   * Assign the Kerberos environment for keytab use
   *
   * @param keytab_file Filename of keytab to use
   */
  void use_keytab(const std::string& keytab_file) {
    // MIT Kerberos
    setenv("KRB5_CLIENT_KTNAME", keytab_file);
    // Heimdal
    setenv("KRB5_KTNAME", keytab_file);
  }

  /**
   * Clear/Unassign the Kerberos environment for keytab use
   */
  void clear_keytab() {
    // MIT Kerberos
    setenv("KRB5_CLIENT_KTNAME", "");
    // Heimdal
    setenv("KRB5_KTNAME", "");
  }

private:
  /**
   * Thread for the ADS process to execute in
   */
  uv_thread_t thread_;
  /**
   * Mutex for process piped buffer allocation and reads
   */
  static uv_mutex_t mutex_;
  /**
   * Information regarding spawned process
   */
  static uv_process_t process_;
  /**
   * ADS configuration directory
   */
  static std::string configuration_directory_;
  /**
   * KRB5_CONFIG configuration file
   */
  static std::string configuration_file_;
  /**
   * Cassandra keytab configuration file
   */
  static std::string cassandra_keytab_file_;
  /**
   * DSE keytab configuration file
   */
  static std::string dse_keytab_file_;
  /**
   * DSE user keytab configuration file
   */
  static std::string dseuser_keytab_file_;
  /**
   * Unknown keytab configuration file
   */
  static std::string unknown_keytab_file_;
  /**
   * Bill keytab configuration file
   */
  static std::string bill_keytab_file_;
  /**
   * Bob keytab configuration file
   */
  static std::string bob_keytab_file_;
  /**
   * Charlie keytab configuration file
   */
  static std::string charlie_keytab_file_;
  /**
   * Steve keytab configuration file
   */
  static std::string steve_keytab_file_;
  /**
   * Flag to determine if the ADS process is initialized
   */
  static bool is_initialized_;

  /**
   * Execute a command while supplying the KRB5_CONFIG to the ADS server
   * configuration file
   *
   * @param Process and arguments to execute
   * @return Error code returned from executing command
   */
  static CommandResult execute_command(char* args[]) {
    // Create the loop
    uv_loop_t loop;
    uv_loop_init(&loop);
    uv_process_options_t options;
    memset(&options, 0, sizeof(uv_process_options_t));

    // Create the options for reading information from the spawn pipes
    uv_pipe_t standard_output;
    uv_pipe_t error_output;
    uv_pipe_init(&loop, &standard_output, 0);
    uv_pipe_init(&loop, &error_output, 0);
    uv_stdio_container_t stdio[3];
    options.stdio_count = 3;
    options.stdio = stdio;
    options.stdio[0].flags = UV_IGNORE;
    options.stdio[1].flags = static_cast<uv_stdio_flags>(UV_CREATE_PIPE | UV_WRITABLE_PIPE);
    options.stdio[1].data.stream = (uv_stream_t*)&standard_output;
    options.stdio[2].flags = static_cast<uv_stdio_flags>(UV_CREATE_PIPE | UV_WRITABLE_PIPE);
    options.stdio[2].data.stream = (uv_stream_t*)&error_output;

    // Create the options for the process
    options.args = args;
    options.exit_cb = EmbeddedADS::process_exit;
    options.file = args[0];

    // Start the process and process loop (if spawned)
    CommandResult result;
    uv_process_t process;
    result.error_code = uv_spawn(&loop, &process, &options);
    if (result.error_code == 0) {
      TEST_LOG("Launched " << args[0] << " with ID " << process_.pid);

      // Configure the storage for the output pipes
      std::string stdout_message;
      std::string stderr_message;
      standard_output.data = &result.standard_output;
      error_output.data = &result.standard_error;

      // Start the output thread loops
      uv_read_start(reinterpret_cast<uv_stream_t*>(&standard_output),
                    EmbeddedADS::output_allocation, EmbeddedADS::process_read);
      uv_read_start(reinterpret_cast<uv_stream_t*>(&error_output), EmbeddedADS::output_allocation,
                    EmbeddedADS::process_read);

      // Start the process loop
      uv_run(&loop, UV_RUN_DEFAULT);
      uv_loop_close(&loop);
    }
    return result;
  }

  /**
   * Check to see if Java is available in order to execute the ADS process
   *
   * @return True if Java is available; false otherwise
   */
  static bool is_java_available() {
    char* args[3];
    args[0] = const_cast<char*>("java");
    args[1] = const_cast<char*>("-help");
    args[2] = NULL;
    return (execute_command(args).error_code == 0);
  }

  /**
   * Check to see if the Kerberos client binaries are available in order to
   * properly execute request for the ADS
   *
   * @return True if kinit and kdestroy are available; false otherwise
   */
  static bool is_kerberos_client_available() {
    // kinit
    char* kinit_args[3];
    kinit_args[0] = const_cast<char*>("kinit");
    kinit_args[1] = const_cast<char*>("--help");
    kinit_args[2] = NULL;
    bool is_kinit_available = (execute_command(kinit_args).error_code == 0);

    // kdestroy
    char* kdestroy_args[3];
    kdestroy_args[0] = const_cast<char*>("kdestroy");
    kdestroy_args[1] = const_cast<char*>("--help");
    kdestroy_args[2] = NULL;
    bool is_kdestroy_available = (execute_command(kdestroy_args).error_code == 0);

    return (is_kinit_available && is_kdestroy_available);
  }

  /**
   * uv_thread_create callback for executing the ADS process
   *
   * @param arg UNUSED
   */
  static void process_start(void* arg) {
    // Create the configuration directory for the ADS
    Utils::mkdir(EMBEDDED_ADS_CONFIGURATION_DIRECTORY);

    // Initialize the loop and process arguments
    uv_loop_t loop;
    uv_loop_init(&loop);
    uv_process_options_t options;
    memset(&options, 0, sizeof(uv_process_options_t));

    char* args[7];
    args[0] = const_cast<char*>("java");
    args[1] = const_cast<char*>("-jar");
    args[2] = const_cast<char*>(EMBEDDED_ADS_JAR_FILENAME);
    args[3] = const_cast<char*>("-k");
    args[4] = const_cast<char*>("--confdir");
    args[5] = const_cast<char*>(EMBEDDED_ADS_CONFIGURATION_DIRECTORY);
    args[6] = NULL;

    // Create the options for reading information from the spawn pipes
    uv_pipe_t standard_output;
    uv_pipe_t error_output;
    uv_pipe_init(&loop, &standard_output, 0);
    uv_pipe_init(&loop, &error_output, 0);
    uv_stdio_container_t stdio[3];
    options.stdio_count = 3;
    options.stdio = stdio;
    options.stdio[0].flags = UV_IGNORE;
    options.stdio[1].flags = static_cast<uv_stdio_flags>(UV_CREATE_PIPE | UV_WRITABLE_PIPE);
    options.stdio[1].data.stream = (uv_stream_t*)&standard_output;
    options.stdio[2].flags = static_cast<uv_stdio_flags>(UV_CREATE_PIPE | UV_WRITABLE_PIPE);
    options.stdio[2].data.stream = (uv_stream_t*)&error_output;

    // Create the options for the process
    options.args = args;
    options.exit_cb = EmbeddedADS::process_exit;
    options.file = args[0];

    // Start the process
    int error_code = uv_spawn(&loop, &process_, &options);
    if (error_code == 0) {
      TEST_LOG("Launched " << args[0] << " with ID " << process_.pid);

      // Configure the storage for the output pipes
      std::string stdout_message;
      std::string stderr_message;
      standard_output.data = &stdout_message;
      error_output.data = &stderr_message;

      // Start the output thread loops
      uv_read_start(reinterpret_cast<uv_stream_t*>(&standard_output),
                    EmbeddedADS::output_allocation, EmbeddedADS::process_read);
      uv_read_start(reinterpret_cast<uv_stream_t*>(&error_output), EmbeddedADS::output_allocation,
                    EmbeddedADS::process_read);

      // Indicate the ADS configurations
      configuration_directory_ = Utils::cwd() + Utils::PATH_SEPARATOR +
                                 EMBEDDED_ADS_CONFIGURATION_DIRECTORY + Utils::PATH_SEPARATOR;
      configuration_file_ = configuration_directory_ + EMBEDDED_ADS_CONFIGURATION_FILE;
      cassandra_keytab_file_ = configuration_directory_ + CASSANDRA_KEYTAB_ADS_CONFIGURATION_FILE;
      dse_keytab_file_ = configuration_directory_ + DSE_KEYTAB_ADS_CONFIGURATION_FILE;
      dseuser_keytab_file_ = configuration_directory_ + DSE_USER_KEYTAB_ADS_CONFIGURATION_FILE;
      unknown_keytab_file_ = configuration_directory_ + UNKNOWN_KEYTAB_ADS_CONFIGURATION_FILE;
      bill_keytab_file_ = configuration_directory_ + BILL_KEYTAB_ADS_CONFIGURATION_FILE;
      bob_keytab_file_ = configuration_directory_ + BOB_KEYTAB_ADS_CONFIGURATION_FILE;
      charlie_keytab_file_ = configuration_directory_ + CHARLIE_KEYTAB_ADS_CONFIGURATION_FILE;
      steve_keytab_file_ = configuration_directory_ + STEVE_KEYTAB_ADS_CONFIGURATION_FILE;

      // Inject the configuration environment variable
      setenv("KRB5_CONFIG", configuration_file_);

      // Start the process loop
      uv_run(&loop, UV_RUN_DEFAULT);
      uv_loop_close(&loop);
    } else {
      TEST_LOG_ERROR(uv_strerror(error_code));
    }
  }

  /**
   * uv_spawn callback for handling the completion of the process
   *
   * @param process Process
   * @param error_code Error/Exit code
   * @param term_signal Terminating signal
   */
  static void process_exit(uv_process_t* process, int64_t error_code, int term_signal) {
    datastax::internal::ScopedMutex lock(&mutex_);
    TEST_LOG("Process " << process->pid << " Terminated: " << error_code);
    uv_close(reinterpret_cast<uv_handle_t*>(process), NULL);
  }

  /**
   * uv_read_start callback for allocating memory for the buffer in the pipe
   *
   * @param handle Handle information for the pipe being read
   * @param suggested_size Suggested size for the buffer
   * @param buffer Buffer to allocate bytes for
   */
  static void output_allocation(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buffer) {
    datastax::internal::ScopedMutex lock(&mutex_);
    buffer->base = new char[OUTPUT_BUFFER_SIZE];
    buffer->len = OUTPUT_BUFFER_SIZE;
  }

  /**
   * uv_read_start callback for processing the buffer in the pipe
   *
   * @param stream Stream to process (stdout/stderr)
   * @param buffer_length Length of the buffer
   * @param buffer Buffer to process
   */
  static void process_read(uv_stream_t* stream, ssize_t buffer_length, const uv_buf_t* buffer) {
    datastax::internal::ScopedMutex lock(&mutex_);

    // Get the pipe message contents
    std::string* message = reinterpret_cast<std::string*>(stream->data);

    if (buffer_length > 0) {
      // Process the buffer and determine if the ADS is finished initializing
      std::string output(buffer->base, buffer_length);
      message->append(output);

      if (!is_initialized_ &&
          message->find("Principal Initialization Complete") != std::string::npos) {
        Utils::msleep(10000); // TODO: Not 100% ready; need to add a better check mechanism
        is_initialized_ = true;
      }
      TEST_LOG(Utils::trim(output));
    } else if (buffer_length < 0) {
      uv_close(reinterpret_cast<uv_handle_t*>(stream), NULL);
    }

    // Clean up the memory allocated
    delete[] buffer->base;
  }

  static void setenv(const std::string& name, const std::string& value) {
#ifdef _WIN32
    putenv(const_cast<char*>(std::string(name + "=" + value).c_str()));
#else
    ::setenv(name.c_str(), value.c_str(), 1);
#endif
  }
};

} // namespace test

#endif // __TEST_EMBEDDED_ADS_HPP__
