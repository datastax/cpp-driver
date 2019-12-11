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

#ifndef __PROCESS_HPP__
#define __PROCESS_HPP__

#include <assert.h>
#include <exception>
#include <stdint.h>
#include <string>
#include <vector>

// Forward declarations for libuv
typedef struct uv_buf_t uv_buf_t;
typedef struct uv_handle_s uv_handle_t;
typedef struct uv_process_s uv_process_t;
typedef struct uv_stream_s uv_stream_t;
#ifdef _MSC_VER
#include <basetsd.h>
#ifndef _SSIZE_T_DEFINED
typedef SSIZE_T ssize_t;
#define _SSIZE_T_DEFINED
#endif
#else
#include <sys/types.h>
#endif

namespace utils {

/**
 * Utility class for spawning external processes
 */
class Process {
public:
  typedef std::vector<std::string> Args;

  /**
   * Result for process execution
   */
  struct Result {
    Result()
        : exit_status(-1) {}

    /**
     * Exit status
     */
    int64_t exit_status;
    /**
     * Standard output from executing process
     */
    std::string standard_output;
    /**
     * Standard error from executing process
     */
    std::string standard_error;
  };

  /**
   * Execute an external process
   *
   * @param command Command array to execute ([0] = command, [1-n] arguments)
   * @throws Process::Exception if failed to execute process
   */
  static Result execute(const Args& command);

private:
  Process() {}

  /**
   * uv_exit_cb for handling the completion of the process
   *
   * @param process Process handle for the executing process
   * @param exit_status Exit status
   * @param term_signal Signal that caused the process to terminate
   */
  static void on_exit(uv_process_t* process, int64_t exit_status, int term_signal);

  /**
   * uv_alloc_cb for allocating memory to use for the buffer in the pipe
   *
   * @param handle Handle information for the pipe being read
   * @param suggested_size Suggested size for the buffer
   * @param buf Buffer used to hold the allocated memory and its length
   */
  static void on_allocate(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf);

  /**
   * uv_read_cb for processing the buffer in the pipe
   *
   * @param stream Stream to process
   * @param nread If > 0 there is data available or < 0 on error. When
   *              we’ve reached EOF, nread will be set to UV_EOF. When
   *              nread < 0, the buf parameter might not point to a valid
   *              buffer; in that case buf.len and buf.base are both set to 0
   * @param buf Buffer containing the data read from the stream
   */
  static void on_read(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf);
};

} // namespace utils

#endif // __PROCESS_HPP__
