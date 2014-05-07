/*
  Copyright 2014 DataStax

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

#ifndef __CASS_COMMON_HPP_INCLUDED__
#define __CASS_COMMON_HPP_INCLUDED__

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

#include <thread>
#include <deque>
#include <list>
#include <map>
#include <string>

#include "cassandra.h"

#define CASS_OPCODE_ERROR        0x00
#define CASS_OPCODE_STARTUP      0x01
#define CASS_OPCODE_READY        0x02
#define CASS_OPCODE_AUTHENTICATE 0x03
#define CASS_OPCODE_CREDENTIALS  0x04
#define CASS_OPCODE_OPTIONS      0x05
#define CASS_OPCODE_SUPPORTED    0x06
#define CASS_OPCODE_QUERY        0x07
#define CASS_OPCODE_RESULT       0x08
#define CASS_OPCODE_PREPARE      0x09
#define CASS_OPCODE_EXECUTE      0x0A
#define CASS_OPCODE_REGISTER     0x0B
#define CASS_OPCODE_EVENT        0x0C
#define CASS_OPCODE_BATCH        0x0D

#include "future.hpp"
#include "error.hpp"
#include "message.hpp"

namespace cass {

typedef std::function<void(int, const char*, size_t)> LogCallback;

struct Session;
typedef std::unique_ptr<Message>               MessagePtr;
typedef FutureImpl<std::string, MessagePtr> MessageFutureImpl;
typedef FutureImpl<Session*, Session*>   SessionFutureImpl;

uv_buf_t
alloc_buffer(
    size_t suggested_size) {
  return uv_buf_init(new char[suggested_size], suggested_size);
}

uv_buf_t
alloc_buffer(
    uv_handle_t *handle,
    size_t       suggested_size) {
  (void) handle;
  return alloc_buffer(suggested_size);
}

void
free_buffer(
    uv_buf_t buf) {
  delete buf.base;
}

void
clear_buffer_deque(
    std::deque<uv_buf_t>& buffers) {
  for (std::deque<uv_buf_t>::iterator it = buffers.begin();
       it != buffers.end();
       ++it) {
    free_buffer(*it);
  }
  buffers.clear();
}

// From boost 1.55
template <class T>
inline void hash_combine(std::size_t& seed, T const& v)
{
  std::hash<T> hasher;
  seed ^= hasher(v) + 0x9e3779b9 + (seed<<6) + (seed>>2);
}

std::string
opcode_to_string(
    int opcode) {
  switch (opcode) {
    case CASS_OPCODE_ERROR:
      return "CASS_OPCODE_ERROR";
    case CASS_OPCODE_STARTUP:
      return "CASS_OPCODE_STARTUP";
    case CASS_OPCODE_READY:
      return "CASS_OPCODE_READY";
    case CASS_OPCODE_AUTHENTICATE:
      return "CASS_OPCODE_AUTHENTICATE";
    case CASS_OPCODE_CREDENTIALS:
      return "CASS_OPCODE_CREDENTIALS";
    case CASS_OPCODE_OPTIONS:
      return "CASS_OPCODE_OPTIONS";
    case CASS_OPCODE_SUPPORTED:
      return "CASS_OPCODE_SUPPORTED";
    case CASS_OPCODE_QUERY:
      return "CASS_OPCODE_QUERY";
    case CASS_OPCODE_RESULT:
      return "CASS_OPCODE_RESULT";
    case CASS_OPCODE_PREPARE:
      return "CASS_OPCODE_PREPARE";
    case CASS_OPCODE_EXECUTE:
      return "CASS_OPCODE_EXECUTE";
    case CASS_OPCODE_REGISTER:
      return "CASS_OPCODE_REGISTER";
    case CASS_OPCODE_EVENT:
      return "CASS_OPCODE_EVENT";
  };
  assert(false);
  return "";
}

} // namespace cass

#endif
