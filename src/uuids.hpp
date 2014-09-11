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

#ifndef __CASS_UUIDS_HPP_INCLUDED__
#define __CASS_UUIDS_HPP_INCLUDED__

#include "third_party/boost/boost/atomic.hpp"
#include "third_party/boost/boost/cstdint.hpp"

#include <uv.h>
#include <openssl/evp.h>
#include <assert.h>
#include <string.h>

#define TIME_OFFSET_BETWEEN_UTC_AND_EPOCH 0x01B21DD213814000LL // Nanoseconds

namespace cass {

// This is a port of the DataStax java-driver UUIDs class

typedef unsigned char Uuid[16];

class Uuids {
public:
  static void initialize_(); // Do not call this

  static void generate_v1(Uuid uuid);
  static void generate_v1(uint64_t timestamp, Uuid uuid);
  static void generate_v4(Uuid uuid);
  static void min_v1(uint64_t timestamp, Uuid uuid);
  static void max_v1(uint64_t timestamp, Uuid uuid);

  static uint64_t get_unix_timestamp(Uuid uuid);
  static uint8_t get_version(Uuid uuid) { return (uuid[6] >> 4) & 0x0F; }
  static void to_string(Uuid uuid, char* output);

private:
  static uint64_t CLOCK_SEQ_AND_NODE; // Calculated
  static const uint64_t MIN_CLOCK_SEQ_AND_NODE = 0x8080808080808080LL;
  static const uint64_t MAX_CLOCK_SEQ_AND_NODE = 0x7f7f7f7f7f7f7f7fLL;

  static uint64_t to_milliseconds(uint64_t timestamp) {
    return timestamp / 10000L;
  }

  static uint64_t from_unix_timestamp(uint64_t timestamp) {
    return (timestamp * 10000L) + TIME_OFFSET_BETWEEN_UTC_AND_EPOCH;
  }

  static void copy_timestamp(uint64_t timestamp, uint8_t version, Uuid uuid);

  static void copy_clock_seq_and_node(uint64_t lsb, Uuid uuid) {
    for (size_t i = 0; i < 8; ++i) {
      uuid[15 - i] = lsb & 0x00000000000000FFL;
      lsb >>= 8;
    }
  }

  static uint64_t uuid_timestamp();
  static uint64_t make_clock_seq_and_node();

private:
  static uv_mutex_t mutex_;
  static boost::atomic<uint64_t> last_timestamp_;
};

} // namespace cass

#endif
