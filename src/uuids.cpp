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

#include "uuids.hpp"

#include "cassandra.h"
#include "scoped_mutex.hpp"
#include "get_time.hpp"

#include "third_party/boost/boost/random/random_device.hpp"

extern "C" {

void cass_uuid_generate_time(CassUuid output) {
  cass::Uuids::generate_v1(output);
}

void cass_uuid_from_time(cass_uint64_t time, CassUuid output) {
  cass::Uuids::generate_v1(time, output);
}

void cass_uuid_min_from_time(cass_uint64_t time, CassUuid output) {
  cass::Uuids::min_v1(time, output);
}

void cass_uuid_max_from_time(cass_uint64_t time, CassUuid output) {
  cass::Uuids::max_v1(time, output);
}

cass_uint64_t cass_uuid_timestamp(CassUuid uuid) {
  return cass::Uuids::get_unix_timestamp(uuid);
}

void cass_uuid_generate_random(CassUuid output) {
  cass::Uuids::generate_v4(output);
}

cass_uint8_t cass_uuid_version(CassUuid uuid) {
  return cass::Uuids::get_version(uuid);
}

void cass_uuid_string(CassUuid uuid, char* output) {
  cass::Uuids::to_string(uuid, output);
}

} // extern "C"

namespace {

class UuidsInitializer {
public:
  UuidsInitializer() { cass::Uuids::initialize_(); }
};

UuidsInitializer uuids_intitalizer_;

}

namespace cass {

boost::mt19937_64 Uuids::ng_;
uv_mutex_t Uuids::mutex_;
boost::atomic<uint64_t> Uuids::last_timestamp_;
uint64_t Uuids::CLOCK_SEQ_AND_NODE;

void Uuids::initialize_() {
  boost::random_device rd;
  ng_.seed(rd());

  uv_mutex_init(&mutex_);
  last_timestamp_ = 0L;
  CLOCK_SEQ_AND_NODE = make_clock_seq_and_node();
}

void Uuids::generate_v1(Uuid uuid) {
  generate_v1(uuid_timestamp(), uuid);
}

void Uuids::generate_v1(uint64_t timestamp, Uuid uuid) {
  copy_timestamp(timestamp, 1, uuid);
  copy_clock_seq_and_node(CLOCK_SEQ_AND_NODE, uuid);
}

void Uuids::generate_v4(Uuid uuid) {
  ScopedMutex lock(&mutex_);
  uint64_t msb = ng_();
  uint64_t lsb = ng_();
  lock.unlock();

  copy_timestamp(msb, 4, uuid);
  lsb = (lsb & 0x3FFFFFFFFFFFFFFFLL) | 0x8000000000000000LL; // RFC4122 variant
  copy_clock_seq_and_node(lsb, uuid);
}

void Uuids::min_v1(uint64_t timestamp, Uuid uuid) {
  copy_timestamp(from_unix_timestamp(timestamp), 1, uuid);
  copy_clock_seq_and_node(MIN_CLOCK_SEQ_AND_NODE, uuid);
}

void Uuids::max_v1(uint64_t timestamp, Uuid uuid) {
  copy_timestamp(from_unix_timestamp(timestamp + 1) - 1, 1, uuid);
  copy_clock_seq_and_node(MAX_CLOCK_SEQ_AND_NODE, uuid);
}

uint64_t Uuids::get_unix_timestamp(Uuid uuid) {
  uint64_t timestamp = 0;

  if (get_version(uuid) != 1) {
    return 0;
  }

  timestamp |= static_cast<uint64_t>(uuid[3]);
  timestamp |= static_cast<uint64_t>(uuid[2]) << 8;
  timestamp |= static_cast<uint64_t>(uuid[1]) << 16;
  timestamp |= static_cast<uint64_t>(uuid[0]) << 24;

  timestamp |= static_cast<uint64_t>(uuid[5]) << 32;
  timestamp |= static_cast<uint64_t>(uuid[4]) << 40;

  timestamp |= static_cast<uint64_t>(uuid[7]) << 48;
  timestamp |= static_cast<uint64_t>(uuid[6]) << 56;

  timestamp &= 0x0FFFFFFFFFFFFFFFLL; // Clear version

  return to_milliseconds(timestamp - TIME_OFFSET_BETWEEN_UTC_AND_EPOCH);
}

void Uuids::to_string(Uuid uuid, char* output) {
  size_t pos = 0;
  for (int i = 0; i < 16; ++i) {
    char buf[3] = { '\0' };
    sprintf(buf, "%02x", uuid[i]);
    if (i == 4 || i == 6 || i == 8 || i == 10) {
      output[pos++] = '-';
    }
    output[pos++] = buf[0];
    output[pos++] = buf[1];
  }
  output[pos] = '\0';
}

void Uuids::copy_timestamp(uint64_t timestamp, uint8_t version, Uuid uuid) {
  uuid[3] = timestamp & 0x00000000000000FFL;
  timestamp >>= 8;
  uuid[2] = timestamp & 0x00000000000000FFL;
  timestamp >>= 8;
  uuid[1] = timestamp & 0x00000000000000FFL;
  timestamp >>= 8;
  uuid[0] = timestamp & 0x00000000000000FFL;
  timestamp >>= 8;

  uuid[5] = timestamp & 0x00000000000000FFL;
  timestamp >>= 8;
  uuid[4] = timestamp & 0x00000000000000FFL;
  timestamp >>= 8;

  uuid[7] = timestamp & 0x00000000000000FFL;
  timestamp >>= 8;
  uuid[6] = (timestamp & 0x000000000000000FL) | (version << 4);
}

uint64_t Uuids::uuid_timestamp() {
  while (true) {
    uint64_t now = from_unix_timestamp(get_time_since_epoch_ms());
    uint64_t last = last_timestamp_.load();
    if (now > last) {
      if (last_timestamp_.compare_exchange_strong(last, now)) {
        return now;
      }
    } else {
      uint64_t last_ms = to_milliseconds(last);
      if (to_milliseconds(now) < last_ms) {
        return last_timestamp_.fetch_add(1L);
      }
      uint64_t candidate = last + 1;
      if (to_milliseconds(candidate) == last_ms &&
          last_timestamp_.compare_exchange_strong(last, candidate)) {
        return candidate;
      }
    }
  }
}

uint64_t Uuids::make_clock_seq_and_node() {
  int count = 0;
  EVP_MD_CTX* mdctx = EVP_MD_CTX_create();

  EVP_DigestInit(mdctx, EVP_md5());

  uv_interface_address_t* addresses;
  int address_count;
  if (uv_interface_addresses(&addresses, &address_count).code == 0) {
    for (int i = 0; i < address_count; ++i) {
      char buf[256];
      uv_interface_address_t address = addresses[i];
      EVP_DigestUpdate(mdctx, address.name, strlen(address.name));
      if (address.address.address4.sin_family == AF_INET) {
        uv_ip4_name(&address.address.address4, buf, sizeof(buf));
        EVP_DigestUpdate(mdctx, buf, strlen(buf));
        count++;
      } else if (address.address.address4.sin_family == AF_INET6) {
        uv_ip6_name(&address.address.address6, buf, sizeof(buf));
        EVP_DigestUpdate(mdctx, buf, strlen(buf));
        count++;
      }
    }
    uv_free_interface_addresses(addresses, address_count);
  }

  assert(count > 0 && "No unique information for UUID node portion");

  uv_cpu_info_t* cpu_infos;
  int cpu_count;
  if (uv_cpu_info(&cpu_infos, &cpu_count).code == 0) {
    for (int i = 0; i < cpu_count; ++i) {
      uv_cpu_info_t cpu_info = cpu_infos[i];
      EVP_DigestUpdate(mdctx, cpu_info.model, strlen(cpu_info.model));
    }
    uv_free_cpu_info(cpu_infos, cpu_count);
  }

  uint8_t hash[16];
  EVP_DigestFinal_ex(mdctx, hash, NULL);
  EVP_MD_CTX_destroy(mdctx);

  uint64_t node = 0L;
  for (int i = 0; i < 6; ++i) {
    node |= (0x00000000000000FFLL & (long)hash[i]) << (i * 8);
  }
  node |= 0x0000010000000000LL; // Multicast bit

  uint64_t clock = ng_();
  uint64_t clock_seq_and_node = 0;
  clock_seq_and_node |= (clock & 0x0000000000003FFFLL) << 48;
  clock_seq_and_node |= 0x8000000000000000LL; // RFC4122 variant
  clock_seq_and_node |= node;

  return clock_seq_and_node;
}

} // namespace cass
