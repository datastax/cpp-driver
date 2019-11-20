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

#include "protocol.hpp"

#include "cassandra.h"

#define DSE_PROTOCOL_VERSION_BIT 0x40
#define DSE_PROTOCOL_VERSION_MASK 0x3F

using namespace datastax;
using namespace datastax::internal::core;

static bool is_protocol_at_least_v5_or_dse_v2(int version) {
  if (version & DSE_PROTOCOL_VERSION_BIT) {
    return version >= CASS_PROTOCOL_VERSION_DSEV2;
  } else {
    return version >= CASS_PROTOCOL_VERSION_V5;
  }
}

ProtocolVersion::ProtocolVersion()
    : value_(-1) {}

ProtocolVersion::ProtocolVersion(int value)
    : value_(value) {}

ProtocolVersion ProtocolVersion::lowest_supported() {
  return ProtocolVersion(CASS_PROTOCOL_VERSION_V3);
}

ProtocolVersion ProtocolVersion::highest_supported(bool is_dse) {
  return ProtocolVersion(is_dse ? CASS_PROTOCOL_VERSION_DSEV2 : CASS_PROTOCOL_VERSION_V4);
}

ProtocolVersion ProtocolVersion::newest_beta() { return ProtocolVersion(CASS_PROTOCOL_VERSION_V5); }

int ProtocolVersion::value() const { return value_; }

bool ProtocolVersion::is_valid() const {
  return *this >= lowest_supported() && *this <= highest_supported(is_dse());
}

bool ProtocolVersion::is_beta() const { return *this == newest_beta(); }

bool ProtocolVersion::is_dse() const { return (value_ & DSE_PROTOCOL_VERSION_BIT) != 0; }

String ProtocolVersion::to_string() const {
  if (value_ > 0) {
    OStringStream ss;
    if (is_dse()) {
      ss << "DSEv" << (value_ & DSE_PROTOCOL_VERSION_MASK);
    } else {
      ss << "v" << value_;
    }
    return ss.str();
  } else {
    return "<invalid>";
  }
}

ProtocolVersion ProtocolVersion::previous() const {
  if (*this <= lowest_supported()) {
    return ProtocolVersion(); // Invalid
  } else if (is_dse() && value_ <= CASS_PROTOCOL_VERSION_DSEV1) {
    // Start trying Cassandra protocol versions
    return ProtocolVersion::highest_supported(false);
  } else {
    return ProtocolVersion(value_ - 1);
  }
}

bool ProtocolVersion::supports_set_keyspace() const {
  assert(value_ > 0 && "Invalid protocol version");
  return is_protocol_at_least_v5_or_dse_v2(value_);
}

bool ProtocolVersion::supports_result_metadata_id() const {
  assert(value_ > 0 && "Invalid protocol version");
  return is_protocol_at_least_v5_or_dse_v2(value_);
}
