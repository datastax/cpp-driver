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

#include "logger.hpp"

#define DSE_PROTOCOL_VERSION_BIT  0x40
#define DSE_PROTOCOL_VERSION_MASK 0x3F

namespace cass {

ProtocolVersion::ProtocolVersion()
  : value_(-1) { }

ProtocolVersion::ProtocolVersion(int value)
  : value_(value) { }

ProtocolVersion ProtocolVersion::lowest_supported() {
  return ProtocolVersion(CASS_LOWEST_SUPPORTED_PROTOCOL_VERSION);
}

ProtocolVersion ProtocolVersion::highest_supported() {
  return ProtocolVersion(CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION);
}

ProtocolVersion ProtocolVersion::newest_beta() {
  return ProtocolVersion(CASS_NEWEST_BETA_PROTOCOL_VERSION);
}

int ProtocolVersion::value() const {
  return value_;
}

bool ProtocolVersion::is_valid() const {
  if (value_ < CASS_LOWEST_SUPPORTED_PROTOCOL_VERSION) {
    LOG_ERROR("Protocol version %s is lower than the lowest supported "
              " protocol version %s",
              to_string().c_str(),
              lowest_supported().to_string().c_str());
    return false;
  } else if (value_ > CASS_HIGHEST_SUPPORTED_PROTOCOL_VERSION) {
    LOG_ERROR("Protocol version %s is higher than the highest supported "
              "protocol version %s (consider using the newest beta protocol version).",
              to_string().c_str(),
              highest_supported().to_string().c_str());
    return false;
  }
  return true;
}

bool ProtocolVersion::is_beta() const {
  return value_ == CASS_NEWEST_BETA_PROTOCOL_VERSION;
}

String ProtocolVersion::to_string() const {
  if (value_ > 0) {
    OStringStream ss;
    if (value_ & DSE_PROTOCOL_VERSION_BIT) {
      ss << "DSEv" << (value_ & DSE_PROTOCOL_VERSION_MASK);
   } else {
      ss << "v" << value_;
    }
    return ss.str();
  } else {
    return "<invalid>";
  }
}

bool ProtocolVersion::attempt_lower_supported(const String& host) {
  if (value_ <= CASS_LOWEST_SUPPORTED_PROTOCOL_VERSION) {
    LOG_ERROR("Host %s does not support any valid protocol version (lowest supported version is %s)",
              host.c_str(),
              lowest_supported().to_string().c_str());
    return false;
  }

  int previous_version = value_--;
  LOG_WARN("Host %s does not support protocol version %s. "
           "Trying protocol version %s...",
           host.c_str(),
           ProtocolVersion(previous_version).to_string().c_str(),
           to_string().c_str());
  return true;
}

bool ProtocolVersion::supports_set_keyspace() const {
  assert(value_ > 0 && "Invalid protocol version");
  return value_ >= CASS_PROTOCOL_VERSION_V5;
}

bool ProtocolVersion::supports_result_metadata_id() const {
  assert(value_ > 0 && "Invalid protocol version");
  // Cassandra v4.x broke the beta protocol v5 version
  // JIRA: https://issues.apache.org/jira/browse/CASSANDRA-10786
  // Mailing List Discussion: https://www.mail-archive.com/dev@cassandra.apache.org/msg11731.html
  // TODO: Remove after Cassandra v4.0.0 is released
  return false;
  //return version_ >= CASS_PROTOCOL_VERSION_V5;
}

} // namespace cass
