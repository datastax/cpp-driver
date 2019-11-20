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

#ifndef CCM_DEPLOYMENT_TYPE_HPP
#define CCM_DEPLOYMENT_TYPE_HPP

#include <algorithm>
#include <string>

namespace CCM {

/**
 * Deployment type indicating how CCM commands should be executed
 */
class DeploymentType {
public:
#ifdef CASS_USE_LIBSSH2
  enum Type { INVALID, LOCAL, REMOTE };
#else
  enum Type { INVALID, LOCAL };
#endif

  DeploymentType(Type type = LOCAL)
      : type_(type) {}

  const char* name() const {
    switch (type_) {
      case LOCAL:
        return "LOCAL";
#ifdef CASS_USE_LIBSSH2
      case REMOTE:
        return "REMOTE";
#endif
      default:
        return "INVALID";
    }
  }

  const char* to_string() const {
    switch (type_) {
      case LOCAL:
        return "Local";
#ifdef CASS_USE_LIBSSH2
      case REMOTE:
        return "Remote";
#endif
      default:
        return "Invalid Deployment Type";
    }
  }

  bool operator==(const DeploymentType& other) const { return type_ == other.type_; }

  static DeploymentType from_string(const std::string& str) {
    if (iequals(DeploymentType(LOCAL).name(), str)) {
      return DeploymentType(LOCAL);
    }
#ifdef CASS_USE_LIBSSH2
    else if (iequals(DeploymentType(REMOTE).name(), str)) {
      return DeploymentType(REMOTE);
    }
#endif
    return DeploymentType(INVALID);
  }

private:
  static bool iequalsc(char l, char r) { return std::tolower(l) == std::tolower(r); }

  static bool iequals(const std::string& lhs, const std::string& rhs) {
    return lhs.size() == rhs.size() && std::equal(lhs.begin(), lhs.end(), rhs.begin(), iequalsc);
  }

private:
  Type type_;
};

} // namespace CCM

#endif // CCM_DEPLOYMENT_TYPE_HPP
