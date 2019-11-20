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

#ifndef CCM_DSE_CREDENTIALS_TYPE_HPP
#define CCM_DSE_CREDENTIALS_TYPE_HPP

#include <cctype>
#include <string>

namespace CCM {

/**
 * DSE credentials type indicating how authentication for DSE downloads is
 * performed through CCM
 */
class DseCredentialsType {
public:
  enum Type { INVALID, USERNAME_PASSWORD, INI_FILE };

  DseCredentialsType(Type type = USERNAME_PASSWORD)
      : type_(type) {}

  const char* name() const {
    switch (type_) {
      case USERNAME_PASSWORD:
        return "USERNAME_PASSWORD";
      case INI_FILE:
        return "INI_FILE";
      default:
        return "INVALID";
    }
  }

  const char* to_string() const {
    switch (type_) {
      case USERNAME_PASSWORD:
        return "Username and Password";
      case INI_FILE:
        return "INI Credentials File";
      default:
        return "Invalid DSE Credentials Type";
    }
  }

  bool operator==(const DseCredentialsType& other) const { return type_ == other.type_; }

  static DseCredentialsType from_string(const std::string& str) {
    if (iequals(DseCredentialsType(USERNAME_PASSWORD).name(), str)) {
      return DseCredentialsType(USERNAME_PASSWORD);
    } else if (iequals(DseCredentialsType(INI_FILE).name(), str)) {
      return DseCredentialsType(INI_FILE);
    }
    return DseCredentialsType(INVALID);
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

#endif // CCM_DSE_CREDENTIALS_TYPE_HPP
