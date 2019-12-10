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

#ifndef CCM_AUTHENTICATION_TYPE_HPP
#define CCM_AUTHENTICATION_TYPE_HPP

#include <cctype>
#include <string>

namespace CCM {

/**
 * Authentication type indicating how SSH authentication should be handled
 */
class AuthenticationType {
public:
  enum Type { INVALID, USERNAME_PASSWORD, PUBLIC_KEY };

  AuthenticationType(Type type = USERNAME_PASSWORD)
      : type_(type) {}

  const char* name() const {
    switch (type_) {
      case USERNAME_PASSWORD:
        return "USERNAME_PASSWORD";
      case PUBLIC_KEY:
        return "PUBLIC_KEY";
      default:
        return "INVALID";
    }
  }

  const char* to_string() const {
    switch (type_) {
      case USERNAME_PASSWORD:
        return "Username and Password";
      case PUBLIC_KEY:
        return "Public Key";
      default:
        return "Invalid Authentication Type";
    }
  }

  bool operator==(const AuthenticationType& other) const { return type_ == other.type_; }

  static AuthenticationType from_string(const std::string& str) {
    if (iequals(AuthenticationType(USERNAME_PASSWORD).name(), str)) {
      return AuthenticationType(USERNAME_PASSWORD);
    } else if (iequals(AuthenticationType(PUBLIC_KEY).name(), str)) {
      return AuthenticationType(PUBLIC_KEY);
    }
    return AuthenticationType(INVALID);
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

#endif // CCM_AUTHENTICATION_TYPE_HPP
