/*
  Copyright (c) 2014-2016 DataStax

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
#include "authentication_type.hpp"

#include <algorithm>

using namespace CCM;

// Constant value definitions for authentication type
const AuthenticationType AuthenticationType::USERNAME_PASSWORD("USERNAME_PASSWORD", 0, "Username and Password");
const AuthenticationType AuthenticationType::PUBLIC_KEY("PUBLIC_KEY", 1, "Public Key");

// Static declarations for authentication type
std::set<AuthenticationType> AuthenticationType::constants_;

AuthenticationType::AuthenticationType()
 : name_("INVALID")
 , ordinal_(-1)
 , display_name_("Invalid authentication") {
}

const std::string& AuthenticationType::name() const {
  return name_;
}

short AuthenticationType::ordinal() const {
  return ordinal_;
}

const std::string& AuthenticationType::to_string() const {
  return display_name_;
}

const std::set<AuthenticationType>& AuthenticationType::get_constants() {
  if (constants_.empty()) {
    constants_.insert(USERNAME_PASSWORD);
    constants_.insert(PUBLIC_KEY);
  }

  return constants_;
}

AuthenticationType::AuthenticationType(const std::string &name, int ordinal, const std::string &display_name)
  : name_(name)
  , ordinal_(ordinal)
  , display_name_(display_name) {}

std::set<AuthenticationType>::iterator AuthenticationType::end() {
  return get_constants().end();
}

std::set<AuthenticationType>::iterator AuthenticationType::begin() {
  return get_constants().begin();
}

bool AuthenticationType::operator<(const AuthenticationType& object) const {
  return ordinal_ < object.ordinal_;
}

bool AuthenticationType::operator==(const AuthenticationType& object) const {
  if (name_ == object.name_) {
    if (ordinal_ == object.ordinal_) {
      if (display_name_ == object.display_name_) {
        return true;
      }
    }
  }

  return false;
}

bool AuthenticationType::operator==(const std::string& object) const {
  std::string lhs = name_;
  std::string rhs = object;
  std::transform(lhs.begin(), lhs.end(), lhs.begin(), ::tolower);
  std::transform(rhs.begin(), rhs.end(), rhs.begin(), ::tolower);
  return lhs.compare(rhs) == 0;
}
