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

#include "dse_credentials_type.hpp"

#include <algorithm>

using namespace CCM;

// Constant value definitions for DSE credentials type
const DseCredentialsType DseCredentialsType::USERNAME_PASSWORD("USERNAME_PASSWORD", 0,
                                                               "Username and Password");
const DseCredentialsType DseCredentialsType::INI_FILE("INI_FILE", 1, "INI Credentials File");

// Static declarations for DSE credentials type
std::set<DseCredentialsType> DseCredentialsType::constants_;

DseCredentialsType::DseCredentialsType()
    : name_("INVALID")
    , ordinal_(-1)
    , display_name_("Invalid DSE credentials") {}

const std::string& DseCredentialsType::name() const { return name_; }

short DseCredentialsType::ordinal() const { return ordinal_; }

const std::string& DseCredentialsType::to_string() const { return display_name_; }

const std::set<DseCredentialsType>& DseCredentialsType::get_constants() {
  if (constants_.empty()) {
    constants_.insert(USERNAME_PASSWORD);
    constants_.insert(INI_FILE);
  }

  return constants_;
}

DseCredentialsType::DseCredentialsType(const std::string& name, int ordinal,
                                       const std::string& display_name)
    : name_(name)
    , ordinal_(ordinal)
    , display_name_(display_name) {}

std::set<DseCredentialsType>::iterator DseCredentialsType::end() { return get_constants().end(); }

std::set<DseCredentialsType>::iterator DseCredentialsType::begin() {
  return get_constants().begin();
}

bool DseCredentialsType::operator<(const DseCredentialsType& object) const {
  return ordinal_ < object.ordinal_;
}

bool DseCredentialsType::operator==(const DseCredentialsType& object) const {
  if (name_ == object.name_) {
    if (ordinal_ == object.ordinal_) {
      if (display_name_ == object.display_name_) {
        return true;
      }
    }
  }

  return false;
}

bool DseCredentialsType::operator==(const std::string& object) const {
  std::string lhs = name_;
  std::string rhs = object;
  std::transform(lhs.begin(), lhs.end(), lhs.begin(), ::tolower);
  std::transform(rhs.begin(), rhs.end(), rhs.begin(), ::tolower);
  return lhs.compare(rhs) == 0;
}
