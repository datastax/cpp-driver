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

#include "deployment_type.hpp"

#include <algorithm>

using namespace CCM;

// Constant value definitions for deployment type
const DeploymentType DeploymentType::LOCAL("LOCAL", 0, "Local");
#ifdef CASS_USE_LIBSSH2
const DeploymentType DeploymentType::REMOTE("REMOTE", 1, "Remote");
#endif

// Static declarations for deployment type
std::set<DeploymentType> DeploymentType::constants_;

DeploymentType::DeploymentType()
 : name_("INVALID")
 , ordinal_(-1)
 , display_name_("Invalid deployment") {
}

const std::string& DeploymentType::name() const {
  return name_;
}

short DeploymentType::ordinal() const {
  return ordinal_;
}

const std::string& DeploymentType::to_string() const {
  return display_name_;
}

const std::set<DeploymentType>& DeploymentType::get_constants() {
  if (constants_.empty()) {
    constants_.insert(LOCAL);
#ifdef CASS_USE_LIBSSH2
    constants_.insert(REMOTE);
#endif
  }

  return constants_;
}

DeploymentType::DeploymentType(const std::string &name, int ordinal, const std::string &display_name)
  : name_(name)
  , ordinal_(ordinal)
  , display_name_(display_name) {}

std::set<DeploymentType>::iterator DeploymentType::end() {
  return get_constants().end();
}

std::set<DeploymentType>::iterator DeploymentType::begin() {
  return get_constants().begin();
}

bool DeploymentType::operator<(const DeploymentType& object) const {
  return ordinal_ < object.ordinal_;
}

bool DeploymentType::operator==(const DeploymentType& object) const {
  if (name_ == object.name_) {
    if (ordinal_ == object.ordinal_) {
      if (display_name_ == object.display_name_) {
        return true;
      }
    }
  }

  return false;
}

bool DeploymentType::operator==(const std::string& object) const {
  std::string lhs = name_;
  std::string rhs = object;
  std::transform(lhs.begin(), lhs.end(), lhs.begin(), ::tolower);
  std::transform(rhs.begin(), rhs.end(), rhs.begin(), ::tolower);
  return lhs.compare(rhs) == 0;
}
