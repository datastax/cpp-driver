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

#include "test_category.hpp"

#include <algorithm>
#include <climits>

// Constant value definitions for test type
const TestCategory TestCategory::CASSANDRA("CASSANDRA", 0, "Cassandra", "*_Cassandra_*");
const TestCategory TestCategory::DSE("DSE", 1, "DataStax Enterprise", "*_DSE_*");

// Static declarations for test type
std::set<TestCategory> TestCategory::constants_;

std::ostream& operator<<(std::ostream& os, const TestCategory& object) {
  os << object.display_name();
  return os;
}

TestCategory::TestCategory()
    : name_("INVALID")
    , ordinal_(-1)
    , display_name_("Invalid test category")
    , filter_("*") {}

TestCategory::TestCategory(const std::string& name) { *this = name; }

const std::string& TestCategory::name() const { return name_; }

short TestCategory::ordinal() const { return ordinal_; }

const std::string& TestCategory::display_name() const { return display_name_; }

const std::string& TestCategory::filter() const { return filter_; }

void TestCategory::operator=(const TestCategory& object) {
  name_ = object.name_;
  ordinal_ = object.ordinal_;
  display_name_ = object.display_name_;
}

void TestCategory::operator=(const std::string& name) { *this = get_enumeration(name); }

bool TestCategory::operator<(const TestCategory& object) const {
  return ordinal_ < object.ordinal_;
}

bool TestCategory::operator==(const TestCategory& object) const {
  if (name_ == object.name_) {
    if (ordinal_ == object.ordinal_) {
      if (display_name_ == object.display_name_) {
        return true;
      }
    }
  }
  return false;
}

bool TestCategory::operator==(const std::string& object) const {
  std::string lhs = name_;
  std::string rhs = object;
  std::transform(lhs.begin(), lhs.end(), lhs.begin(), ::tolower);
  std::transform(rhs.begin(), rhs.end(), rhs.begin(), ::tolower);
  return lhs.compare(rhs) == 0;
}

bool TestCategory::operator!=(const TestCategory& object) const { return !(*this == object); }

bool TestCategory::operator!=(const std::string& object) const { return !(*this == object); }

TestCategory::iterator TestCategory::begin() { return get_constants().begin(); }

TestCategory::iterator TestCategory::end() { return get_constants().end(); }

TestCategory::TestCategory(const std::string& name, short ordinal, const std::string& display_name,
                           const std::string& filter)
    : name_(name)
    , ordinal_(ordinal)
    , display_name_(display_name)
    , filter_(filter) {}

const std::set<TestCategory>& TestCategory::get_constants() {
  if (constants_.empty()) {
    constants_.insert(CASSANDRA);
    constants_.insert(DSE);
  }

  return constants_;
}

TestCategory TestCategory::get_enumeration(const std::string& name) const {
  // Iterator over the constants and find the corresponding enumeration
  if (!name.empty()) {
    for (iterator iterator = begin(); iterator != end(); ++iterator) {
      if (*iterator == name) {
        return *iterator;
      }
    }
  }

  // Enumeration was not found; throw exception if not
  throw Exception(name + " is not a valid test category");
}
