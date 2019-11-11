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

#ifndef __TEST_CATEGORY_HPP__
#define __TEST_CATEGORY_HPP__
#include "exception.hpp"

#include <ostream>
#include <set>
#include <string>

/**
 * Test category enumeration
 */
class TestCategory {
public:
  class Exception : public test::Exception {
  public:
    Exception(const std::string& message)
        : test::Exception(message) {}
  };
  /**
   * Iterator
   */
  typedef std::set<TestCategory>::iterator iterator;
  /**
   * Cassandra category
   */
  static const TestCategory CASSANDRA;
  /**
   * DataStax Enterprise category
   */
  static const TestCategory DSE;

  /**
   * Default constructor to handle issues with static initialization of
   * constant enumerations
   */
  TestCategory();
  /**
   * Construct the enumeration from the given name
   *
   * @param name Enumeration name
   */
  TestCategory(const std::string& name);
  /**
   * Name of constant
   *
   * @return Name of constant
   */
  const std::string& name() const;
  /**
   * Ordinal of constant
   *
   * @return Ordinal of constant
   */
  short ordinal() const;
  /**
   * Get the display name
   *
   * @return Display name of the enumeration
   */
  const std::string& display_name() const;
  /**
   * Get the filter associated with the enumeration
   *
   * @return Filter for the enumeration
   */
  const std::string& filter() const;
  /**
   * First item in the enumeration constants
   *
   * @return Iterator pointing to the first element in the set
   */
  static iterator begin();
  /**
   * Last item in the enumeration constants
   *
   * @return Iterator pointing to the last element in the set
   */
  static iterator end();

  /**
   * Assign
   *
   * @param object Right hand side comparison object
   */
  void operator=(const TestCategory& object);
  /**
   * Assign
   *
   * @param object Right hand side comparison object
   */
  void operator=(const std::string& object);
  /**
   * Less than (can be used for sorting)
   *
   * @param object Right hand side comparison object
   * @return True if LHS < RHS; false otherwise
   */
  bool operator<(const TestCategory& object) const;
  /**
   * Equal to
   *
   * @param object Right hand side comparison object
   * @return True if LHS == RHS; false otherwise
   */
  bool operator==(const TestCategory& object) const;
  /**
   * Equal to (case-incentive string comparison)
   *
   * @param object Right hand side comparison object
   * @return True if LHS == RHS; false otherwise
   */
  bool operator==(const std::string& object) const;
  /**
   * Not equal to
   *
   * @param object Right hand side comparison object
   * @return True if LHS != RHS; false otherwise
   */
  bool operator!=(const TestCategory& object) const;
  /**
   * Not equal to (case-incentive string comparison)
   *
   * @param object Right hand side comparison object
   * @return True if LHS != RHS; false otherwise
   */
  bool operator!=(const std::string& object) const;

  /**
   * Stream operator (out)
   *
   * @param os Output stream
   * @param object Object to write to stream
   * @return Output stream
   */
  friend std::ostream& operator<<(std::ostream& os, const TestCategory& object);

private:
  /**
   * Enumeration constants
   */
  static std::set<TestCategory> constants_;
  /**
   * Name of constant
   */
  std::string name_;
  /**
   * Ordinal of constant
   */
  short ordinal_;
  /**
   * Display name for constant
   */
  std::string display_name_;
  /**
   * Filter for constant
   */
  std::string filter_;

  /**
   * Constructor
   *
   * @param name Name for enumeration
   * @param ordinal Ordinal value for enumeration
   * @param display_name Display name for enumeration
   * @param filter Filter for enumeration
   */
  TestCategory(const std::string& name, short ordinal, const std::string& display_name,
               const std::string& filter);
  /**
   * Get the enumeration constants
   *
   * @return List of enumeration constants
   */
  static const std::set<TestCategory>& get_constants();
  /**
   * Get the enumeration based on the name
   *
   * @param name Name of the enumeration (case-insensitive)
   * @return Enumeration constant
   */
  TestCategory get_enumeration(const std::string& name) const;
};

#endif // __TEST_CATEGORY_HPP__
