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

#ifndef __TEST_SET_HPP__
#define __TEST_SET_HPP__
#include "nullable_value.hpp"
#include "test_utils.hpp"

namespace test { namespace driver {

/**
 * Set wrapped value
 */
template <typename T>
class Set
    : public Collection
    , Comparable<Set<T> > {
public:
  Set()
      : Collection(CASS_COLLECTION_TYPE_SET) {}

  Set(const std::set<T>& set)
      : Collection(CASS_COLLECTION_TYPE_SET, set.size())
      , set_(set) {
    // Create the collection
    for (typename std::set<T>::const_iterator iterator = set.begin(); iterator != set.end();
         ++iterator) {
      T value = *iterator;
      Collection::append<T>(value);
      set_.insert(value);
      primary_sub_type_ = value.value_type();
      secondary_sub_type_ = primary_sub_type_;
    }
  }

  Set(const std::vector<T>& set)
      : Collection(CASS_COLLECTION_TYPE_SET, set.size()) {
    // Create the collection
    for (typename std::vector<T>::const_iterator iterator = set.begin(); iterator < set.end();
         ++iterator) {
      T value = *iterator;
      Collection::append<T>(value);
      set_.insert(value);
      primary_sub_type_ = value.value_type();
      secondary_sub_type_ = primary_sub_type_;
    }
  }

  Set(const CassValue* value)
      : Collection(CASS_COLLECTION_TYPE_SET) {
    initialize(value);
  }

  void append(Collection collection) { Collection::append(collection); }

  std::string cql_type() const {
    std::string cql_type = "set<" + (*set_.begin()).cql_type() + ">";
    return cql_type;
  }

  std::string cql_value() const { return str(); }

  bool is_null() const { return Collection::is_null_; }

  /**
   * Comparison operation for driver value set. This comparison is performed in
   * lexicographical order.
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const std::set<T>& rhs) const {
    // Ensure they are the same size
    if (set_.size() < rhs.size()) return -1;
    if (set_.size() > rhs.size()) return 1;

    // Iterate and compare (sets are already sorted)
    typename std::set<T>::const_iterator rhs_iterator = rhs.begin();
    for (typename std::set<T>::const_iterator iterator = set_.begin(); iterator != set_.end();
         ++iterator) {
      int comparison = (*iterator).compare(*rhs_iterator);
      if (comparison != 0) return comparison;
      ++rhs_iterator;
    }
    return 0;
  }

  /**
   * Comparison operation for driver value set. This comparison is performed in
   * lexicographical order.
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const Set& rhs) const { return compare(rhs.set_); }

  void set(Tuple tuple, size_t index) { Collection::set(tuple, index); }

  void set(UserType user_type, const std::string& name) { Collection::set(user_type, name); }

  /**
   * Get the size of the set
   *
   * @return The number of values in the set
   */
  size_t size() const { return set_.size(); }

  void statement_bind(Statement statement, size_t index) {
    if (is_null()) {
      ASSERT_EQ(CASS_OK, cass_statement_bind_null(statement.get(), index));
    } else {
      ASSERT_EQ(CASS_OK, cass_statement_bind_collection(statement.get(), index, get()));
    }
  }

  void statement_bind(Statement statement, const std::string& name) {
    if (is_null()) {
      ASSERT_EQ(CASS_OK, cass_statement_bind_null_by_name(statement.get(), name.c_str()));
    } else {
      ASSERT_EQ(CASS_OK,
                cass_statement_bind_collection_by_name(statement.get(), name.c_str(), get()));
    }
  }

  std::string str() const {
    if (is_null()) {
      return "null";
    } else if (set_.empty()) {
      return "{}";
    } else {
      std::stringstream set_string;
      set_string << "{";
      for (typename std::set<T>::const_iterator iterator = set_.begin(); iterator != set_.end();
           ++iterator) {
        set_string << (*iterator).cql_value();

        // Add a comma separation to the set (unless the last element)
        if (iterator != --set_.end()) {
          set_string << ", ";
        }
      }
      set_string << "}";
      return set_string.str();
    }
  }

  std::set<T> value() const { return set_; }

  CassCollectionType collection_type() const { return collection_type_; }

  CassValueType value_type() const { return primary_sub_type_; }

private:
  /**
   * Values used in the set
   */
  std::set<T> set_;

  void initialize(const CassValue* value) {
    // Call the parent class
    Collection::initialize(value);

    // Add the values to the set
    if (!is_null_) {
      const CassValue* current_value = next();
      while (current_value) {
        set_.insert(T(current_value));
        current_value = next();
      }
    }
  }
};

template <class T>
inline std::ostream& operator<<(std::ostream& os, const Set<T>& set) {
  os << set.cql_value();
  return os;
}

}} // namespace test::driver

#endif // __TEST_SET_HPP__
