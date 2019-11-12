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

#ifndef __TEST_LIST_HPP__
#define __TEST_LIST_HPP__
#include "nullable_value.hpp"
#include "test_utils.hpp"

#include <algorithm>

namespace test { namespace driver {

/**
 * List wrapped value
 */
template <typename T>
class List
    : public Collection
    , Comparable<List<T> > {
public:
  List()
      : Collection(CASS_COLLECTION_TYPE_LIST) {}

  List(const std::vector<T>& list)
      : Collection(CASS_COLLECTION_TYPE_LIST, list.size())
      , list_(list) {
    // Create the collection
    for (typename std::vector<T>::const_iterator iterator = list.begin(); iterator != list.end();
         ++iterator) {
      T value = *iterator;
      Collection::append<T>(value);
      primary_sub_type_ = value.value_type();
      secondary_sub_type_ = primary_sub_type_;
    }
  }

  List(const CassValue* value)
      : Collection(CASS_COLLECTION_TYPE_LIST) {
    initialize(value);
  }

  void append(Collection collection) { Collection::append(collection); }

  std::string cql_type() const {
    std::string cql_type = "list<" + list_[0].cql_type() + ">";
    return cql_type;
  }

  std::string cql_value() const { return str(); }

  /**
   * Comparison operation for driver value list. This comparison is performed in
   * lexicographical order.
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const std::vector<T>& rhs) const {
    // Ensure they are the same size
    if (list_.size() < rhs.size()) return -1;
    if (list_.size() > rhs.size()) return 1;

    // Sort the values for lexicographical comparison
    std::vector<T> lhs_sorted(list_);
    std::vector<T> rhs_sorted(rhs);
    std::sort(lhs_sorted.begin(), lhs_sorted.end());
    std::sort(rhs_sorted.begin(), rhs_sorted.end());

    // Iterate and compare
    for (size_t i = 0; i < lhs_sorted.size(); ++i) {
      int comparison = lhs_sorted[i].compare(rhs_sorted[i]);
      if (comparison != 0) return comparison;
    }
    return 0;
  }

  /**
   * Comparison operation for driver value list. This comparison is performed in
   * lexicographical order.
   *
   * @param rhs Right hand side to compare
   * @return -1 if LHS < RHS, 1 if LHS > RHS, and 0 if equal
   */
  int compare(const List& rhs) const { return compare(rhs.list_); }

  bool is_null() const { return Collection::is_null_; }

  void set(Tuple tuple, size_t index) { Collection::set(tuple, index); }

  void set(UserType user_type, const std::string& name) { Collection::set(user_type, name); }

  /**
   * Get the size of the list
   *
   * @return The number of values in the list
   */
  size_t size() const { return list_.size(); }

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
    } else if (list_.empty()) {
      return "[]";
    } else {
      std::stringstream list_string;
      list_string << "[";
      for (typename std::vector<T>::const_iterator iterator = list_.begin();
           iterator != list_.end(); ++iterator) {
        list_string << (*iterator).cql_value();

        // Add a comma separation to the list (unless the last element)
        if ((iterator + 1) != list_.end()) {
          list_string << ", ";
        }
      }
      list_string << "]";
      return list_string.str();
    }
  }

  std::vector<T> value() const { return list_; }

  CassCollectionType collection_type() const { return collection_type_; }

  CassValueType value_type() const { return primary_sub_type_; }

private:
  /**
   * Values used in the list
   */
  std::vector<T> list_;

  void initialize(const CassValue* value) {
    // Call the parent class
    Collection::initialize(value);

    // Add the values to the list
    if (!is_null_) {
      const CassValue* current_value = next();
      while (current_value) {
        list_.push_back(T(current_value));
        current_value = next();
      }
    }
  }
};

template <class T>
inline std::ostream& operator<<(std::ostream& os, const List<T>& list) {
  os << list.cql_value();
  return os;
}

}} // namespace test::driver

#endif // __TEST_LIST_HPP__
