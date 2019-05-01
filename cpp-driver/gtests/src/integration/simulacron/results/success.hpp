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

#ifndef __RESULT_SUCCESS_HPP__
#define __RESULT_SUCCESS_HPP__

#include "result.hpp"

#include "exception.hpp"
#include "test_utils.hpp"

#include "cassandra.h"

#include <map>
#include <sstream>
#include <vector>

namespace prime {

class Rows;
class Success;

/**
 * Priming row
 */
class Row {
public:
  class Exception : public test::Exception {
  public:
    Exception(const std::string& message)
        : test::Exception(message) {}
  };
  typedef std::pair<std::string, std::string> Column;

  Row() {}

  /**
   * Add a column|value pair
   *
   * @param name Column name
   * @param value_type Value type for the column
   * @param value Value for the column
   * @return PrimingRow instance
   */
  Row& add_column(const std::string& name, const CassValueType value_type,
                  const std::string& value) {
    std::string cql_type = test::Utils::scalar_cql_type(value_type);
    if (value_type == CASS_VALUE_TYPE_LIST || value_type == CASS_VALUE_TYPE_MAP ||
        value_type == CASS_VALUE_TYPE_SET) {
      throw Exception("Value Type " + cql_type +
                      "Needs to be Parameterized: "
                      "Use add_column(string name, string cql_value_type, string value) instead");
    }

    // TODO: Add types when supported by Simulacron
    if (value_type == CASS_VALUE_TYPE_CUSTOM || value_type == CASS_VALUE_TYPE_UDT) {
      throw Exception("Value Type is not Supported by Simulacron: " + cql_type);
    }

    return add_column(name, cql_type, value);
  }

  /**
   * Add a column|value pair
   *
   * @param name Column name
   * @param cql_value_type Value type for the column
   * @param value Value for the column
   * @return PrimingRow instance
   */
  Row& add_column(const std::string& name, const std::string& cql_value_type,
                  const std::string& value) {
    // TODO: Add validation (mainly for parameterized types)
    // Ensure the column doesn't already exist
    if (columns_.find(name) != columns_.end()) {
      throw Exception("Unable to Add Column: Already Exists [" + name + "]");
    }
    columns_.insert(Pair(name, Column(cql_value_type, value)));
    return *this;
  }

  /**
   * Equality to check whether the columns are equal; number of columns and
   * names only (values are ignored)
   *
   * @param rhs PrimingRow to compare
   * @return True if number of columns and column name are equal; false
   *         otherwise
   */
  bool operator==(const Row& rhs) const {
    return columns_.size() == rhs.columns_.size() &&
           std::equal(columns_.begin(), columns_.end(), rhs.columns_.begin(), ColumnsKeyEquality());
  }
  bool operator!=(const Row& rhs) const { return !(*this == rhs); }

  /**
   * Build the column types for the column used by the row
   *
   * @param writer JSON writer to add the column types to
   */
  void build_column_types(rapidjson::PrettyWriter<rapidjson::StringBuffer>& writer) const {
    writer.Key("column_types");
    writer.StartObject();

    for (Map::const_iterator iterator = columns_.begin(); iterator != columns_.end(); ++iterator) {
      std::string name = iterator->first;
      std::string cql_type = iterator->second.first;
      writer.Key(name.c_str());
      writer.String(cql_type.c_str());
    }

    writer.EndObject();
  }

  /**
   * Build the row based on the columns
   *
   * @param writer JSON writer to add the row to
   */
  void build_row(rapidjson::PrettyWriter<rapidjson::StringBuffer>& writer) const {
    writer.StartObject();

    for (Map::const_iterator iterator = columns_.begin(); iterator != columns_.end(); ++iterator) {
      std::string name = iterator->first;
      std::string value = iterator->second.second;

      // Add the column|value pair
      writer.Key(name.c_str());
      bool is_array = value.compare(0, 1, "[") == 0 && value.compare(value.size() - 1, 1, "]") == 0;
      if (is_array) {
        value = value.substr(1, value.size() - 2);
        writer.StartArray();
        std::vector<std::string> values = test::Utils::explode(value, ',');
        for (std::vector<std::string>::iterator iterator = values.begin(); iterator != values.end();
             ++iterator) {
          writer.String((*iterator).c_str());
        }
        writer.EndArray();
      } else {
        writer.String(value.c_str());
      }
    }

    writer.EndObject();
  }

private:
  typedef std::map<std::string, Column> Map;
  typedef std::pair<std::string, Column> Pair;

  /**
   * Columns
   */
  Map columns_;

  /**
   * Overload comparable class for pairing columns
   */
  class ColumnsKeyEquality {
  public:
    /**
     * Check to see if the column names are equal
     *
     * @param lhs Left hand side of the equality
     * @param rhs Right hand side of the equality
     * @return True if column names are equal; false otherwise
     */
    bool operator()(const Pair& lhs, const Pair& rhs) const {
      return lhs.first.compare(rhs.first) == 0;
    }
  };
};

/**
 * Priming rows
 */
class Rows {
public:
  class Exception : public test::Exception {
  public:
    Exception(const std::string& message)
        : test::Exception(message) {}
  };

  Rows() {}

  /**
   * Add a row
   *
   * @param columns Columns to add
   * @param value Value (type|value) for the column
   * @return PrimingColumns instance
   */
  Rows& add_row(const Row& columns) {
    if (!rows_.empty() && rows_.front() != columns) {
      throw Exception("Unable to Add Row: Columns are incompatible with previous row(s)");
    }
    rows_.push_back(columns);
    return *this;
  }

  /**
   * Checking if the rows are empty (not primed)
   *
   * @return True if rows are empty; false otherwise
   */
  bool empty() const { return rows_.empty(); }

  /**
   * Build the column types for the column used by the rows
   *
   * @param writer JSON writer to add the column types to
   */
  void build_column_types(rapidjson::PrettyWriter<rapidjson::StringBuffer>& writer) const {
    rows_.front().build_column_types(writer);
  }

  /**
   * Build the rows
   *
   * @param writer JSON writer to add the rows to
   */
  void build_rows(rapidjson::PrettyWriter<rapidjson::StringBuffer>& rows) const {
    rows.Key("rows");
    rows.StartArray();
    for (std::vector<Row>::const_iterator iterator = rows_.begin(); iterator != rows_.end();
         ++iterator) {
      iterator->build_row(rows);
    }
    rows.EndArray();
  }

private:
  /**
   * The primed rows
   */
  std::vector<Row> rows_;
};

/**
 * Priming result 'success'
 */
class Success : public Result {
public:
  Success()
      : Result("success") {}

  /**
   * Fully construct the 'success' result
   *
   * @param delay_in_ms Delay in milliseconds before forwarding result
   * @param rows Rows to return when responding to the request
   */
  Success(unsigned long delay_in_ms, const Rows& rows)
      : Result("success", delay_in_ms)
      , rows_(rows) {}

  /**
   * Set a fixed delay to the response time of a result
   *
   * @param delay_in_ms Delay in milliseconds before responding with the result
   * @return Success instance
   */
  Success& with_delay_in_ms(unsigned long delay_in_ms) {
    delay_in_ms_ = delay_in_ms;
    return *this;
  }

  /**
   * Set the rows to the return in the response of the request
   *
   * @param rows Rows to return when responding to the request
   * @return Success instance
   */
  Success& with_rows(const Rows& rows) {
    rows_ = rows;
    return *this;
  }

  /**
   * Generate the JSON for the base result
   *
   * @return  writer JSON writer to add the base result properties to
   */
  virtual void build(rapidjson::PrettyWriter<rapidjson::StringBuffer>& writer) const {
    // Call the parent build functionality
    Result::build(writer);

    // Determine if the rows and column types JSON objects should be added
    if (!rows_.empty()) {
      rows_.build_rows(writer);
      rows_.build_column_types(writer);
    }
  }

private:
  /**
   * Rows
   */
  Rows rows_;
};

} // namespace prime

#endif //__RESULT_SUCCESS_HPP__
