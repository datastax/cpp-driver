/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __PRIMING_ROWS_HPP__
#define __PRIMING_ROWS_HPP__

#include "exception.hpp"
#include "test_utils.hpp"

#include "cassandra.h"

#include <rapidjson/stringbuffer.h>
#include <rapidjson/prettywriter.h>

#include <map>
#include <string>
#include <sstream>
#include <vector>

/**
 * Priming row
 */
class PrimingRow {
friend class PrimingRows;
public:
  class Exception : public test::Exception {
  public:
    Exception(const std::string& message)
      : test::Exception(message) {}
  };
  typedef std::pair<std::string, std::string> Column;

  /**
   * Builder instantiation of the PrimingColumns object
   *
   * @return PrimingRow instance
   */
  static PrimingRow builder() {
    return PrimingRow();
  }

  /**
   * Add a column|value pair
   *
   * @param name Column name
   * @param value_type Value type for the column
   * @param value Value for the column
   * @return PrimingRow instance
   */
  PrimingRow& add_column(const std::string& name,
    const CassValueType value_type, const std::string& value) {
    std::string cql_type = get_cql_type(value_type);
    if (value_type == CASS_VALUE_TYPE_LIST ||
      value_type == CASS_VALUE_TYPE_MAP ||
      value_type == CASS_VALUE_TYPE_SET) {
      throw Exception("Value Type " + cql_type + "Needs to be Parameterized: "
        "Use add_column(string name, string cql_value_type, string value) instead");
     }

    //TODO: Add types when supported by SCassandra
    if (value_type == CASS_VALUE_TYPE_CUSTOM ||
      value_type == CASS_VALUE_TYPE_UDT) {
      throw Exception("Value Type is not Supported by SCassandra: "
        + cql_type);
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
  PrimingRow& add_column(const std::string& name,
    const std::string& cql_value_type, const std::string& value) {
    //TODO: Add validation (mainly for parameterized types)
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
  bool operator==(const PrimingRow& rhs) const {
    return columns_.size() == rhs.columns_.size()
      && std::equal(columns_.begin(), columns_.end(), rhs.columns_.begin(),
        ColumnsKeyEquality());
  }
  bool operator!=(const PrimingRow& rhs) const {
    return !(*this == rhs);
  }

protected:
  /**
   * Build the column types for the column used by the row
   *
   * @param writer JSON writer to add the column types to
   */
  void build_column_types(
    rapidjson::PrettyWriter<rapidjson::StringBuffer>* writer) {
    // Initialize the column types JSON object
    writer->Key("column_types");
    writer->StartObject();

    // Iterate over the columns and create the column types
    for (Map::iterator iterator = columns_.begin(); iterator != columns_.end();
      ++iterator) {
      std::string name = iterator->first;
      std::string cql_type = iterator->second.first;

      // Add the column type
      writer->Key(name.c_str());
      writer->String(cql_type.c_str());
    }

    // Finalize the column types JSON object
    writer->EndObject();
  }

  /**
   * Build the row based on the columns
   *
   * @param writer JSON writer to add the row to
   */
  void build_row(rapidjson::PrettyWriter<rapidjson::StringBuffer>* writer) {
    // Initialize the row JSON object
    writer->StartObject();

    // Iterate over the columns and create the row
    for (Map::iterator iterator = columns_.begin(); iterator != columns_.end();
      ++iterator) {
      std::string name = iterator->first;
      std::string value = iterator->second.second;

      // Add the column|value pair
      writer->Key(name.c_str());
      bool is_array = value.compare(0, 1, "[")  == 0 &&
        value.compare(value.size() - 1, 1, "]") == 0;
      if (is_array) {
        value = value.substr(1, value.size() - 2);
        writer->StartArray();
        std::vector<std::string> values = test::Utils::explode(value, ',');
        for (std::vector<std::string>::iterator iterator = values.begin();
          iterator != values.end(); ++iterator) {
          writer->String((*iterator).c_str());
        }
        writer->EndArray();
      } else {
        writer->String(value.c_str());
      }
    }

    // Finalize the row JSON object
    writer->EndObject();
  }

private:
  typedef std::map<std::string, Column> Map;
  typedef std::pair<std::string, Column> Pair;
  /**
   * Columns
   */
  Map columns_;

  /**
   * Disallow constructor for builder pattern
   */
  PrimingRow() {};

  /**
   * Get the CQL type from the driver value type.
   *
   * @param type Driver value type
   * @return String CQL representation of the type
   */
  std::string get_cql_type(CassValueType type) {
    switch(type) {
      case CASS_VALUE_TYPE_CUSTOM:
        return "custom";
      case CASS_VALUE_TYPE_ASCII:
        return "ascii";
      case CASS_VALUE_TYPE_BIGINT:
        return "bigint";
      case CASS_VALUE_TYPE_BLOB:
        return "blob";
      case CASS_VALUE_TYPE_BOOLEAN:
        return "boolean";
      case CASS_VALUE_TYPE_COUNTER:
        return "counter";
      case CASS_VALUE_TYPE_DECIMAL:
        return "decimal";
      case CASS_VALUE_TYPE_DOUBLE:
        return "double";
      case CASS_VALUE_TYPE_FLOAT:
        return "float";
      case CASS_VALUE_TYPE_INT:
        return "int";
      case CASS_VALUE_TYPE_TEXT:
        return "text";
      case CASS_VALUE_TYPE_TIMESTAMP:
        return "timestamp";
      case CASS_VALUE_TYPE_UUID:
        return "uuid";
      case CASS_VALUE_TYPE_VARCHAR:
        return "varchar";
      case CASS_VALUE_TYPE_VARINT:
        return "varint";
      case CASS_VALUE_TYPE_TIMEUUID:
        return "timeuuid";
      case CASS_VALUE_TYPE_INET:
        return "inet";
      case CASS_VALUE_TYPE_DATE:
        return "date";
      case CASS_VALUE_TYPE_TIME:
        return "time";
      case CASS_VALUE_TYPE_SMALL_INT:
        return "smallint";
      case CASS_VALUE_TYPE_TINY_INT:
        return "tinyint";
      case CASS_VALUE_TYPE_LIST:
        return "list";
      case CASS_VALUE_TYPE_MAP:
        return "map";
      case CASS_VALUE_TYPE_SET:
        return "set";
      case CASS_VALUE_TYPE_UDT:
        return "udt";
      case CASS_VALUE_TYPE_TUPLE:
        return "tuple";
      case CASS_VALUE_TYPE_UNKNOWN:
      default:
        std::stringstream message;
        message << "Unsupported Value Type: " << type
          << " will need to be added";
        throw Exception(message.str());
        break;
     }
  }

  /**
   * Overload comparable class for pairing columns
   */
  class ColumnsKeyEquality {
  public:
    /**
     * Check to see if the colum names are equal
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
class PrimingRows {
friend class PrimingRequest;
public:
  class Exception : public test::Exception {
  public:
    Exception(const std::string& message)
      : test::Exception(message) {}
  };

  /**
   * Builder instantiation of the PrimingColumns object
   *
   * @return PrimingColumns instance
   */
  static PrimingRows builder() {
    return PrimingRows();
  }

  /**
   * Add a row
   *
   * @param columns Columns to add
   * @param value Value (type|value) for the column
   * @return PrimingColumns instance
   */
  PrimingRows& add_row(PrimingRow columns) {
    // Make sure the columns can be added to the rows
    if (!rows_.empty() && rows_.front() != columns) {
      throw Exception(
        "Unable to Add Row: Columns are incompatible with previous row(s)");
    }
    rows_.push_back(columns);
    return *this;
  }

  /**
   * Checking if the rows are empty (not primed)
   *
   * @return True if rows are empty; false otherwise
   */
  bool empty() const {
    return rows_.empty();
  }

protected:
  /**
   * Build the column types for the column used by the rows
   *
   * @param writer JSON writer to add the column types to
   */
  void build_column_types(
    rapidjson::PrettyWriter<rapidjson::StringBuffer>* writer) {
    rows_.front().build_column_types(writer);
  }

  /**
   * Build the rows
   *
   * @param writer JSON writer to add the rows to
   */
  void build_rows(rapidjson::PrettyWriter<rapidjson::StringBuffer>* rows) {
    // Iterate over the rows and add each row to the rows object array
    rows->Key("rows");
    rows->StartArray();
    for (std::vector<PrimingRow>::iterator iterator = rows_.begin();
      iterator != rows_.end(); ++iterator) {
      iterator->build_row(rows);
    }
    rows->EndArray();
  }

private:
  /**
   * The primed rows
   */
  std::vector<PrimingRow> rows_;

  /**
   * Disallow constructor for builder pattern
   */
  PrimingRows() {};
};

#endif //__PRIMING_ROWS_HPP__