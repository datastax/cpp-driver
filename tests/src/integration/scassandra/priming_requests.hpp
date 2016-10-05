/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __PRIMING_REQUEST_HPP__
#define __PRIMING_REQUEST_HPP__

#include "exception.hpp"
#include "priming_result.hpp"
#include "priming_rows.hpp"

#include "cassandra.h"

#include <set>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

/**
 * Priming request
 */
class PrimingRequest {
public:
  class Exception : public test::Exception {
  public:
    Exception(const std::string& message)
      : test::Exception(message) {}
  };

  /**
   * Builder instantiation of the PrimingRequest object
   *
   * @return PrimingRequest instance
   */
  static PrimingRequest builder() {
    return PrimingRequest();
  }

  /**
   * Generate the JSON for the priming request
   *
   * @return JSON representing the priming request
   */
  std::string json() {
    // Initialize the rapid JSON writer
    rapidjson::StringBuffer buffer;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> json(buffer);

    // Build the JSON object (priming request)
    json.StartObject();
    when_.build(&json);
    then_.build(&json);
    json.EndObject();
    return buffer.GetString();
  }

  /**
   * Set a fixed delay to the response time of a request
   *
   * @param fixed_delay Delay in milliseconds before responding to the request
   * @return PrimingRequest instance
   */
  PrimingRequest& with_fixed_delay(const unsigned long fixed_delay_ms) {
    then_.with_fixed_delay(fixed_delay_ms);
    return *this;
  }

  /**
   * Set a response for the request
   *
   * @param result Response to the request
   * @return PrimingRequest instance
   */
  PrimingRequest& with_result(const PrimingResult& result) {
    then_.with_result(result);
    return *this;
  }

  /**
   * Set the rows to the return in the response of the request
   *
   * @param rows Rows to return when responding to the request
   * @return PrimingRequest instance
   */
  PrimingRequest& with_rows(const PrimingRows& rows) {
    then_.with_rows(rows);
    return *this;
  }

  /**
   * Add a consistency level that is valid for the request
   *
   * @param consistency Consistency level
   * @return PrimingRequest instance
   */
  PrimingRequest& with_consistency(const CassConsistency consistency) {
    when_.with_consistency(consistency);
    return *this;
  }

  /**
   * Set the consistency levels that are valid for the request
   *
   * @param consistency Consistency levels for the request
   * @return PrimingRequest instance
   */
  PrimingRequest& with_consistency(
    const std::vector<CassConsistency>& consistency) {
    when_.with_consistency(consistency);
    return *this;
  }

  /**
   * Set the keyspace for the request
   *
   * @param keyspace Name of the keyspace
   * @return PrimingRequest instance
   */
  PrimingRequest& with_keyspace(const std::string& keyspace) {
    when_.with_keyspace(keyspace);
    return *this;
  }

  /**
   * Set the query for the request
   *
   * @param query Query for the request
   * @return PrimingRequest instance
   */
  PrimingRequest& with_query(const std::string& query) {
    when_.with_query(query);
    return *this;
  }

  /**
   * Set the query pattern (regex) for the request
   *
   * @param query_pattern Query pattern for the request
   * @return PrimingRequest instance
   */
  PrimingRequest& with_query_pattern(const std::string& query_pattern) {
    when_.with_query_pattern(query_pattern);
    return *this;
   }

  /**
   * Set the table for the request
   *
   * @param table Name of the table
   * @return PrimingRequest instance
   */
  PrimingRequest& with_table(const std::string& table) {
    when_.with_table(table);
    return *this;
  }

private:
  class Then {
  friend class PrimingRequest;
  private:
    /**
     * Fixed delay (in milliseconds)
     */
    unsigned long fixed_delay_ms_;
    /**
     * Result
     */
    PrimingResult result_;
    /**
     * Rows
     */
    PrimingRows rows_;

    Then()
      : fixed_delay_ms_(0)
      , result_(PrimingResult::SUCCESS) {};

    /**
     * Build the then section for the priming request
     *
     * @param writer JSON writer to add the column types to
     */
    void build(rapidjson::PrettyWriter<rapidjson::StringBuffer>* writer) {
      // Initialize the then JSON object
      writer->Key("then");
      writer->StartObject();


      // Determine if the fixed delay JSON object should be added
      if (fixed_delay_ms_ > 0) {
        writer->Key("fixedDelay");
        writer->Uint64(fixed_delay_ms_);
      }

      // Add the result JSON object
      writer->Key("result");
      writer->String(result_.json_value().c_str());

      // Determine if the rows and column types JSON objects should be added
      if (!rows_.empty()) {
        rows_.build_rows(writer);
        rows_.build_column_types(writer);
      }

      // Finalize the then JSON object
      writer->EndObject();
    }

    /**
     * Set a fixed delay to the response time of a request
     *
     * @param fixed_delay Delay in milliseconds before responding to the request
     */
    void with_fixed_delay(const unsigned long fixed_delay) {
      fixed_delay_ms_ = fixed_delay;
    }

    /**
     * Set a response for the request
     *
     * @param result Response to the request
     */
    void with_result(const PrimingResult& result) {
      result_ = result;
    }

    /**
     * Set the rows to the return in the response of the request
     *
     * @param rows Rows to return when responding to the request
     */
    void with_rows(const PrimingRows& rows) {
      rows_ = rows;
    }
  };

  class When {
  friend class PrimingRequest;
  private:
    /**
     * Consistency levels
     */
    std::vector<CassConsistency> consistency_;
    /**
     * Keyspace name
     */
    std::string keyspace_;
    /**
     * Query
     */
    std::string query_;
    /**
     * Query pattern
     */
    std::string query_pattern_;
    /**
     * Table name
     */
    std::string table_;

    When() {};

    /**
     * Build the when section for the priming request
     *
     * @param writer JSON writer to add the column types to
     * @throws PrimingRequest::Exception If query and query pattern are present
     */
    void build(rapidjson::PrettyWriter<rapidjson::StringBuffer>* writer) {
      // Determine if an exception should be thrown
      if (!query_.empty() && !query_pattern_.empty()) {
        throw PrimingRequest::Exception("Unable to Build WHEN: Query and query "
          "pattern can not be used at the same time");
      }

      // Initialize the when JSON object
      writer->Key("when");
      writer->StartObject();

      // Determine if the consistency JSON object should be added
      if (!consistency_.empty()) {
        writer->Key("consistency");
        writer->StartArray();
        for (std::vector<CassConsistency>::iterator iterator = consistency_.begin();
          iterator != consistency_.end(); ++iterator) {
          writer->String(get_cql_consistency(*iterator).c_str());
        }
        writer->EndArray();
      }

      // Determine if the keyspace JSON object should be added
      if (!keyspace_.empty()) {
        writer->Key("keyspace");
        writer->String(keyspace_.c_str());
      }

      // Determine if the query JSON object should be added
      if (!query_.empty()) {
        writer->Key("query");
        writer->String(query_.c_str());
      }

      // Determine if the query pattern JSON object should be added
      if (!query_pattern_.empty()) {
        writer->Key("queryPattern");
        writer->String(query_pattern_.c_str());
      }

      // Determine if the table JSON object should be added
      if (!table_.empty()) {
        writer->Key("table");
        writer->String(table_.c_str());
      }

      // Finalize the when JSON object
      writer->EndObject();
    }

    /**
     * Add a consistency level that is valid for the request
     *
     * @param consistency Consistency level
     */
    void with_consistency(const CassConsistency consistency) {
      consistency_.push_back(consistency);
    }

    /**
     * Set the consistency levels that are valid for the request
     *
     * @param consistency Consistency levels for the request
     */
    void with_consistency(
      const std::vector<CassConsistency>& consistency) {
      consistency_ = consistency;
    }

    /**
     * Set the keyspace for the request
     *
     * @param keyspace Name of the keyspace
     */
    void with_keyspace(const std::string& keyspace) {
      keyspace_ = keyspace;
    }

    /**
     * Set the query for the request
     *
     * @param query Query for the request
     */
    void with_query(const std::string& query) {
      query_ = query;
    }

    /**
     * Set the query pattern (regex) for the request
     *
     * @param query_pattern Query pattern for the request
     */
    void with_query_pattern(const std::string& query_pattern) {
      query_pattern_ = query_pattern;
    }

    /**
     * Set the table for the request
     *
     * @param table Name of the table
     */
    void with_table(const std::string& table) {
      table_ = table;
    }

    /**
     * Convert the driver consistency level to a value for the priming request
     *
     * @param consistency Consistency level
     * @return String representation of the consistency level
     */
    std::string get_cql_consistency(CassConsistency consistency) const {
      switch(consistency) {
        case CASS_CONSISTENCY_ANY:
          return "ANY";
        case CASS_CONSISTENCY_ONE:
          return "ONE";
        case CASS_CONSISTENCY_TWO:
          return "TWO";
        case CASS_CONSISTENCY_THREE:
          return "THREE";
        case CASS_CONSISTENCY_QUORUM:
          return "QUORUM";
        case CASS_CONSISTENCY_ALL:
          return "ALL";
        case CASS_CONSISTENCY_LOCAL_QUORUM:
          return "LOCAL_QUORUM";
        case CASS_CONSISTENCY_EACH_QUORUM:
          return "EACH_QUORUM";
        case CASS_CONSISTENCY_SERIAL:
          return "SERIAL";
        case CASS_CONSISTENCY_LOCAL_SERIAL:
          return "LOCAL_SERIAL";
        case CASS_CONSISTENCY_LOCAL_ONE:
          return "LOCAL_ONE";
        case CASS_CONSISTENCY_UNKNOWN:
        default:
          throw Exception("Invalid Consistency: UNKNOWN");
      }
    }
  };

  /**
   * Then portion of the priming request
   */
  Then then_;
  /**
   * When portion of the priming request
   */
  When when_;

  /**
   * Disallow constructor for builder pattern
   */
  PrimingRequest() {};
};

#endif //__PRIMING_REQUEST_HPP__