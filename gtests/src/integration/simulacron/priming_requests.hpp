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

#ifndef __PRIMING_REQUEST_HPP__
#define __PRIMING_REQUEST_HPP__

#include "exception.hpp"
#include "results.hpp"
#include "shared_ptr.hpp"

#include "cassandra.h"

#include <set>

namespace prime {

/**
 * Priming request
 */
class Request {
public:
  class Exception : public test::Exception {
  public:
    Exception(const std::string& message)
        : test::Exception(message) {}
  };

  Request()
      : then_(new Success()){};

  /**
   * Generate the JSON for the priming request
   *
   * @return JSON representing the priming request
   */
  std::string json() {
    rapidjson::StringBuffer buffer;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
    writer.SetIndent(' ', 2);

    writer.StartObject();
    when_.build(writer);
    writer.Key("then");
    writer.StartObject();
    then_->build(writer);
    writer.EndObject();

    // Finalize the object and return the JSON string
    writer.EndObject();
    return buffer.GetString();
  }

  /**
   * Add a consistency level that is valid for the request
   *
   * @param consistency Consistency level
   * @return PrimingRequest instance
   */
  Request& with_consistencies(const CassConsistency consistency) {
    when_.with_consistency(consistency);
    return *this;
  }

  /**
   * Set the consistency levels that are allowed for the request
   *
   * @param consistency Consistency levels for the request
   * @return PrimingRequest instance
   */
  Request& with_consistencies(const std::vector<CassConsistency>& consistency) {
    when_.with_consistencies(consistency);
    return *this;
  }

  /**
   * Set the query for the request (regex patterns allowed)
   *
   * @param query Query for the request
   * @return PrimingRequest instance
   */
  Request& with_query(const std::string& query) {
    when_.with_query(query);
    return *this;
  }

  /**
   * Set a response for the request
   *
   * @param result Response to the request
   * @return PrimingRequest instance
   */
  Request& with_result(Result* result) {
    then_ = result;
    return *this;
  }

private:
  class When {
    friend class Request;

  private:
    /**
     * Consistency levels
     */
    std::vector<CassConsistency> consistencies_;
    /**
     * Query
     */
    std::string query_;

    When(){};

    /**
     * Build the when section for the priming request
     *
     * @param writer JSON writer to add the column types to
     */
    void build(rapidjson::PrettyWriter<rapidjson::StringBuffer>& writer) {
      writer.Key("when");
      writer.StartObject();

      if (!consistencies_.empty()) {
        writer.Key("consistency");
        writer.StartArray();
        for (std::vector<CassConsistency>::iterator iterator = consistencies_.begin();
             iterator != consistencies_.end(); ++iterator) {
          writer.String(cass_consistency_string(*iterator));
        }
        writer.EndArray();
      }

      writer.Key("query");
      writer.String(query_.c_str());

      writer.EndObject();
    }

    /**
     * Add a consistency level that is valid for the request
     *
     * @param consistency Consistency level
     */
    void with_consistency(const CassConsistency consistency) {
      consistencies_.push_back(consistency);
    }

    /**
     * Set the consistency levels that are valid for the request
     *
     * @param consistency Consistency levels for the request
     */
    void with_consistencies(const std::vector<CassConsistency>& consistency) {
      consistencies_ = consistency;
    }

    /**
     * Set the query for the request (regex patterns allowed)
     *
     * @param query Query for the request
     */
    void with_query(const std::string& query) { query_ = query; }
  };

private:
  /**
   * When portion of the priming request
   */
  When when_;
  /**
   * Then portion of the priming request
   */
  SharedPtr<Result> then_;
};

} // namespace prime

#endif //__PRIMING_REQUEST_HPP__
