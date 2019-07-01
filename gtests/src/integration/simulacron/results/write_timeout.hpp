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

#ifndef __RESULT_WRITE_TIMEOUT_HPP__
#define __RESULT_WRITE_TIMEOUT_HPP__

#include "result.hpp"

#include "exception.hpp"

#include "cassandra.h"

namespace prime {

/**
 * Priming result 'write_timeout'
 */
class WriteTimeout : public Result {
public:
  WriteTimeout()
      : Result("write_timeout")
      , consistency_(CASS_CONSISTENCY_LOCAL_ONE)
      , received_responses_(0)
      , required_responses_(1)
      , write_type_(CASS_WRITE_TYPE_SIMPLE) {}

  /**
   * Fully construct the 'write_timeout' result
   *
   * @param delay_in_ms Delay in milliseconds before forwarding result
   * @param consistency Consistency level data was written at
   * @param received_responses Number of responses received from replicas
   * @param required_responses Number of responses required from replicas
   * @param write_type The type of write that resulted in write timeout
   */
  WriteTimeout(unsigned long delay_in_ms, CassConsistency consistency,
               unsigned int received_responses, unsigned int required_responses,
               CassWriteType write_type)
      : Result("write_timeout", delay_in_ms)
      , consistency_(consistency)
      , received_responses_(received_responses)
      , required_responses_(required_responses)
      , write_type_(write_type) {}

  /**
   * Set a fixed delay to the response time of a result
   *
   * @param delay_in_ms Delay in milliseconds before responding with the result
   * @return WriteTimeout instance
   */
  WriteTimeout& with_delay_in_ms(unsigned long delay_in_ms) {
    delay_in_ms_ = delay_in_ms;
    return *this;
  }

  /**
   * Set the consistency level the data was written at
   *
   * @param consistency Consistency level data was written at
   * @return WriteTimeout instance
   */
  WriteTimeout& with_consistency(CassConsistency consistency) {
    consistency_ = consistency;
    return *this;
  }

  /**
   * Set the number of responses that were received from replicas
   *
   * @param received_responses Number of responses received from replicas
   * @return WriteTimeout instance
   */
  WriteTimeout& with_received_responses(unsigned int received_responses) {
    received_responses_ = received_responses;
    return *this;
  }

  /**
   * Set the number of responses that are required from replicas
   *
   * @param required_responses Number of responses required from replicas
   * @return WriteTimeout instance
   */
  WriteTimeout& with_required_responses(unsigned int required_responses) {
    required_responses_ = required_responses;
    return *this;
  }

  /**
   * Set the number of responses that are required from replicas
   *
   * @param write_type The type of write that resulted in write timeout
   * @return WriteTimeout instance
   */
  WriteTimeout& with_write_type(CassWriteType write_type) {
    write_type_ = write_type;
    return *this;
  }

  /**
   * Generate the JSON for the 'write_timeout` result
   *
   * @return  writer JSON writer to add the 'write_timeout' result properties to
   */
  virtual void build(rapidjson::PrettyWriter<rapidjson::StringBuffer>& writer) const {
    Result::build(writer);

    writer.Key("consistency_level");
    writer.String(cass_consistency_string(consistency_));
    writer.Key("received");
    writer.Uint(received_responses_);
    writer.Key("block_for");
    writer.Uint(required_responses_);
    writer.Key("write_type");
    writer.String(cass_write_type_string(write_type_));
  }

private:
  /**
   * The consistency level the data was written at
   */
  CassConsistency consistency_;
  /**
   * Number of responses received from replicas
   */
  unsigned int received_responses_;
  /**
   * Number of responses required from replicas
   */
  unsigned int required_responses_;
  /**
   * The type of write that resulted in write timeout
   */
  CassWriteType write_type_;
};

} // namespace prime

#endif //__RESULT_WRITE_TIMEOUT_HPP__
