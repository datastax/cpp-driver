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

#ifndef __RESULT_RESULT_HPP__
#define __RESULT_RESULT_HPP__

#include <string>

#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

namespace prime {

/**
 * Priming result for request responses
 */
class Result {
public:
  virtual ~Result() {}

  /**
   * Generate the JSON for the base result
   *
   * @return  writer JSON writer to add the base result properties to
   */
  virtual void build(rapidjson::PrettyWriter<rapidjson::StringBuffer>& writer) const {
    writer.Key("result");
    writer.String(result_.c_str());
    writer.Key("delay_in_ms");
    writer.Uint64(delay_in_ms_);
  }

protected:
  /**
   * Delay in milliseconds before forwarding result
   */
  unsigned long delay_in_ms_;

  /**
   * Protected constructor for base result type
   *
   * @param result String value for the JSON result value
   */
  Result(const std::string& result)
      : delay_in_ms_(0)
      , result_(result) {}

  /**
   * Protected constructor for base result type
   *
   * @param result String value for the JSON result value
   * @param delay_in_ms Delay in milliseconds before forwarding result
   */
  Result(const std::string& result, unsigned long delay_in_ms)
      : delay_in_ms_(delay_in_ms)
      , result_(result) {}

private:
  /**
   * JSON result property value
   */
  std::string result_;
};

} // namespace prime

#endif //__RESULT_RESULT_HPP__
