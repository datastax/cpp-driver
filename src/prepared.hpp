/*
  Copyright 2014 DataStax

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

#ifndef __CASS_PREPARED_HPP_INCLUDED__
#define __CASS_PREPARED_HPP_INCLUDED__

#include <string>

namespace cass {

class Prepared {
public:
  Prepared(const std::string& id, const std::string& statement)
      : id_(id)
      , statement_(statement) {}

  const std::string& id() const { return id_; }
  const std::string& statement() const { return statement_; }

private:
  std::string id_;
  std::string statement_;
};

} // namespace cass

#endif
