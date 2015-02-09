/*
  Copyright (c) 2014-2015 DataStax

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

#include "ref_counted.hpp"
#include "result_response.hpp"
#include "scoped_ptr.hpp"

#include <string>

namespace cass {

class Prepared : public RefCounted<Prepared> {
public:
  Prepared(const ResultResponse* result,
           const std::string& statement,
           const std::vector<std::string>& key_columns);

  const ScopedPtr<const ResultResponse>& result() const { return result_; }
  const std::string& id() const { return id_; }
  const std::string& statement() const { return statement_; }
  const std::vector<size_t>& key_indices() const { return key_indices_; }

private:
  ScopedPtr<const ResultResponse> result_;
  std::string id_;
  std::string statement_;
  std::vector<size_t> key_indices_;
};

} // namespace cass

#endif
