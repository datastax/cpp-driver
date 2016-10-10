/*
  Copyright (c) 2014-2016 DataStax

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

#include "external.hpp"
#include "ref_counted.hpp"
#include "result_response.hpp"
#include "metadata.hpp"
#include "scoped_ptr.hpp"

#include <string>

namespace cass {

class Prepared : public RefCounted<Prepared> {
public:
  typedef SharedRefPtr<const Prepared> ConstPtr;

  Prepared(const ResultResponse::Ptr& result,
           const std::string& statement,
           const Metadata::SchemaSnapshot& schema_metadata);

  const ResultResponse::ConstPtr& result() const { return result_; }
  const std::string& id() const { return id_; }
  const std::string& statement() const { return statement_; }
  const ResultResponse::PKIndexVec& key_indices() const { return key_indices_; }

private:
  ResultResponse::ConstPtr result_;
  std::string id_;
  std::string statement_;
  ResultResponse::PKIndexVec key_indices_;
};

} // namespace cass

EXTERNAL_TYPE(cass::Prepared, CassPrepared)

#endif
