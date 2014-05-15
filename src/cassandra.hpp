/*
  Copyright (c) 2014 DataStax

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

#ifndef __CASS_CASSANDRA_HPP_INCLUDED__
#define __CASS_CASSANDRA_HPP_INCLUDED__

#include "cassandra.h"
#include "session.hpp"
#include "statement.hpp"
#include "future.hpp"
#include "prepared.hpp"
#include "batch.hpp"
#include "result.hpp"
#include "row.hpp"
#include "value.hpp"

// This abstraction allows us to separate internal types from the
// external opaque pointers that we expose.
template<typename In, typename Ex>
struct External : public In {
  inline In* from() { return static_cast<In*>(this); }
  inline static Ex* to(In* in) { return static_cast<Ex*>(in); }
  inline static const Ex* to(const In* in) { return static_cast<const Ex*>(in); }
};

extern "C" {

struct cass_session_s : public External<cass::Session, cass_session_t> { };
struct cass_statement_s : public External<cass::Statement, cass_statement_t> { };
struct cass_future_s : public External<cass::Future, cass_future_t> { };
struct cass_prepared_s : public External<cass::Prepared, cass_prepared_t> { };
struct cass_batch_s : public External<cass::Batch, cass_batch_t> { };
struct cass_result_s : public External<cass::Result, cass_result_t> { };
struct cass_collection_s : public External<cass::Collection, cass_collection_t> { };
struct cass_iterator_s : public External<cass::Iterator, cass_iterator_t> { };
struct cass_row_s : public External<cass::Row, cass_row_t> { };
struct cass_value_s : public External<cass::Value, cass_value_t> { };

}

#endif
