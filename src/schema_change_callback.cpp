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

#include "schema_change_callback.hpp"

#include "address.hpp"
#include "connection.hpp"
#include "control_connection.hpp"
#include "error_response.hpp"
#include "get_time.hpp"
#include "logger.hpp"
#include "result_iterator.hpp"
#include "result_response.hpp"
#include "string_ref.hpp"

#include <iomanip>
#include <sstream>

#define MAX_SCHEMA_AGREEMENT_WAIT_MS 10000
#define RETRY_SCHEMA_AGREEMENT_WAIT_MS 200

namespace cass {

SchemaChangeCallback::SchemaChangeCallback(Connection* connection,
                                           const SpeculativeExecution::Ptr& speculative_execution,
                                           const Response::Ptr& response,
                                           uint64_t elapsed)
  : MultipleRequestCallback(connection)
  , speculative_execution_(speculative_execution)
  , request_response_(response)
  , start_ms_(get_time_since_epoch_ms())
  , elapsed_ms_(elapsed) {}

void SchemaChangeCallback::execute() {
  execute_query("local", "SELECT schema_version FROM system.local WHERE key='local'");
  execute_query("peers", "SELECT peer, rpc_address, schema_version FROM system.peers");
}

bool SchemaChangeCallback::has_schema_agreement(const ResponseMap& responses) {
  StringRef current_version;

  ResultResponse* local_result;

  if (MultipleRequestCallback::get_result_response(responses, "local", &local_result) &&
      local_result->row_count() > 0) {
    const Row* row = &local_result->first_row();

    const Value* v = row->get_by_name("schema_version");
    if (!v->is_null()) {
      current_version = StringRef(v->data(), v->size());
    }
  } else {
    LOG_DEBUG("No row found in %s's local system table",
              connection()->address_string().c_str());
  }

  ResultResponse* peers_result;
  if (MultipleRequestCallback::get_result_response(responses, "peers", &peers_result)) {
    ResultIterator rows(peers_result);
    while (rows.next()) {
      const Row* row = rows.row();

      Address address;
      bool is_valid_address
          = ControlConnection::determine_address_for_peer_host(connection()->address(),
                                                               row->get_by_name("peer"),
                                                               row->get_by_name("rpc_address"),
                                                               &address);

      if (is_valid_address && speculative_execution_->is_host_up(address)) {
        const Value* v = row->get_by_name("schema_version");
        if (!row->get_by_name("rpc_address")->is_null() && !v->is_null()) {
          StringRef version(v->to_string_ref());
          if (version != current_version) {
            return false;
          }
        }
      }
    }
  }

  return true;
}

void SchemaChangeCallback::on_set(const ResponseMap& responses) {
  // Don't wait for schema agreement if the underlying request is cancelled
  if (speculative_execution_->state() == RequestCallback::REQUEST_STATE_CANCELLED) {
    return;
  }

  elapsed_ms_ += get_time_since_epoch_ms() - start_ms_;

  bool has_error = false;
  for (MultipleRequestCallback::ResponseMap::const_iterator it = responses.begin(),
       end = responses.end(); it != end; ++it) {
    if (check_error_or_invalid_response("SchemaChangeCallback", CQL_OPCODE_RESULT, it->second.get())) {
      has_error = true;
    }
  }

  if (!has_error && has_schema_agreement(responses)) {
    LOG_DEBUG("Found schema agreement in %llu ms",
              static_cast<unsigned long long>(elapsed_ms_));
    speculative_execution_->set_response(request_response_);
    return;
  } else if (elapsed_ms_ >= MAX_SCHEMA_AGREEMENT_WAIT_MS) {
    LOG_WARN("No schema agreement on live nodes after %llu ms. "
             "Schema may not be up-to-date on some nodes.",
             static_cast<unsigned long long>(elapsed_ms_));
    speculative_execution_->set_response(request_response_);
    return;
  }

  LOG_DEBUG("Schema still not up-to-date on some live nodes. "
            "Trying again in %d ms", RETRY_SCHEMA_AGREEMENT_WAIT_MS);

  // Try again
  Ptr callback(
        new SchemaChangeCallback(connection(),
                                 speculative_execution_,
                                 request_response_,
                                 elapsed_ms_));
  connection()->schedule_schema_agreement(callback,
                                          RETRY_SCHEMA_AGREEMENT_WAIT_MS);
}

void SchemaChangeCallback::on_error(CassError code, const std::string& message) {
  std::ostringstream ss;
  ss << "An error occurred waiting for schema agreement: '" << message
     << "' (0x" << std::hex << std::uppercase << std::setw(8) << std::setfill('0') << code << ")";
  LOG_ERROR("%s", ss.str().c_str());
  speculative_execution_->set_response(request_response_);
}

void SchemaChangeCallback::on_timeout() {
  LOG_ERROR("A timeout occurred waiting for schema agreement");
  speculative_execution_->set_response(request_response_);
}

void SchemaChangeCallback::on_closing() {
  LOG_WARN("Connection closed while waiting for schema agreement");
  speculative_execution_->set_response(request_response_);
}

} // namespace cass
