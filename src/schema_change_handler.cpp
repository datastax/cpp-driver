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

#include "schema_change_handler.hpp"

#include "address.hpp"
#include "connection.hpp"
#include "control_connection.hpp"
#include "error_response.hpp"
#include "get_time.hpp"
#include "logger.hpp"
#include "result_iterator.hpp"
#include "result_response.hpp"

#include "third_party/boost/boost/utility/string_ref.hpp"

#include <iomanip>
#include <sstream>

#define MAX_SCHEMA_AGREEMENT_WAIT_MS 10000
#define RETRY_SCHEMA_AGREEMENT_WAIT_MS 200

namespace cass {

SchemaChangeHandler::SchemaChangeHandler(Connection* connection,
                                         RequestHandler* request_handler,
                                         Response* response,
                                         uint64_t elapsed)
  : MultipleRequestHandler(connection)
  , logger_(connection->logger())
  , request_handler_(request_handler)
  , request_response_(response)
  , start_ms_(get_time_since_epoch_ms())
  , elapsed_ms_(elapsed) {}

void SchemaChangeHandler::execute() {
  execute_query("SELECT schema_version FROM system.local WHERE key='local'");
  execute_query("SELECT peer, rpc_address, schema_version FROM system.peers");
}

bool SchemaChangeHandler::has_schema_agreement(const ResponseVec& responses) {
  boost::string_ref current_version;

  ResultResponse* local_result =
      static_cast<ResultResponse*>(responses[0]);

  if (local_result->row_count() > 0) {
    local_result->decode_first_row();

    const Row* row = &local_result->first_row();

    const Value* v = row->get_by_name("schema_version");
    if (!v->is_null()) {
      current_version = boost::string_ref(v->buffer().data(), v->buffer().size());
    }
  } else {
    logger_->debug("SchemaChangeHandler: No row found in %s's local system table",
                   connection()->address_string().c_str());
  }

  ResultResponse* peers_result =
      static_cast<ResultResponse*>(responses[1]);
  peers_result->decode_first_row();

  ResultIterator rows(peers_result);
  while (rows.next()) {
    const Row* row = rows.row();

    Address address;
    bool is_valid_address
        = ControlConnection::determine_address_for_peer_host(logger_,
                                                             connection()->address(),
                                                             row->get_by_name("peer"),
                                                             row->get_by_name("rpc_address"),
                                                             &address);

    if (is_valid_address && request_handler_->is_host_up(address)) {
      const Value* v = row->get_by_name("schema_version");
      if (!row->get_by_name("rpc_address")->is_null() && !v->is_null()) {
        boost::string_ref version(v->buffer().data(), v->buffer().size());
        if (version != current_version) {
          return false;
        }
      }
    }
  }

  return true;
}

void SchemaChangeHandler::on_set(const ResponseVec& responses) {
  elapsed_ms_ += get_time_since_epoch_ms() - start_ms_;

  bool has_error = false;
  for (MultipleRequestHandler::ResponseVec::const_iterator it = responses.begin(),
       end = responses.end(); it != end; ++it) {
    if (check_error_or_invalid_response("SchemaChangeHandler", CQL_OPCODE_RESULT,
                                        *it, logger_)) {
      has_error = true;
    }
  }
  if (has_error) return;

  if (has_schema_agreement(responses)) {
    logger_->debug("SchemaChangeHandler: Found schema agreement in %llu ms", elapsed_ms_);
    request_handler_->set_response(request_response_);
    return;
  } else if (elapsed_ms_ >= MAX_SCHEMA_AGREEMENT_WAIT_MS) {
    logger_->warn("SchemaChangeHandler: No schema aggreement on live nodes after %llu ms. "
                  "Schema may not be up-to-date on some nodes.",
                  elapsed_ms_);
    request_handler_->set_response(request_response_);
    return;
  }

  logger_->debug("SchemaChangeHandler: Schema still not up-to-date on some live nodes. "
                 "Trying again in %d ms", RETRY_SCHEMA_AGREEMENT_WAIT_MS);

  // Try again
  SharedRefPtr<SchemaChangeHandler> handler(
        new SchemaChangeHandler(connection(),
                                request_handler_.get(),
                                request_response_,
                                elapsed_ms_));
  connection()->schedule_schema_agreement(handler,
                                          RETRY_SCHEMA_AGREEMENT_WAIT_MS);
}

void SchemaChangeHandler::on_error(CassError code, const std::string& message) {
  std::ostringstream ss;
  ss << "SchemaChangeHandler: An error occured waiting for schema agreement: '" << message
     << "' (0x" << std::hex << std::uppercase << std::setw(8) << std::setfill('0') << code << ")";
  logger_->error(ss.str().c_str());
  request_handler_->set_response(request_response_);
}

void SchemaChangeHandler::on_timeout() {
  logger_->error("SchemaChangeHandler: A timeout occured waiting for schema agreement");
  request_handler_->set_response(request_response_);
}

void SchemaChangeHandler::on_closing() {
  logger_->warn("SchemaChangeHandler: Connection closed while waiting for schema agreement");
  request_handler_->set_response(request_response_);
}

} // namespace cass
