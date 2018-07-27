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

#include "schema_agreement_handler.hpp"

#include "cassandra.h"
#include "control_connection.hpp"
#include "query_request.hpp"
#include "result_iterator.hpp"

#include <iomanip>

#define RETRY_SCHEMA_AGREEMENT_WAIT_MS 200
#define SELECT_LOCAL_SCHEMA "SELECT schema_version FROM system.local WHERE key='local'"
#define SELECT_PEERS_SCHEMA "SELECT peer, rpc_address, schema_version FROM system.peers"

namespace cass {

/**
 * A request callback for executing multiple queries together for the schema
 * agreement handler.
 */
class SchemaAgreementCallback : public ChainedRequestCallback {
public:
  SchemaAgreementCallback(const String& key, const String& query,
                          const SchemaAgreementHandler::Ptr& handler)
    : ChainedRequestCallback(key, query)
    , handler_(handler) { }

private:
  virtual void on_chain_write(Connection *connection);
  virtual void on_chain_set();
  virtual void on_chain_error(CassError code, const String& message);
  virtual void on_chain_timeout();

  bool has_schema_aggrement();

private:
  SchemaAgreementHandler::Ptr handler_;
};

void SchemaAgreementCallback::on_chain_write(Connection* connection) {
  handler_->start(connection);
}

void SchemaAgreementCallback::on_chain_set() {
  assert(handler_->connection_);
  if (has_schema_aggrement()) {
    LOG_DEBUG("Found schema agreement in %llu ms",
              static_cast<unsigned long long>(get_time_since_epoch_ms() - handler_->start_time_ms_));
    handler_->finish();
  } else {
    handler_->schedule();
  }
}

void SchemaAgreementCallback::on_chain_error(CassError code, const String& message) {
  OStringStream ss;
  ss << "An error occurred waiting for schema agreement: '" << message
     << "' (0x" << std::hex << std::uppercase << std::setw(8) << std::setfill('0') << code << ")";
  LOG_ERROR("%s", ss.str().c_str());
  handler_->finish();
}

void SchemaAgreementCallback::on_chain_timeout() {
  LOG_ERROR("A query timeout occurred waiting for schema agreement");
  handler_->finish();
}

bool SchemaAgreementCallback::has_schema_aggrement() {
  StringRef current_version;

  ResultResponse::Ptr local_result(result("local"));
  if (local_result && local_result->row_count() > 0) {
    const Row* row = &local_result->first_row();

    const Value* v = row->get_by_name("schema_version");
    if (!v->is_null()) {
      current_version = v->to_string_ref();
    }
  } else {
    LOG_DEBUG("No row found in %s's local system table",
              handler_->connection_->address_string().c_str());
  }

  ResultResponse::Ptr peers_result(result("peers"));
  if (peers_result) {
    ResultIterator rows(peers_result.get());
    while (rows.next()) {
      const Row* row = rows.row();

      Address address;
      bool is_valid_address
          = determine_address_for_peer_host(handler_->connection_->address(),
                                            row->get_by_name("peer"),
                                            row->get_by_name("rpc_address"),
                                            &address);

      if (is_valid_address && handler_->listener_->on_is_host_up(address)) {
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

ChainedRequestCallback::Ptr SchemaAgreementHandler::callback() {
  return Memory::allocate<SchemaAgreementCallback>(
        "local", SELECT_LOCAL_SCHEMA,
        Ptr(this))
      ->chain("peers", SELECT_PEERS_SCHEMA);
}

void SchemaAgreementHandler::start(Connection* connection) {
  if (!connection_) { // Start only once
    inc_ref(); // Reference for the event loop
    connection_.reset(connection);
    timer_.start(connection_->loop(), max_schema_wait_time_ms_,
                 bind_callback(&SchemaAgreementHandler::on_timeout, this));
  }
}

void SchemaAgreementHandler::schedule() {
  LOG_DEBUG("Schema still not up-to-date on some live nodes. "
            "Trying again in %d ms", RETRY_SCHEMA_AGREEMENT_WAIT_MS);
  retry_timer_.start(connection_->loop(), RETRY_SCHEMA_AGREEMENT_WAIT_MS,
                     bind_callback(&SchemaAgreementHandler::on_retry_timeout, this));
}

void SchemaAgreementHandler::finish() {
  if (!is_finished_) {
    is_finished_ = true;
    request_handler_->set_response(current_host_, response_);
    if (connection_) {
      retry_timer_.stop();
      timer_.stop();
      dec_ref();
    }
  }
}

void SchemaAgreementHandler::on_retry_timeout(Timer* timer) {
  if (connection_->is_closing()) {
    LOG_WARN("Connection closed while attempting to check schema agreement");
    finish();
  } else if (connection_->write_and_flush(callback()) == Request::REQUEST_ERROR_NO_AVAILABLE_STREAM_IDS) {
    LOG_WARN("No stream available when attempting to check schema agreement");
    finish();
  }
}

void SchemaAgreementHandler::on_timeout(Timer* timer) {
  LOG_WARN("No schema agreement on live nodes after %llu ms. "
           "Schema may not be up-to-date on some nodes.",
           static_cast<unsigned long long>(max_schema_wait_time_ms_));
  finish();
}

SchemaAgreementHandler::SchemaAgreementHandler(const RequestHandler::Ptr& request_handler,
                                               const Host::Ptr& current_host,
                                               const Response::Ptr& response,
                                               SchemaAgreementListener* listener,
                                               uint64_t max_schema_wait_time_ms)
  : is_finished_(false)
  , start_time_ms_(get_time_since_epoch_ms())
  , max_schema_wait_time_ms_(max_schema_wait_time_ms)
  , listener_(listener)
  , request_handler_(request_handler)
  , current_host_(current_host)
  , response_(response) { }

} // namespace cass
