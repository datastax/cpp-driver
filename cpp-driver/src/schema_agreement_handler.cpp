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

#include "result_iterator.hpp"

#define RETRY_SCHEMA_AGREEMENT_WAIT_MS 200
#define SELECT_LOCAL_SCHEMA "SELECT schema_version FROM system.local WHERE key='local'"
#define SELECT_PEERS_SCHEMA "SELECT peer, rpc_address, host_id, schema_version FROM system.peers"

using namespace datastax;
using namespace datastax::internal::core;

SchemaAgreementHandler::SchemaAgreementHandler(const RequestHandler::Ptr& request_handler,
                                               const Host::Ptr& current_host,
                                               const Response::Ptr& response,
                                               SchemaAgreementListener* listener,
                                               uint64_t max_wait_time_ms,
                                               const AddressFactory::Ptr& address_factory)
    : WaitForHandler(request_handler, current_host, response, max_wait_time_ms,
                     RETRY_SCHEMA_AGREEMENT_WAIT_MS)
    , listener_(listener)
    , address_factory_(address_factory) {}

ChainedRequestCallback::Ptr SchemaAgreementHandler::callback() {
  WaitforRequestVec requests;
  requests.push_back(make_request("local", SELECT_LOCAL_SCHEMA));
  requests.push_back(make_request("peers", SELECT_PEERS_SCHEMA));
  return WaitForHandler::callback(requests);
}

bool SchemaAgreementHandler::on_set(const ChainedRequestCallback::Ptr& callback) {
  StringRef current_version;

  ResultResponse::Ptr local_result(callback->result("local"));
  if (local_result && local_result->row_count() > 0) {
    const Row* row = &local_result->first_row();

    const Value* v = row->get_by_name("schema_version");
    if (!v->is_null()) {
      current_version = v->to_string_ref();
    }
  } else {
    LOG_DEBUG("No row found in %s's local system table", host()->address_string().c_str());
  }

  ResultResponse::Ptr peers_result(callback->result("peers"));
  if (peers_result) {
    ResultIterator rows(peers_result.get());
    while (rows.next()) {
      const Row* row = rows.row();

      Address address;
      bool is_valid_address = address_factory_->create(row, this->host(), &address);

      if (is_valid_address && listener_->on_is_host_up(address)) {
        const Value* v = row->get_by_name("schema_version");
        if (!row->get_by_name("rpc_address")->is_null() && !v->is_null()) {
          StringRef version(v->to_string_ref());
          if (version != current_version) {
            LOG_DEBUG("Schema still not up-to-date on some live nodes. "
                      "Trying again in %llu ms",
                      static_cast<unsigned long long>(retry_wait_time_ms()));
            return false;
          }
        }
      }
    }
  }

  LOG_DEBUG("Found schema agreement in %llu ms",
            static_cast<unsigned long long>(get_time_since_epoch_ms() - start_time_ms()));

  return true;
}

void SchemaAgreementHandler::on_error(WaitForHandler::WaitForError code, const String& message) {
  switch (code) {
    case WAIT_FOR_ERROR_REQUEST_ERROR:
      LOG_ERROR("An error occurred waiting for schema agreement: %s", message.c_str());
      break;
    case WAIT_FOR_ERROR_REQUEST_TIMEOUT:
      LOG_WARN("A query timeout occurred waiting for schema agreement");
      break;
    case WAIT_FOR_ERROR_CONNECTION_CLOSED:
      LOG_WARN("Connection closed while attempting to check schema agreement");
      break;
    case WAIT_FOR_ERROR_NO_STREAMS:
      LOG_WARN("No stream available when attempting to check schema agreement");
      break;
    case WAIT_FOR_ERROR_TIMEOUT:
      LOG_WARN("No schema agreement on live nodes after %llu ms. "
               "Schema may not be up-to-date on some nodes.",
               static_cast<unsigned long long>(max_wait_time_ms()));
      break;
  }
}
