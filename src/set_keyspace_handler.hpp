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

#ifndef __CASS_SET_KEYSPACE_HANDLER_HPP_INCLUDED__
#define __CASS_SET_KEYSPACE_HANDLER_HPP_INCLUDED__

#include "response_callback.hpp"
#include "request_handler.hpp"
#include "message.hpp"
#include "prepare_request.hpp"
#include "io_worker.hpp"
#include "connection.hpp"

namespace cass {

class SetKeyspaceHandler : public ResponseCallback {
public:
  SetKeyspaceHandler(const std::string& keyspace, Connection* connection,
                     ResponseCallback* response_callback)
      : connection_(connection)
      , request_(new Message(CQL_OPCODE_QUERY))
      , response_callback_(response_callback) {
    QueryRequest* query = static_cast<QueryRequest*>(request_->body.get());
    query->statement("use \"" + keyspace + "\"");
    query->consistency(CASS_CONSISTENCY_ONE);
  }

  virtual Message* request() const { return request_.get(); }

  virtual void on_set(Message* response) {
    switch (response->opcode) {
      case CQL_OPCODE_RESULT:
        on_result_response(response);
        break;
      case CQL_OPCODE_ERROR:
        connection_->defunct();
        response_callback_->on_error(CASS_ERROR_UNABLE_TO_SET_KEYSPACE,
                                     "Unable to set keyspsace");
        break;
      default:
        break;
    }
  }

  virtual void on_error(CassError code, const std::string& message) {
    connection_->defunct();
    response_callback_->on_error(CASS_ERROR_UNABLE_TO_SET_KEYSPACE,
                                 "Unable to set keyspsace");
  }

  virtual void on_timeout() {
    // TODO(mpenick): What to do here?
    response_callback_->on_timeout();
  }

private:
  void on_result_response(Message* response) {
    ResultResponse* result = static_cast<ResultResponse*>(response->body.get());
    if (result->kind == CASS_RESULT_KIND_SET_KEYSPACE) {
      connection_->execute(response_callback_.release());
    } else {
      connection_->defunct();
      response_callback_->on_error(CASS_ERROR_UNABLE_TO_SET_KEYSPACE,
                                   "Unable to set keyspsace");
    }
  }

private:
  Connection* connection_;
  std::unique_ptr<Message> request_;
  std::unique_ptr<ResponseCallback> response_callback_;
};

} // namespace cass

#endif
