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

#ifndef __CASS_PREPARE_HANDLER_HPP_INCLUDED__
#define __CASS_PREPARE_HANDLER_HPP_INCLUDED__

#include "response_callback.hpp"
#include "request_handler.hpp"
#include "message.hpp"
#include "prepare_request.hpp"
#include "io_worker.hpp"

namespace cass {

class PrepareHandler : public ResponseCallback {
public:
  PrepareHandler(RequestHandler* request_handler)
      : request_(new Message())
      , request_handler_(request_handler) {}

  bool init(const std::string& prepared_id) {
    PrepareRequest* prepare = new PrepareRequest();
    request_->opcode = prepare->opcode();
    request_->body.reset(prepare);
    if (request_handler_->request()->opcode == CQL_OPCODE_EXECUTE) {
      ExecuteRequest* execute =
          static_cast<ExecuteRequest*>(request_handler_->request()->body.get());
      prepare->prepare_string(execute->prepared_statement);
      return true;
    } else if (request_handler_->request()->opcode == CQL_OPCODE_BATCH) {
      BatchRequest* batch =
          static_cast<BatchRequest*>(request_handler_->request()->body.get());
      std::string prepared_statement;
      if (batch->prepared_statement(prepared_id, &prepared_statement)) {
        prepare->prepare_string(prepared_statement);
        return true;
      }
    }
    return false; // Invalid request type
  }

  virtual Message* request() const { return request_.get(); }

  virtual void on_set(Message* response) {
    switch (response->opcode) {
      case CQL_OPCODE_RESULT: {
        ResultResponse* result =
            static_cast<ResultResponse*>(response->body.get());
        if (result->kind == CASS_RESULT_KIND_PREPARED) {
          request_handler_.release()->retry(RETRY_WITH_CURRENT_HOST);
        } else {
          request_handler_.release()->retry(RETRY_WITH_NEXT_HOST);
        }
      } break;
      case CQL_OPCODE_ERROR:
        request_handler_.release()->retry(RETRY_WITH_NEXT_HOST);
        break;
      default:
        break;
    }
  }

  virtual void on_error(CassError code, const std::string& message) {
    request_handler_.release()->retry(RETRY_WITH_NEXT_HOST);
  }

  virtual void on_timeout() {
    request_handler_.release()->retry(RETRY_WITH_NEXT_HOST);
  }

private:
  std::unique_ptr<Message> request_;
  std::unique_ptr<RequestHandler> request_handler_;
};

} // namespace cass

#endif
