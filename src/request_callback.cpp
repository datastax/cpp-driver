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

#include "request_callback.hpp"

#include "config.hpp"
#include "connection.hpp"
#include "constants.hpp"
#include "logger.hpp"
#include "query_request.hpp"
#include "request.hpp"
#include "result_response.hpp"
#include "serialization.hpp"

namespace cass {

void RequestCallback::start(Connection* connection, int stream) {
  connection_ = connection;
  stream_ = stream;
  on_start();
}

int32_t RequestCallback::encode(int version, int flags, BufferVec* bufs) {
  if (version < 1 || version > 4) {
    return Request::REQUEST_ERROR_UNSUPPORTED_PROTOCOL;
  }

  size_t index = bufs->size();
  bufs->push_back(Buffer()); // Placeholder

  const Request* req = request();
  int32_t length = 0;

  if (version >= 4 && req->custom_payload()) {
    flags |= CASS_FLAG_CUSTOM_PAYLOAD;
    length += req->custom_payload()->encode(bufs);
  }

  int32_t result = req->encode(version, this, bufs);
  if (result < 0) return result;
  length += result;

  const size_t header_size
      = (version >= 3) ? CASS_HEADER_SIZE_V3 : CASS_HEADER_SIZE_V1_AND_V2;

  Buffer buf(header_size);
  size_t pos = 0;
  pos = buf.encode_byte(pos, version);
  pos = buf.encode_byte(pos, flags);

  if (version >= 3) {
    pos = buf.encode_int16(pos, stream_);
  } else {
    pos = buf.encode_byte(pos, stream_);
  }

  pos = buf.encode_byte(pos, req->opcode());
  buf.encode_int32(pos, length);
  (*bufs)[index] = buf;

  return length + header_size;
}

void RequestCallback::set_state(RequestCallback::State next_state) {
  switch (state_) {
    case REQUEST_STATE_NEW:
      if (next_state == REQUEST_STATE_NEW ||
          next_state == REQUEST_STATE_CANCELLED ||
          next_state == REQUEST_STATE_WRITING) {
        state_ = next_state;
      } else {
        assert(false && "Invalid request state after new");
      }
      break;

    case REQUEST_STATE_WRITING:
      if(next_state == REQUEST_STATE_READING ||
         next_state == REQUEST_STATE_READ_BEFORE_WRITE ||
         next_state == REQUEST_STATE_FINISHED) {
        state_ = next_state;
      } else if (next_state == REQUEST_STATE_CANCELLED) {
        state_ = REQUEST_STATE_CANCELLED_WRITING;
      } else {
        assert(false && "Invalid request state after writing");
      }
      break;

    case REQUEST_STATE_READING:
      if(next_state == REQUEST_STATE_FINISHED) {
        state_ = next_state;
      } else if (next_state == REQUEST_STATE_CANCELLED) {
        state_ = REQUEST_STATE_CANCELLED_READING;
      } else {
        assert(false && "Invalid request state after reading");
      }
      break;

    case REQUEST_STATE_READ_BEFORE_WRITE:
      if (next_state == REQUEST_STATE_FINISHED) {
        state_ = next_state;
      } else if (next_state == REQUEST_STATE_CANCELLED) {
        state_ = REQUEST_STATE_CANCELLED_READ_BEFORE_WRITE;
      } else {
        assert(false && "Invalid request state after read before write");
      }
      break;

    case REQUEST_STATE_FINISHED:
      if (next_state == REQUEST_STATE_NEW ||
          next_state == REQUEST_STATE_CANCELLED) {
        state_ = next_state;
      } else {
        assert(false && "Invalid request state after finished");
      }
      break;

    case REQUEST_STATE_CANCELLED:
      assert((next_state == REQUEST_STATE_FINISHED &&
              next_state == REQUEST_STATE_CANCELLED) ||
             "Invalid request state after cancelled");
      // Ignore. Leave the request in the cancelled state.
      break;


    case REQUEST_STATE_CANCELLED_READ_BEFORE_WRITE:
      if (next_state == REQUEST_STATE_CANCELLED) {
        state_ = next_state;
      } else {
        assert(false && "Invalid request state after cancelled (read before write)");
      }

    case REQUEST_STATE_CANCELLED_READING:
      if (next_state == REQUEST_STATE_CANCELLED) {
        state_ = next_state;
      } else {
        assert(false && "Invalid request state after cancelled (read outstanding)");
      }
      break;

    case REQUEST_STATE_CANCELLED_WRITING:
      if (next_state == REQUEST_STATE_CANCELLED ||
          next_state == REQUEST_STATE_CANCELLED_READING ||
          next_state == REQUEST_STATE_CANCELLED_READ_BEFORE_WRITE) {
        state_ = next_state;
      } else {
        assert(false && "Invalid request state after cancelled (write outstanding)");
      }
      break;

    default:
      assert(false && "Invalid request state");
      break;
  }
}

bool MultipleRequestCallback::get_result_response(const ResponseMap& responses,
                                                  const std::string& index,
                                                  ResultResponse** response) {
  ResponseMap::const_iterator it = responses.find(index);
  if (it == responses.end() || it->second->opcode() != CQL_OPCODE_RESULT) {
    return false;
  }
  *response = static_cast<ResultResponse*>(it->second.get());
  return true;
}

void MultipleRequestCallback::execute_query(const std::string& index, const std::string& query) {
  if (has_errors_or_timeouts_) return;
  responses_[index] = Response::Ptr();
  SharedRefPtr<InternalCallback> callback(
        new InternalCallback(Ptr(this),
                             Request::ConstPtr(new QueryRequest(query)), index));
  remaining_++;
  if (!connection_->write(callback)) {
    on_error(CASS_ERROR_LIB_NO_STREAMS, "No more streams available");
  }
}

MultipleRequestCallback::InternalCallback::InternalCallback(const MultipleRequestCallback::Ptr& parent,
                                                            const Request::ConstPtr& request,
                                                            const std::string& index)
  : SimpleRequestCallback(request)
  , parent_(parent)
  , index_(index) { }

void MultipleRequestCallback::InternalCallback::on_internal_set(ResponseMessage* response) {
  parent_->responses_[index_] = response->response_body();
  if (--parent_->remaining_ == 0 && !parent_->has_errors_or_timeouts_) {
    parent_->on_set(parent_->responses_);
  }
}

void MultipleRequestCallback::InternalCallback::on_internal_error(CassError code,
                                                                  const std::string& message) {
  if (!parent_->has_errors_or_timeouts_) {
    parent_->on_error(code, message);
  }
  parent_->has_errors_or_timeouts_ = true;
}

void MultipleRequestCallback::InternalCallback::on_internal_timeout() {
  if (!parent_->has_errors_or_timeouts_) {
    parent_->on_timeout();
  }
  parent_->has_errors_or_timeouts_ = true;
}

void SimpleRequestCallback::on_start() {
  timer_.start(connection()->loop(),
               request()->request_timeout_ms(connection()->config().request_timeout_ms()),
               this,
               on_timeout);
}

void SimpleRequestCallback::on_set(ResponseMessage* response) {
  timer_.stop();
  on_internal_set(response);
}

void SimpleRequestCallback::on_error(CassError code, const std::string& message) {
  timer_.stop();
  on_internal_error(code, message);
}

void SimpleRequestCallback::on_retry(bool use_next_host) {
  timer_.stop();
  on_internal_timeout(); // Retries are unhandled so timeout
}

void SimpleRequestCallback::on_cancel() {
  timer_.stop();
}

} // namespace cass
