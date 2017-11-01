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

#include "prepare_host_handler.hpp"

#include "prepare_request.hpp"
#include "protocol.hpp"
#include "query_request.hpp"
#include "stream_manager.hpp"
#include "session.hpp"

#include <algorithm>

namespace cass {

struct CompareEntryKeyspace {
  bool operator()(const PreparedMetadata::Entry::Ptr& lhs,
                  const PreparedMetadata::Entry::Ptr& rhs) const {
    return lhs->keyspace() < rhs->keyspace();
  }
};

static int max_prepares_for_protocol_version(int protocol_version,
                                             unsigned max_prepares_per_flush) {
  return static_cast<int>(std::max(max_prepares_per_flush,
                                   static_cast<unsigned>(max_streams_for_protocol_version(protocol_version))));

}

PrepareHostHandler::PrepareHostHandler(const Host::Ptr& host,
                                       Session* session,
                                       int protocol_version)
  : host_(host)
  , protocol_version_(protocol_version)
  , session_(session)
  , callback_(NULL)
  , connection_(NULL)
  , prepares_outstanding_(0)
  , max_prepares_outstanding_(max_prepares_for_protocol_version(
                                protocol_version,
                                session->config().max_requests_per_flush())) { }

void PrepareHostHandler::prepare(Callback callback) {
  callback_ = callback;
  prepared_metadata_entries_ = session_->prepared_metadata_entries();

  if (prepared_metadata_entries_.empty()) {
    callback_(this);
    return;
  }

  // Sort by keyspace to minimize the number of times the keyspace
  // needs to be changed.
  std::sort(prepared_metadata_entries_.begin(),
            prepared_metadata_entries_.end(),
            CompareEntryKeyspace());

  current_entry_it_ = prepared_metadata_entries_.begin();

  connection_ = new Connection(session_->loop(),
                               session_->config(),
                               session_->metrics(),
                               host_,
                               "", // No keyspace
                               protocol_version_,
                               this);

  inc_ref(); // Reference for the event loop

  connection_->connect();
}

void PrepareHostHandler::on_ready(Connection* connection) {
  prepare_next();
}

void PrepareHostHandler::on_close(Connection* connection) {
  callback_(this);

  dec_ref(); // The event loop is done with this handler
}

// This is the main loop for preparing statements. It's called after each
// request successfully completes, either setting the keyspace or preparing
// a statement. It attempts to group prepare requests into a single batch as
// long as the keyspace is the same and the number of outstanding requests is
// under the maximum.
void PrepareHostHandler::prepare_next() {
  // Finish current prepares before continuing
  if (--prepares_outstanding_ > 0) {
    return;
  }

  // Check to see if we're done
  if (is_done()) {
    close();
    return;
  }

  prepares_outstanding_ = 0;

  // Write prepare requests until there's no more left, the keyspace changes,
  // or the maximum number of oustanding prepares is reached.
  while (!is_done() &&
         check_and_set_keyspace() &&
         prepares_outstanding_ < max_prepares_outstanding_) {
    const std::string& query((*current_entry_it_)->query());
    PrepareRequest::Ptr prepare_request(new PrepareRequest(query));

    // Set the keyspace in case per request keyspaces are supported
    prepare_request->set_keyspace(current_keyspace_);
    prepare_request->set_request_timeout_ms(session_->config().request_timeout_ms());

    if (!connection_->write(PrepareCallback::Ptr(
                              new PrepareCallback(prepare_request, Ptr(this))),
                            false)) { // Flush after we write all prepare requests
      LOG_WARN("Failed to write prepare request while preparing all queries on host %s",
               host_->address_string().c_str());
      close();
      return;
    }

    prepares_outstanding_++;
    current_entry_it_++;
  }

  connection_->flush();
}

bool PrepareHostHandler::check_and_set_keyspace() {
  if (supports_set_keyspace(protocol_version_)) {
    return true;
  }

  const std::string& keyspace((*current_entry_it_)->keyspace());

  if (keyspace != current_keyspace_) {
    if (!connection_->write(PrepareCallback::Ptr(
                              new SetKeyspaceCallback(keyspace, Ptr(this))))) {
      LOG_WARN("Failed to write \"USE\" keyspace request while preparing all queries on host %s",
               host_->address_string().c_str());
      close();
      return false;
    }
    current_keyspace_ = keyspace;
    return false;
  }

  return true;
}

bool PrepareHostHandler::is_done() const {
  return current_entry_it_ == prepared_metadata_entries_.end();
}

void PrepareHostHandler::close() {
  connection_->close();
}

PrepareHostHandler::PrepareCallback::PrepareCallback(const PrepareRequest::ConstPtr& prepare_request,
                                                     const PrepareHostHandler::Ptr& handler)
  : SimpleRequestCallback(prepare_request)
  , handler_(handler) { }

void PrepareHostHandler::PrepareCallback::on_internal_set(ResponseMessage* response) {
  LOG_DEBUG("Successfully prepared query \"%s\" on host %s while preparing all queries",
            static_cast<const PrepareRequest*>(request())->query().c_str(),
            handler_->host()->address_string().c_str());
  handler_->prepare_next();
}

void PrepareHostHandler::PrepareCallback::on_internal_error(CassError code, const std::string& message) {
  LOG_WARN("Prepare request failed on host %s while attempting to prepare all queries: %s (%s)",
           handler_->host_->address_string().c_str(),
           message.c_str(),
           cass_error_desc(code));
  handler_->close();
}

void PrepareHostHandler::PrepareCallback::on_internal_timeout() {
  LOG_WARN("Prepare request timed out on host %s while attempting to prepare all queries",
           handler_->host_->address_string().c_str());
  handler_->close();
}

PrepareHostHandler::SetKeyspaceCallback::SetKeyspaceCallback(const std::string& keyspace,
                                                             const PrepareHostHandler::Ptr& handler)
  : SimpleRequestCallback(Request::ConstPtr(
                            new QueryRequest("USE " + keyspace)))
  , handler_(handler) { }

void PrepareHostHandler::SetKeyspaceCallback::on_internal_set(ResponseMessage* response) {
  LOG_TRACE("Successfully set keyspace to \"%s\" on host %s while preparing all queries",
            handler_->current_keyspace_.c_str(),
            handler_->host()->address_string().c_str());
  handler_->prepare_next();
}

void PrepareHostHandler::SetKeyspaceCallback::on_internal_error(CassError code, const std::string& message) {
  LOG_WARN("\"USE\" keyspace request failed on host %s while attempting to prepare all queries: %s (%s)",
           handler_->host_->address_string().c_str(),
           message.c_str(),
           cass_error_desc(code));
  handler_->close();
}

void PrepareHostHandler::SetKeyspaceCallback::on_internal_timeout() {
  LOG_WARN("\"USE\" keyspace request timed out on host %s while attempting to prepare all queries",
           handler_->host_->address_string().c_str());
  handler_->close();
}

} // namespace cass
