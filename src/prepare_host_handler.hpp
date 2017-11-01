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


#ifndef __CASS_PREPARE_HOST_HANDLER_HPP_INCLUDED__
#define __CASS_PREPARE_HOST_HANDLER_HPP_INCLUDED__

#include "connection.hpp"
#include "host.hpp"
#include "prepared.hpp"
#include "ref_counted.hpp"

#include <string>

namespace cass {

class Session;

/**
 * A handler for pre-preparing statements on a newly available host.
 */
class PrepareHostHandler : public RefCounted<PrepareHostHandler>
                         , public Connection::Listener {
public:
  typedef void (*Callback)(const PrepareHostHandler*);

  typedef SharedRefPtr<PrepareHostHandler> Ptr;

  PrepareHostHandler(const Host::Ptr& host,
                     Session* session,
                     int protocol_version);

  Session* session() const { return session_; }
  const Host::Ptr host() const { return host_; }

  void prepare(Callback callback);

private:
  virtual void on_ready(Connection* connection);

  virtual void on_close(Connection* connection);

  virtual void on_availability_change(Connection* connection) { }

  virtual void on_event(EventResponse* response) { }

private:
  /**
   * A callback for preparing a single statement on a host. It continues the
   * preparation process on success, otherwise it closes the temporary
   * connection and logs a warning.
   */
  class PrepareCallback : public SimpleRequestCallback {
  public:
    PrepareCallback(const PrepareRequest::ConstPtr& prepare_request,
                    const PrepareHostHandler::Ptr& handler);

    virtual void on_internal_set(ResponseMessage* response);

    virtual void on_internal_error(CassError code, const std::string& message);

    virtual void on_internal_timeout();

  private:
    PrepareHostHandler::Ptr handler_;
  };

  /**
   * A callback for setting the keyspace on a connection. This is requrired
   * pre-V5/DSEv2 because the keyspace state is per connection.  It continues
   * the preparation process on success, otherwise it closes the temporary
   * connection and logs a warning.
   */
  class SetKeyspaceCallback : public SimpleRequestCallback {
  public:
    SetKeyspaceCallback(const std::string& keyspace,
                        const PrepareHostHandler::Ptr& handler);

    virtual void on_internal_set(ResponseMessage* response);

    virtual void on_internal_error(CassError code, const std::string& message);

    virtual void on_internal_timeout();

  private:
    PrepareHostHandler::Ptr handler_;
  };

private:
  // This is the main method for iterating over the list of prepared statements
  void prepare_next();

  // Returns true if the keyspace is current or using protocol v5/DSEv2
  bool check_and_set_keyspace();

  bool is_done() const;

  void close();

private:
  const Host::Ptr host_;
  int protocol_version_;
  Session* session_;
  Callback callback_;
  Connection* connection_;
  std::string current_keyspace_;
  int prepares_outstanding_;
  const int max_prepares_outstanding_;
  PreparedMetadata::Entry::Vec prepared_metadata_entries_;
  PreparedMetadata::Entry::Vec::const_iterator current_entry_it_;
};

} // namespace cass

#endif
