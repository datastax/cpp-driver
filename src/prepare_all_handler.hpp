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

#ifndef DATASTAX_INTERNAL_PREPARE_ALL_HANDLER_HPP
#define DATASTAX_INTERNAL_PREPARE_ALL_HANDLER_HPP

#include "address.hpp"
#include "atomic.hpp"
#include "host.hpp"
#include "ref_counted.hpp"
#include "request_callback.hpp"
#include "request_handler.hpp"
#include "response.hpp"

namespace datastax { namespace internal { namespace core {

/**
 * A handler that tracks the progress of prepares on all hosts and returns the
 * initial "PREPARED" result response when the last prepare is finished.
 */
class PrepareAllHandler : public RefCounted<PrepareAllHandler> {
public:
  typedef SharedRefPtr<PrepareAllHandler> Ptr;

  PrepareAllHandler(const Host::Ptr& current_host, const Response::Ptr& response,
                    const RequestHandler::Ptr& request_handler, int remaining);

private:
  friend class PrepareAllCallback;

  const RequestWrapper& wrapper() const { return request_handler_->wrapper(); }
  void finish();

private:
  Host::Ptr current_host_;
  Response::Ptr response_;
  RequestHandler::Ptr request_handler_;
  Atomic<int> remaining_;
};

/**
 * A callback for preparing a statement. It's used in conjunction with
 * `PrepareAllHandler` to prepare a statement on all hosts. It calls finish
 *  on the handler when the request is done (success, error, or timeout).
 */
class PrepareAllCallback : public SimpleRequestCallback {
public:
  typedef SharedRefPtr<PrepareAllCallback> Ptr;

  PrepareAllCallback(const Address& address, const PrepareAllHandler::Ptr& handler);
  ~PrepareAllCallback();

private:
  void finish();

  virtual void on_internal_set(ResponseMessage* response);
  virtual void on_internal_error(CassError code, const String& message);
  virtual void on_internal_timeout();

private:
  Address address_;
  PrepareAllHandler::Ptr handler_;
  bool is_finished_;
};

}}} // namespace datastax::internal::core

#endif
