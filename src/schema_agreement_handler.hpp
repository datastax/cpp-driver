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

#ifndef DATASTAX_INTERNAL_SCHEMA_CHANGE_HANDLER_HPP
#define DATASTAX_INTERNAL_SCHEMA_CHANGE_HANDLER_HPP

#include "address_factory.hpp"
#include "wait_for_handler.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace core {

/**
 * A listener that used for determining if a host is up.
 */
class SchemaAgreementListener {
public:
  virtual ~SchemaAgreementListener() {}

  /**
   * A callback for determining if a host is up.
   *
   * @param address A host address.
   * @return Returns true if the host is up.
   */
  virtual bool on_is_host_up(const Address& address) = 0;
};

/**
 * A handler that waits for schema agreement after schema changes. It waits for
 * schema to propagate to all nodes then it sets the future of the request that
 * originally made the schema change.
 */
class SchemaAgreementHandler : public WaitForHandler {
public:
  typedef SharedRefPtr<SchemaAgreementHandler> Ptr;

  /**
   * Constructor.
   *
   * @param request_handler The request handler for the schema change.
   * @param current_host The host that processed the schema change.
   * @param response The original response for the schema change.
   * @param listener A listener for determining host liveness.
   * @param max_wait_time_ms The maximum amount of time to wait for the data to
   * become available.
   * @param address_factory Address factory for determining peer addresses.
   */
  SchemaAgreementHandler(const RequestHandler::Ptr& request_handler, const Host::Ptr& current_host,
                         const Response::Ptr& response, SchemaAgreementListener* listener,
                         uint64_t max_wait_time_ms, const AddressFactory::Ptr& address_factory);

  /**
   * Gets a request callback for executing queries on behalf of the handler.
   *
   * @return Returns a chain of queries to execute.
   */
  ChainedRequestCallback::Ptr callback();

private:
  virtual bool on_set(const ChainedRequestCallback::Ptr& callback);
  virtual void on_error(WaitForError code, const String& message);

private:
  SchemaAgreementListener* const listener_;
  AddressFactory::Ptr address_factory_;
};

}}} // namespace datastax::internal::core

#endif
