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

#ifndef DATASTAX_INTERNAL_STARTUP_REQUEST_HPP
#define DATASTAX_INTERNAL_STARTUP_REQUEST_HPP

#include "constants.hpp"
#include "map.hpp"
#include "request.hpp"
#include "scoped_ptr.hpp"
#include "string.hpp"

namespace datastax { namespace internal { namespace core {

class StartupRequest : public Request {
public:
  StartupRequest(const String& application_name, const String& application_version,
                 const String& client_id, bool no_compact_enabled)
      : Request(CQL_OPCODE_STARTUP)
      , application_name_(application_name)
      , application_version_(application_version)
      , client_id_(client_id)
      , no_compact_enabled_(no_compact_enabled) {}

  const String& application_name() const { return application_name_; }
  const String& application_version() const { return application_version_; }
  const String& client_id() const { return client_id_; }
  bool no_compact_enabled() const { return no_compact_enabled_; }

private:
  int encode(ProtocolVersion version, RequestCallback* callback, BufferVec* bufs) const;

private:
  typedef Map<String, String> OptionsMap;

  String application_name_;
  String application_version_;
  String client_id_;
  bool no_compact_enabled_;
};

}}} // namespace datastax::internal::core

#endif
