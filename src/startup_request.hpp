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

#ifndef __CASS_STARTUP_REQUEST_HPP_INCLUDED__
#define __CASS_STARTUP_REQUEST_HPP_INCLUDED__

#include "request.hpp"
#include "constants.hpp"
#include "scoped_ptr.hpp"

#include <map>
#include <string>

namespace cass {

class StartupRequest : public Request {
public:
  StartupRequest(bool no_compact_enabled)
      : Request(CQL_OPCODE_STARTUP)
      , version_("3.0.0")
      , compression_("")
      , no_compact_enabled_(no_compact_enabled) { }

  const std::string version() const { return version_; }
  const std::string compression() const { return compression_; }
  bool no_compact_enabled() const { return no_compact_enabled_; }

private:
  int encode(int version, RequestCallback* callback, BufferVec* bufs) const;

private:
  typedef std::map<std::string, std::string> OptionsMap;

  std::string version_;
  std::string compression_;
  bool no_compact_enabled_;
};

} // namespace cass

#endif
