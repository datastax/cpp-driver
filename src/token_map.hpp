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

#ifndef __CASS_TOKEN_MAP_HPP_INCLUDED__
#define __CASS_TOKEN_MAP_HPP_INCLUDED__

#include "host.hpp"

#include <string>

namespace cass {

class VersionNumber;
class Value;
class ResultResponse;
class StringRef;

class TokenMap {
public:
  static TokenMap* from_partitioner(StringRef partitioner);

  virtual ~TokenMap() { }

  virtual void add_host(const Host::Ptr& host, const Value* tokens) = 0;
  virtual void update_host_and_build(const Host::Ptr& host, const Value* tokens) = 0;
  virtual void remove_host_and_build(const Host::Ptr& host) = 0;
  virtual void clear_tokens_and_hosts() = 0;

  virtual void add_keyspaces(const VersionNumber& cassandra_version, ResultResponse* result) = 0;
  virtual void update_keyspaces_and_build(const VersionNumber& cassandra_version, ResultResponse* result) = 0;
  virtual void drop_keyspace(const std::string& keyspace_name) = 0;
  virtual void clear_replicas_and_strategies() = 0;

  virtual void build() = 0;

  virtual const CopyOnWriteHostVec& get_replicas(const std::string& keyspace_name,
                                                 const std::string& routing_key) const = 0;
};

} // namespace cass

#endif
