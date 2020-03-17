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

#ifndef DATASTAX_INTERNAL_TOKEN_MAP_HPP
#define DATASTAX_INTERNAL_TOKEN_MAP_HPP

#include "host.hpp"
#include "ref_counted.hpp"
#include "string.hpp"
#include "string_ref.hpp"

namespace datastax { namespace internal { namespace core {

class VersionNumber;
class Value;
class ResultResponse;

class TokenMap : public RefCounted<TokenMap> {
public:
  typedef SharedRefPtr<TokenMap> Ptr;

  static TokenMap::Ptr from_partitioner(StringRef partitioner);

  virtual ~TokenMap() {}

  virtual void add_host(const Host::Ptr& host) = 0;
  virtual void update_host_and_build(const Host::Ptr& host) = 0;
  virtual void remove_host_and_build(const Host::Ptr& host) = 0;

  virtual void add_keyspaces(const VersionNumber& cassandra_version,
                             const ResultResponse* result) = 0;
  virtual void update_keyspaces_and_build(const VersionNumber& cassandra_version,
                                          const ResultResponse* result) = 0;
  virtual void drop_keyspace(const String& keyspace_name) = 0;

  virtual void build() = 0;

  virtual TokenMap::Ptr copy() const = 0;

  virtual const CopyOnWriteHostVec& get_replicas(const String& keyspace_name,
                                                 const String& routing_key) const = 0;

  virtual String dump(const String& keyspace_name) const = 0;
};

}}} // namespace datastax::internal::core

#endif
