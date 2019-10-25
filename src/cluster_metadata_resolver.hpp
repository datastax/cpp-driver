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

#ifndef DATASTAX_INTERNAL_CLUSTER_METADATA_RESOLVER_HPP
#define DATASTAX_INTERNAL_CLUSTER_METADATA_RESOLVER_HPP

#include "address.hpp"
#include "allocated.hpp"
#include "callback.hpp"
#include "ref_counted.hpp"
#include "resolver.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace core {

struct ClusterSettings;

/**
 * An abstract class for resolving contact points and other cluster metadata.
 */
class ClusterMetadataResolver : public RefCounted<ClusterMetadataResolver> {
public:
  typedef SharedRefPtr<ClusterMetadataResolver> Ptr;
  typedef internal::Callback<void, ClusterMetadataResolver*> Callback;

  virtual ~ClusterMetadataResolver() {}

  void resolve(uv_loop_t* loop, const AddressVec& contact_points, const Callback& callback) {
    callback_ = callback;
    internal_resolve(loop, contact_points);
  }

  virtual void cancel() { internal_cancel(); }

  const AddressVec& resolved_contact_points() const { return resolved_contact_points_; }
  const String& local_dc() const { return local_dc_; }

protected:
  virtual void internal_resolve(uv_loop_t* loop, const AddressVec& contact_points) = 0;

  virtual void internal_cancel() = 0;

protected:
  AddressVec resolved_contact_points_;
  String local_dc_;
  Callback callback_;
};

/**
 * A interface for constructing instances of `ClusterMetadataResolver`s. The factory's instance
 * creation method is passed the cluster settings object to allow cluster metadata resolvers to
 * configure themselves with appropriate settings.
 */
class ClusterMetadataResolverFactory : public RefCounted<ClusterMetadataResolverFactory> {
public:
  typedef SharedRefPtr<ClusterMetadataResolverFactory> Ptr;

  virtual ~ClusterMetadataResolverFactory() {}
  virtual ClusterMetadataResolver::Ptr new_instance(const ClusterSettings& settings) const = 0;
  virtual const char* name() const = 0;
};

/**
 * This factory creates cluster metadata resolvers that determine contact points using DNS.
 * Domain names with multiple A-records are expanded into multiple contact points and addresses
 * that are already resolved are passed through.
 */
class DefaultClusterMetadataResolverFactory : public ClusterMetadataResolverFactory {
public:
  virtual ClusterMetadataResolver::Ptr new_instance(const ClusterSettings& settings) const;
  virtual const char* name() const { return "Default"; }
};

}}} // namespace datastax::internal::core

#endif
