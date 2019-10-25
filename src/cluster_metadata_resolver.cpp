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

#include "cluster_metadata_resolver.hpp"

#include "cluster.hpp"
#include "logger.hpp"

using namespace datastax::internal::core;

namespace {

class DefaultClusterMetadataResolver : public ClusterMetadataResolver {
public:
  DefaultClusterMetadataResolver(uint64_t resolve_timeout_ms, int port)
      : resolve_timeout_ms_(resolve_timeout_ms)
      , port_(port) {}

private:
  virtual void internal_resolve(uv_loop_t* loop, const AddressVec& contact_points) {
    inc_ref();

    for (AddressVec::const_iterator it = contact_points.begin(), end = contact_points.end();
         it != end; ++it) {
      // If the port is not set then use the default port value.
      int port = it->port() <= 0 ? port_ : it->port();

      if (it->is_resolved()) {
        resolved_contact_points_.push_back(Address(it->hostname_or_address(), port));
      } else {
        if (!resolver_) {
          resolver_.reset(
              new MultiResolver(bind_callback(&DefaultClusterMetadataResolver::on_resolve, this)));
        }
        resolver_->resolve(loop, it->hostname_or_address(), port, resolve_timeout_ms_);
      }
    }

    if (!resolver_) {
      callback_(this);
      dec_ref();
      return;
    }
  }

  virtual void internal_cancel() {
    if (resolver_) resolver_->cancel();
  }

private:
  void on_resolve(MultiResolver* resolver) {
    const Resolver::Vec& resolvers = resolver->resolvers();
    for (Resolver::Vec::const_iterator it = resolvers.begin(), end = resolvers.end(); it != end;
         ++it) {
      const Resolver::Ptr resolver(*it);
      if (resolver->is_success()) {
        const AddressVec& addresses = resolver->addresses();
        if (!addresses.empty()) {
          for (AddressVec::const_iterator it = addresses.begin(), end = addresses.end(); it != end;
               ++it) {
            resolved_contact_points_.push_back(*it);
          }
        } else {
          LOG_ERROR("No addresses resolved for %s:%d\n", resolver->hostname().c_str(),
                    resolver->port());
        }
      } else if (resolver->is_timed_out()) {
        LOG_ERROR("Timed out attempting to resolve address for %s:%d\n",
                  resolver->hostname().c_str(), resolver->port());
      } else if (!resolver->is_canceled()) {
        LOG_ERROR("Unable to resolve address for %s:%d\n", resolver->hostname().c_str(),
                  resolver->port());
      }
    }

    callback_(this);
    dec_ref();
  }

private:
  MultiResolver::Ptr resolver_;
  const uint64_t resolve_timeout_ms_;
  const int port_;
};

} // namespace

ClusterMetadataResolver::Ptr
DefaultClusterMetadataResolverFactory::new_instance(const ClusterSettings& settings) const {
  return ClusterMetadataResolver::Ptr(new DefaultClusterMetadataResolver(
      settings.control_connection_settings.connection_settings.socket_settings.resolve_timeout_ms,
      settings.port));
}
