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

#ifndef DATASTAX_INTERNAL_ADDRESS_FACTORY_HPP
#define DATASTAX_INTERNAL_ADDRESS_FACTORY_HPP

#include "config.hpp"
#include "host.hpp"
#include "ref_counted.hpp"

namespace datastax { namespace internal { namespace core {

class Row;

/**
 * An address factory that creates `Address` using the `rpc_address` column.
 */
class AddressFactory : public RefCounted<AddressFactory> {
public:
  typedef SharedRefPtr<AddressFactory> Ptr;
  virtual ~AddressFactory() {}
  virtual bool create(const Row* peers_row, const Host::Ptr& connected_host, Address* output);
  virtual bool is_peer(const Row* peers_row, const Host::Ptr& connected_host,
                       const Address& expected);
};

/**
 * An address factory that creates `Address` using the connected host's address and the `host_id`
 * (for the SNI servername) column.
 */
class SniAddressFactory : public AddressFactory {
  virtual bool create(const Row* peers_row, const Host::Ptr& connected_host, Address* output);
  virtual bool is_peer(const Row* peers_row, const Host::Ptr& connected_host,
                       const Address& expected);
};

inline AddressFactory* create_address_factory_from_config(const Config& config) {
  if (config.cloud_secure_connection_config().is_loaded()) {
    return new SniAddressFactory();
  } else {
    return new AddressFactory();
  }
}

}}} // namespace datastax::internal::core

#endif
