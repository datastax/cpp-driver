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

#include "address_factory.hpp"

#include "row.hpp"

using namespace datastax::internal::core;

bool AddressFactory::create(const Row* peers_row, const Host::Ptr& connected_host,
                            Address* output) {
  Address connected_address = connected_host->address();
  const Value* peer_value = peers_row->get_by_name("peer");
  const Value* rpc_value = peers_row->get_by_name("rpc_address");

  Address peer_address;
  if (!peer_value ||
      !peer_value->decoder().as_inet(peer_value->size(), connected_address.port(), &peer_address)) {
    LOG_WARN("Invalid address format for peer address");
    return false;
  }
  if (rpc_value && !rpc_value->is_null()) {
    if (!rpc_value->decoder().as_inet(rpc_value->size(), connected_address.port(), output)) {
      LOG_WARN("Invalid address format for rpc address");
      return false;
    }
    if (connected_address == *output || connected_address == peer_address) {
      LOG_DEBUG("system.peers on %s contains a line with rpc_address for itself. "
                "This is not normal, but is a known problem for some versions of DSE. "
                "Ignoring this entry.",
                connected_address.to_string(false).c_str());
      return false;
    }
    if (Address("0.0.0.0", 0).equals(*output, false) || Address("::", 0).equals(*output, false)) {
      LOG_WARN("Found host with 'bind any' for rpc_address; using listen_address (%s) to contact "
               "instead. If this is incorrect you should configure a specific interface for "
               "rpc_address on the server.",
               peer_address.to_string(false).c_str());
      *output = peer_address;
    }
  } else {
    LOG_WARN("No rpc_address for host %s in system.peers on %s. Ignoring this entry.",
             peer_address.to_string(false).c_str(), connected_address.to_string(false).c_str());
    return false;
  }
  return true;
}

bool AddressFactory::is_peer(const Row* peers_row, const Host::Ptr& connected_host,
                             const Address& expected) {
  Address address;
  return create(peers_row, connected_host, &address) && address == expected;
}

bool SniAddressFactory::create(const Row* peers_row, const Host::Ptr& connected_host,
                               Address* output) {
  CassUuid host_id;
  if (!peers_row->get_uuid_by_name("host_id", &host_id)) {
    // Attempt to get an peer address for the error log.
    Address peer_address;
    const Value* peer_value = peers_row->get_by_name("peer");
    if (!peer_value || !peer_value->decoder().as_inet(
                           peer_value->size(), connected_host->address().port(), &peer_address)) {
      LOG_WARN("Invalid address format for peer address");
    }
    LOG_ERROR("Invalid `host_id` for host. %s will be ignored.",
              peer_address.is_valid() ? peer_address.to_string().c_str() : "<unknown>");
    return false;
  }
  *output = Address(connected_host->address().hostname_or_address(),
                    connected_host->address().port(), to_string(host_id));
  return true;
}

bool SniAddressFactory::is_peer(const Row* peers_row, const Host::Ptr& connected_host,
                                const Address& expected) {
  const Value* value = peers_row->get_by_name("rpc_address");
  Address rpc_address;
  if (!value ||
      !value->decoder().as_inet(value->size(), connected_host->address().port(), &rpc_address)) {
    return false;
  }
  return rpc_address == expected;
}
