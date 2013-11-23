/*
  Copyright (c) 2013 Matthew Stump

  This file is part of cassandra.

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

#ifndef __CQL_H_INCLUDED__
#define __CQL_H_INCLUDED__

#include <stdint.h>
#include <boost/noncopyable.hpp>
#include <cql/cql_config.hpp>

namespace cql {

typedef ::uint8_t   cql_byte_t;
typedef ::uint16_t  cql_short_t;
typedef ::int32_t   cql_int_t;
typedef ::int64_t   cql_bigint_t;
typedef ::int8_t    cql_stream_id_t;

enum cql_opcode_enum {
    CQL_OPCODE_ERROR        = 0x00,
    CQL_OPCODE_STARTUP      = 0x01,
    CQL_OPCODE_READY        = 0x02,
    CQL_OPCODE_AUTHENTICATE = 0x03,
    CQL_OPCODE_CREDENTIALS  = 0x04,
    CQL_OPCODE_OPTIONS      = 0x05,
    CQL_OPCODE_SUPPORTED    = 0x06,
    CQL_OPCODE_QUERY        = 0x07,
    CQL_OPCODE_RESULT       = 0x08,
    CQL_OPCODE_PREPARE      = 0x09,
    CQL_OPCODE_EXECUTE      = 0x0A,
    CQL_OPCODE_REGISTER     = 0x0B,
    CQL_OPCODE_EVENT        = 0x0C
};

enum cql_consistency_enum {
    CQL_CONSISTENCY_ANY          = 0x0000,
    CQL_CONSISTENCY_ONE          = 0x0001,
    CQL_CONSISTENCY_TWO          = 0x0002,
    CQL_CONSISTENCY_THREE        = 0x0003,
    CQL_CONSISTENCY_QUORUM       = 0x0004,
    CQL_CONSISTENCY_ALL          = 0x0005,
    CQL_CONSISTENCY_LOCAL_QUORUM = 0x0006,
    CQL_CONSISTENCY_EACH_QUORUM  = 0x0007,
	CQL_CONSISTENCY_DEFAULT = CQL_CONSISTENCY_ONE
};

enum cql_column_type_enum {
    CQL_COLUMN_TYPE_UNKNOWN   = 0xFFFF,
    CQL_COLUMN_TYPE_CUSTOM    = 0x0000,
    CQL_COLUMN_TYPE_ASCII     = 0x0001,
    CQL_COLUMN_TYPE_BIGINT    = 0x0002,
    CQL_COLUMN_TYPE_BLOB      = 0x0003,
    CQL_COLUMN_TYPE_BOOLEAN   = 0x0004,
    CQL_COLUMN_TYPE_COUNTER   = 0x0005,
    CQL_COLUMN_TYPE_DECIMAL   = 0x0006,
    CQL_COLUMN_TYPE_DOUBLE    = 0x0007,
    CQL_COLUMN_TYPE_FLOAT     = 0x0008,
    CQL_COLUMN_TYPE_INT       = 0x0009,
    CQL_COLUMN_TYPE_TEXT      = 0x000A,
    CQL_COLUMN_TYPE_TIMESTAMP = 0x000B,
    CQL_COLUMN_TYPE_UUID      = 0x000C,
    CQL_COLUMN_TYPE_VARCHAR   = 0x000D,
    CQL_COLUMN_TYPE_VARINT    = 0x000E,
    CQL_COLUMN_TYPE_TIMEUUID  = 0x000F,
    CQL_COLUMN_TYPE_INET      = 0x0010,
    CQL_COLUMN_TYPE_LIST      = 0x0020,
    CQL_COLUMN_TYPE_MAP       = 0x0021,
    CQL_COLUMN_TYPE_SET       = 0x0022
};

enum cql_log_level_enum {
    CQL_LOG_CRITICAL = 0x00,
    CQL_LOG_ERROR    = 0x01,
    CQL_LOG_INFO     = 0x02,
    CQL_LOG_DEBUG    = 0x03
};

enum cql_result_type_enum {
    CQL_RESULT_VOID          = 0x0001,
    CQL_RESULT_ROWS          = 0x0002,
    CQL_RESULT_SET_KEYSPACE  = 0x0003,
    CQL_RESULT_PREPARED      = 0x0004,
    CQL_RESULT_SCHEMA_CHANGE = 0x0005
};

enum cql_event_enum {
    CQL_EVENT_TYPE_UNKNOWN  = 0x00,
    CQL_EVENT_TYPE_TOPOLOGY = 0x01,
    CQL_EVENT_TYPE_STATUS   = 0x02,
    CQL_EVENT_TYPE_SCHEMA   = 0x03
};

enum cql_event_schema_enum {
    CQL_EVENT_SCHEMA_UNKNOWN = 0x00,
    CQL_EVENT_SCHEMA_CREATED = 0x01,
    CQL_EVENT_SCHEMA_DROPPED = 0x02,
    CQL_EVENT_SCHEMA_UPDATED = 0x03
};

enum cql_event_status_enum {
    CQL_EVENT_STATUS_UNKNOWN = 0x00,
    CQL_EVENT_STATUS_UP      = 0x01,
    CQL_EVENT_STATUS_DOWN    = 0x02
};

enum cql_event_topology_enum {
    CQL_EVENT_TOPOLOGY_UNKNOWN     = 0x00,
    CQL_EVENT_TOPOLOGY_ADD_NODE    = 0x01,
    CQL_EVENT_TOPOLOGY_REMOVE_NODE = 0x02
};

//  The distance to a Cassandra node as assigned by a
//  com.datastax.driver.core.policies.LoadBalancingPolicy (through
//  its *distance method). The distance assigned to an host
//  influence how many connections the driver maintains towards this host. If for
//  a given host the assigned HostDistance is CQL_HOST_DISTANCE_LOCAL or
//  CQL_HOST_DISTANCE_REMOTE, some connections will be maintained by the driver to
//  this host. More active connections will be kept to CQL_HOST_DISTANCE_LOCAL host
//  than to a CQL_HOST_DISTANCE_REMOTE one (and thus well behaving
//  cql_load_balancing_policy_t should assign a CQL_HOST_DISTANCE_REMOTE distance
//  only to hosts that are the less often queried). However, if an host is
//  assigned the distance CQL_HOST_DISTANCE_IGNORE, no connection to that host will
//  maintained active. In other words, CQL_HOST_DISTANCE_IGNORE should be assigned to
//  hosts that should not be used by this driver (because they are in a remote
//  data center for instance).
enum cql_host_distance_enum {
	CQL_HOST_DISTANCE_LOCAL,
	CQL_HOST_DISTANCE_REMOTE,
    // host ignored by driver (no connection allowed)
	CQL_HOST_DISTANCE_IGNORE
};

const char*
to_string(const cql_host_distance_enum);

const char*
to_string(const cql_consistency_enum);

// Initializes cql library, must be called before any interaction
// with library.
// This function MUST be called only once.
// This function is NOT thread safe.
CQL_EXPORT void cql_initialize();

// Terminates cql library.
// This function must be called at program end, this MUST
// be called only once.
CQL_EXPORT void cql_terminate();

} // namespace cql
#endif // __CQL_H_INCLUDED__
