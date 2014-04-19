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

#ifndef __CQL_DEFINES_H_INCLUDED__
#define __CQL_DEFINES_H_INCLUDED__

#define CQL_FRAME_MAX_SIZE 1024 * 1024 * 256

#define CQL_VERSION_1_REQUEST  0x01
#define CQL_VERSION_1_RESPONSE 0x81

#define CQL_VERSION       "CQL_VERSION"
#define CQL_VERSION_IMPL  "3.0.0"
#define CQL_VERSION_3_0_0 "3.0.0"

#define CQL_COMPRESSION        "COMPRESSION"

#define CQL_RESULT_ROWS_FLAGS_GLOBAL_TABLES_SPEC 0x0001

#define CQL_EVENT_TOPOLOGY_CHANGE "TOPOLOGY_CHANGE"
#define CQL_EVENT_TOPOLOGY_CHANGE_NEW "NEW_NODE"
#define CQL_EVENT_TOPOLOGY_CHANGE_REMOVE "REMOVED_NODE"

#define CQL_EVENT_STATUS_CHANGE "STATUS_CHANGE"
#define CQL_EVENT_STATUS_CHANGE_UP "UP"
#define CQL_EVENT_STATUS_CHANGE_DOWN "DOWN"

#define CQL_EVENT_SCHEMA_CHANGE "SCHEMA_CHANGE"
#define CQL_EVENT_SCHEMA_CHANGE_CREATED "CREATED"
#define CQL_EVENT_SCHEMA_CHANGE_UPDATED "UPDATED"
#define CQL_EVENT_SCHEMA_CHANGE_DROPPED "DROPPED"

#endif // __CQL_DEFINES_H_INCLUDED__
