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

#pragma once

#include "test_utils.hpp"

struct PolicyTool {

  std::map<std::string, int> coordinators;

  void show_coordinators();	// show what queries went to what node IP.
  void reset_coordinators();

  void init(CassSession* session,
            int n,
            CassConsistency cl,
            bool batch = false);

  CassError init_return_error(CassSession* session,
                              int n,
                              CassConsistency cl,
                              bool batch = false);

  void create_schema(CassSession* session,
                     int replicationFactor);

  void create_schema(CassSession* session,
                     int dc1, int dc2);

  void drop_schema(CassSession* session);

  void add_coordinator(std::string address);

  void assert_queried(std::string address, int n);

  void assertQueriedAtLeast(std::string address, int n);

  void query(CassSession* session, int n, CassConsistency cl);

  CassError query_return_error(CassSession* session,
                               int n,
                               CassConsistency cl);
};
