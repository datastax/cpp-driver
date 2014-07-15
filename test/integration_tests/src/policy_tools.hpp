#pragma once

#include "address.hpp"
#include "test_utils.hpp"

struct PolicyTool {

  std::map<cass::Address, int> coordinators;

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

  void add_coordinator(cass::Address address);

  void assertQueried(cass::Address address, int n);

  void assertQueriedAtLeast(cass::Address address, int n);

  void query(CassSession* session, int n, CassConsistency cl);

  CassError query_return_error(CassSession* session,
                               int n,
                               CassConsistency cl);
};
