/*
  Copyright (c) 2014-2016 DataStax
*/
#include "dse_integration.hpp"

DseIntegration::DseIntegration()
  : Integration()
  , dse_session_() {}

void DseIntegration::SetUp() {
  // Call the parent setup class
  Integration::SetUp();

  // Create the DSE session object from the Cassandra session object
  dse_session_ = session_;
}

void DseIntegration::connect(Cluster cluster) {
  // Call the parent connect function
  Integration::connect(cluster);
  dse_session_ = session_;
}

void DseIntegration::connect() {
  // Call the parent connect function
  Integration::connect();
  dse_session_ = session_;
}
