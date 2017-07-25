/*
  Copyright (c) 2016-2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "embedded_ads.hpp"

namespace test {

// Initialize static variables
uv_process_t EmbeddedADS::process_;
uv_mutex_t EmbeddedADS::mutex_;
std::string EmbeddedADS::configuration_directory_ = "";
std::string EmbeddedADS::configuration_file_ = "";
std::string EmbeddedADS::cassandra_keytab_file_ = "";
std::string EmbeddedADS::dse_keytab_file_ = "";
std::string EmbeddedADS::dseuser_keytab_file_ = "";
std::string EmbeddedADS::unknown_keytab_file_ = "";
std::string EmbeddedADS::bob_keytab_file_ = "";
std::string EmbeddedADS::bill_keytab_file_ = "";
std::string EmbeddedADS::charlie_keytab_file_ = "";
std::string EmbeddedADS::steve_keytab_file_ = "";
bool EmbeddedADS::is_initialized_ = false;

} // namespace test
