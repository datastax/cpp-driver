/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_DRIVER_UTILS_HPP__
#define __TEST_DRIVER_UTILS_HPP__

#include "cassandra.h"

#include <string>
#include <vector>

namespace test {
namespace driver {
namespace internals {

class Utils {
public:
  /**
   * Get the attempted hosts/addresses of the future (sorted)
   *
   * @param future Future to retrieve hosts/addresses from
   * @return Attempted hosts/Addresses (sorted)
   */
  static const std::vector<std::string> attempted_hosts(CassFuture* future);

  /**
   * Get the host/address of the future
   *
   * @param future Future to retrieve hosts/addresses from
   * @return Host/Address
   */
  static const std::string host(CassFuture* future);

  /**
   * Enable/Disable the recording of hosts attempted during the execution of
   * a statement
   *
   * @param statement Statement to enable/disable attempted host recording
   * @param enable True if attempted host should be recorded; false otherwise
   */
  static void set_record_attempted_hosts(CassStatement* statement, bool enable);

};

} // namespace internals
} // namespace driver
} // namespace test

#endif // __TEST_DRIVER_UTILS_HPP__