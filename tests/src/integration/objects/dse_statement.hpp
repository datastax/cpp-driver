/*
  Copyright (c) DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#ifndef __TEST_DSE_STATEMENT_HPP__
#define __TEST_DSE_STATEMENT_HPP__
#include "objects/statement.hpp"

namespace test {
namespace driver {
namespace dse {

/**
 * Wrapped DSE statement object
 */
class Statement : public driver::Statement {
public:
  /**
   * Create the DSE statement object from the native driver statement object
   *
   * @param statement Native driver object
   */
  Statement(CassStatement* statement)
    : driver::Statement(statement) {}

  /**
   * Create the DSE statement object from the shared reference
   *
   * @param statement Shared reference
   */
  Statement(Ptr statement)
    : driver::Statement(statement) {}

  /**
   * Create the DSE statement object from a wrapped statement object
   *
   * @param statement Wrapped statement object
   */
  Statement(driver::Statement statement)
    : driver::Statement(statement) {}

  /**
   * Create the statement object from a query
   *
   * @param query Query to create statement from
   * @param parameter_count Number of parameters contained in the query
   *                        (default: 0)
   */
  Statement(const std::string& query, size_t parameter_count = 0)
    : driver::Statement(cass_statement_new(query.c_str(), parameter_count)) {}

  /**
   * Set the name of the user to execute the statement as
   *
   * @param name Name to execute the statement as
   */
  void set_execute_as(const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_statement_set_execute_as(get(), name.c_str()));
  }
};

/**
 * Wrapped DSE batch object
 */
class Batch : public driver::Batch {
public:
  /**
   * Create the batch object based on the type of batch statement to use
   *
   * @param batch_type Type of batch to create (default: Unlogged)
   */
  Batch(CassBatchType batch_type = CASS_BATCH_TYPE_UNLOGGED)
    : driver::Batch(cass_batch_new(batch_type)) {}

  /**
   * Create the batch object from the native driver batch object
   *
   * @param batch Native driver object
   */
  Batch(CassBatch* batch)
    : driver::Batch(batch) {}

  /**
   * Create the batch object from the shared reference
   *
   * @param batch Shared reference
   */
  Batch(Ptr batch)
    : driver::Batch(batch) {}

  /**
   * Create the DSE batch object from a wrapped batch object
   *
   * @param batch Wrapped batch object
   */
  Batch(driver::Batch batch)
    : driver::Batch(batch) {}

  /**
   * Set the name of the user to execute the batch statement as
   *
   * @param name Name to execute the batch statement as
   */
  void set_execute_as(const std::string& name) {
    ASSERT_EQ(CASS_OK, cass_batch_set_execute_as(get(), name.c_str()));
  }
};

} // namespace dse
} // namespace driver
} // namespace test

#endif // __TEST_DSE_STATEMENT_HPP__
