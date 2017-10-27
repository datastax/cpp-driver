/*
 * Copyright (C) 2016 Christopher Batey and Dogan Narinc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra.server.e2e.query

import com.datastax.driver.core.exceptions.{WriteTimeoutException, ReadTimeoutException, UnavailableException}
import com.datastax.driver.core.{WriteType, ConsistencyLevel, SimpleStatement}
import org.scassandra.server.AbstractIntegrationTest
import org.scassandra.server.priming.ErrorConstants
import org.scassandra.server.priming.json._
import org.scassandra.server.priming.query.{Then, When}

class PrimingErrorsTest extends AbstractIntegrationTest {

  test("ReadTimeout: should return the consistency passed ini") {
    prime(When(queryPattern = Some(".*")), Then(None, result = Some(ReadTimeout)))
    val statement = new SimpleStatement("select * from some_table")
    val consistency: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM
    statement.setConsistencyLevel(consistency)

    val exception = intercept[ReadTimeoutException] {
      session.execute(statement)
    }

    exception.getConsistencyLevel should equal(consistency)
  }

  test("WriteTimeout: should return the consistency passed ini") {
    prime(When(queryPattern = Some(".*")), Then(None, result = Some(WriteTimeout)))
    val statement = new SimpleStatement("insert into something")
    val consistency: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM
    statement.setConsistencyLevel(consistency)

    val exception = intercept[WriteTimeoutException] {
      session.execute(statement)
    }

    exception.getConsistencyLevel should equal(consistency)
  }

  test("Unavailable: should return the consistency passed ini") {
    prime(When(queryPattern = Some(".*")), Then(None, result = Some(Unavailable)))
    val statement = new SimpleStatement("insert into something")
    val consistency: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM
    statement.setConsistencyLevel(consistency)

    val exception = intercept[UnavailableException] {
      session.execute(statement)
    }

    exception.getConsistencyLevel should equal(consistency)
  }

  test("ReadTimeout: with required / received and data present set") {
    val properties = Map[String, String](
      ErrorConstants.ReceivedResponse -> "2",
      ErrorConstants.RequiredResponse -> "3",
      ErrorConstants.DataPresent -> "true")

    prime(When(queryPattern = Some(".*")), Then(None, result = Some(ReadTimeout), config = Some(properties)))

    val statement = new SimpleStatement("select * from some_table")
    val consistency: ConsistencyLevel = ConsistencyLevel.ALL
    statement.setConsistencyLevel(consistency)

    val exception = intercept[ReadTimeoutException] {
      session.execute(statement)
    }

    exception.getConsistencyLevel should equal(consistency)
    exception.getReceivedAcknowledgements should equal(2)
    exception.getRequiredAcknowledgements should equal(3)
    exception.wasDataRetrieved() should equal(true)
  }

  test("WriteTimeout: with required / received and data present set") {
    val properties = Map[String, String](
      ErrorConstants.ReceivedResponse -> "2",
      ErrorConstants.RequiredResponse -> "3",
      ErrorConstants.WriteType -> "BATCH")

    prime(When(queryPattern = Some(".*")), Then(None, result = Some(WriteTimeout), config = Some(properties)))

    val statement = new SimpleStatement("select * from some_table")
    val consistency: ConsistencyLevel = ConsistencyLevel.ALL
    statement.setConsistencyLevel(consistency)

    val exception = intercept[WriteTimeoutException] {
      session.execute(statement)
    }

    exception.getConsistencyLevel should equal(consistency)
    exception.getReceivedAcknowledgements should equal(2)
    exception.getRequiredAcknowledgements should equal(3)
    exception.getWriteType should equal(WriteType.BATCH)
  }

  test("Unavailable: with alive and required sett") {
    val properties = Map[String, String](
      ErrorConstants.Alive -> "2",
      ErrorConstants.RequiredResponse -> "3")
    prime(When(queryPattern = Some(".*")), Then(None, result = Some(Unavailable), config = Some(properties)))
    val statement = new SimpleStatement("select * from some_table")
    val consistency: ConsistencyLevel = ConsistencyLevel.ALL
    statement.setConsistencyLevel(consistency)

    val exception = intercept[UnavailableException] {
      session.execute(statement)
    }

    exception.getConsistencyLevel should equal(consistency)
    exception.getRequiredReplicas should equal(3)
    exception.getAliveReplicas should equal(2)
  }
}
