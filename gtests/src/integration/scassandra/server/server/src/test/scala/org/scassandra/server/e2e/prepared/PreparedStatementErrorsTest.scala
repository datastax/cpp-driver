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
package org.scassandra.server.e2e.prepared

import com.datastax.driver.core.{WriteType, ConsistencyLevel}
import com.datastax.driver.core.exceptions.{ReadTimeoutException, UnavailableException, WriteTimeoutException}
import dispatch.Defaults._
import dispatch._
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.ScalaFutures
import org.scassandra.server.priming._
import org.scassandra.server.priming.json._
import org.scassandra.server.priming.prepared.{ThenPreparedSingle, WhenPrepared}
import org.scassandra.server.{AbstractIntegrationTest, PrimingHelper}

class PreparedStatementErrorsTest extends AbstractIntegrationTest with BeforeAndAfter with ScalaFutures {

  before {
    val svc = url("http://localhost:8043/prime-prepared-single").DELETE
    val response = Http(svc OK as.String)
    response()
  }

  test("Prepared statement with priming - read_request_timeout") {
    val preparedStatementText: String = "select * from people where name = ?"
    val properties = Map[String, String](
      ErrorConstants.ReceivedResponse -> "2",
      ErrorConstants.RequiredResponse -> "3",
      ErrorConstants.DataPresent -> "true")
    val consistency = ConsistencyLevel.TWO

    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
      ThenPreparedSingle(None, result = Some(ReadTimeout), config = Some(properties))
    )
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris")
    boundStatement.setConsistencyLevel(consistency)

    //when
    val exception = intercept[ReadTimeoutException] {
      session.execute(boundStatement)
    }

    exception.getConsistencyLevel should equal(consistency)
    exception.getReceivedAcknowledgements should equal(2)
    exception.getRequiredAcknowledgements should equal(3)
    exception.wasDataRetrieved() should equal(true)
  }

  test("Prepared statement with priming - write_request_timeout") {
    val preparedStatementText: String = "select * from people where name = ?"
    val properties = Map[String, String](
      ErrorConstants.ReceivedResponse -> "2",
      ErrorConstants.RequiredResponse -> "3",
      ErrorConstants.WriteType -> "BATCH")
    val consistency = ConsistencyLevel.EACH_QUORUM

    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
      ThenPreparedSingle(None, result = Some(WriteTimeout), config = Some(properties))
    )

    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris")
    boundStatement.setConsistencyLevel(consistency)


    val exception = intercept[WriteTimeoutException] {
      session.execute(boundStatement)
    }

    exception.getConsistencyLevel should equal(consistency)
    exception.getReceivedAcknowledgements should equal(2)
    exception.getRequiredAcknowledgements should equal(3)
    exception.getWriteType should equal(WriteType.BATCH)
  }

  test("Prepared statement with priming - unavailable") {
    //given
    val properties = Map[String, String](
      ErrorConstants.Alive -> "2",
      ErrorConstants.RequiredResponse -> "3")
    val preparedStatementText: String = "select * from people where name = ?"
    val consistency = ConsistencyLevel.LOCAL_ONE
    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
      ThenPreparedSingle(None, result = Some(Unavailable), config = Some(properties))
    )
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris")
    boundStatement.setConsistencyLevel(consistency)

    //when
    val exception = intercept[UnavailableException] {
      session.execute(boundStatement)
    }

    //then
    exception.getConsistencyLevel should equal(consistency)
    exception.getRequiredReplicas should equal(3)
    exception.getAliveReplicas should equal(2)
  }
}
