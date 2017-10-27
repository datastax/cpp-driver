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
package org.scassandra.server.priming

import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scassandra.codec.Consistency
import org.scassandra.codec.Consistency._
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.messages.BatchQueryKind.Simple
import org.scassandra.codec.messages.BatchType.LOGGED

class ActivityLogTest extends FunSuite with Matchers with BeforeAndAfter {

  var underTest = new ActivityLog

  before {
    underTest = new ActivityLog
  }

  test("Clear connection activity log") {
    underTest.recordConnection()
    underTest.clearConnections()
    underTest.retrieveConnections().size should equal(0)
  }

  test("Clear query activity log") {
    underTest.recordQuery("select * from people", ONE, None)
    underTest.clearQueries()
    underTest.retrieveQueries().size should equal(0)
  }

  test("No connections should exist by default") {
    underTest.retrieveConnections().size should equal(0)
  }

  test("Store connection and retrieve connection") {
    underTest.recordConnection()
    underTest.retrieveConnections().size should equal(1)
  }

  test("Store query and retrieve connection") {
    val query: String = "select * from people"
    underTest.recordQuery(query, ONE, None)
    underTest.retrieveQueries().size should equal(1)
    underTest.retrieveQueries().head.query should equal(query)
    underTest.retrieveQueries().head.consistency should equal(ONE)
  }
  
  test("Store primed statement and retrieve primed execution") {
    underTest.clearPreparedStatementExecutions()
    val preparedStatementText = "select * from people where name = ?"
    val variables = List("Chris")
    val consistency = ONE
    val variableTypes = List(DataType.Ascii, DataType.Bigint)

    underTest.recordPreparedStatementExecution(preparedStatementText, consistency, Some(Consistency.LOCAL_SERIAL), variables, variableTypes, Some(1))
    val preparedStatementRecord = underTest.retrievePreparedStatementExecutions()

    preparedStatementRecord.size should equal(1)
    preparedStatementRecord.head should equal(PreparedStatementExecution(preparedStatementText, consistency, Some(Consistency.LOCAL_SERIAL), variables, variableTypes, Some(1)))
  }

  test("Clear prepared statement execution log") {
    underTest.recordPreparedStatementExecution("anything", TWO, None, List(), List(), None)
    underTest.clearPreparedStatementExecutions()
    underTest.retrievePreparedStatementExecutions().size should equal(0)
  }

  test("Clear prepared statement preparations log") {
    underTest.clearPreparedStatementPreparations()
    underTest.recordPreparedStatementPreparation(PreparedStatementPreparation("anything"))
    underTest.clearPreparedStatementPreparations()
    underTest.retrievePreparedStatementExecutions().size should equal(0)
  }

  test("Store primed statement and retrieve prepared statements when not executed)") {
    underTest.clearPreparedStatementPreparations()
    val text = "select * from people where name = ?"

    underTest.recordPreparedStatementPreparation(PreparedStatementPreparation(text))
    val preparedStatementRecord = underTest.retrievePreparedStatementPreparations()

    preparedStatementRecord.size should equal(1)
    preparedStatementRecord.head should equal(PreparedStatementPreparation(text))
  }
  
  test("Records query parameters") {
    underTest.recordQuery("query", ONE, None, List("Hello"), List(DataType.Text))

    underTest.retrieveQueries() should equal(List(Query("query", ONE, None, List("Hello"), List(DataType.Text))))
  }

  test("Record batch execution") {
    val consistency: Consistency = ONE
    val statements: List[BatchQuery] = List(BatchQuery("select * from hello", Simple))
    val execution: BatchExecution = BatchExecution(statements, consistency, None, LOGGED, None)
    underTest.recordBatchExecution(execution)

    val executions: List[BatchExecution] = underTest.retrieveBatchExecutions()

    executions should equal(List(execution))
  }

  test("Clear batch execution") {
    underTest.recordBatchExecution(BatchExecution(List(BatchQuery("select * from hello", Simple)), ONE, None, LOGGED, None))

    underTest.clearBatchExecutions()

    underTest.retrieveBatchExecutions() should equal(List())
  }
}
