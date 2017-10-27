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
package org.scassandra.server.actors

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestActorRef, TestKitBase, TestProbe}
import akka.util.Timeout
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scassandra.codec._
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.messages.{ColumnSpecWithoutTable, PreparedMetadata, QueryParameters, Row}
import org.scassandra.server.actors.PrepareHandler.{PreparedStatementQuery, PreparedStatementResponse}
import org.scassandra.server.priming._
import org.scassandra.server.priming.prepared.PrimePreparedStore
import org.scassandra.server.priming.query.Reply
import scodec.bits.ByteVector

import scala.concurrent.duration._
import scala.language.postfixOps

class ExecuteHandlerTest extends FunSuite with ProtocolActorTest with Matchers with TestKitBase with ImplicitSender
  with BeforeAndAfter with MockitoSugar {
  implicit lazy val system = ActorSystem()
  implicit val protocolVersion = ProtocolVersion.latest

  var underTest: ActorRef = null
  var prepareHandlerTestProbe: TestProbe = _
  val activityLog: ActivityLog = new ActivityLog
  val primePreparedStore = mock[PrimePreparedStore]
  val stream: Byte = 0x3

  implicit val atMost: Duration = 1 seconds
  implicit val timeout: Timeout = 1 seconds

  val preparedId = 1
  val preparedIdBytes = ByteVector(preparedId)

  before {
    reset(primePreparedStore)
    when(primePreparedStore.apply(any(classOf[String]), any(classOf[Execute]))(any[ProtocolVersion])).thenReturn(None)
    prepareHandlerTestProbe = TestProbe()
    underTest = TestActorRef(new ExecuteHandler(primePreparedStore, activityLog, prepareHandlerTestProbe.ref))
    activityLog.clearPreparedStatementExecutions()

    receiveWhile(10 milliseconds) {
      case msg @ _ =>
    }
  }

  test("Should return empty result message for execute - no params") {
    val execute = Execute(preparedIdBytes)

    underTest ! protocolMessage(execute)

    prepareHandlerTestProbe.expectMsg(PreparedStatementQuery(List(preparedId)))
    prepareHandlerTestProbe.reply(PreparedStatementResponse(Map(preparedId -> ("Some query", Prepared(preparedIdBytes)))))

    expectMsgPF() {
      case ProtocolResponse(_, VoidResult) => true
    }
  }

  test("Should look up prepared prime in store with consistency & query") {
    val consistency = Consistency.THREE
    val query = "select * from something where name = ?"
    val prepared = Prepared(preparedIdBytes)
    val execute = Execute(preparedIdBytes, QueryParameters(consistency=consistency))

    underTest ! protocolMessage(execute)

    prepareHandlerTestProbe.expectMsg(PreparedStatementQuery(List(preparedId)))
    prepareHandlerTestProbe.reply(PreparedStatementResponse(Map(preparedId -> (query, prepared))))
    verify(primePreparedStore).apply(query, execute)
  }

  test("Should create rows message if prime matches") {
    val query = "select * from something where name = ?"
    val id = 1
    val rows = Rows(rows = Row("a" -> 1, "b" -> 2) :: Nil)
    val primeMatch = Some(Reply(rows))
    when(primePreparedStore.apply(any[String], any[Execute])(any[ProtocolVersion])).thenReturn(primeMatch)

    val execute = Execute(preparedIdBytes)
    underTest ! protocolMessage(execute)

    prepareHandlerTestProbe.expectMsg(PreparedStatementQuery(List(preparedId)))
    prepareHandlerTestProbe.reply(PreparedStatementResponse(Map(preparedId -> (query, Prepared(preparedIdBytes)))))

    expectMsgPF() {
      case ProtocolResponse(_, `rows`) => true
    }
  }

  test("Should record execution in activity log") {
    val query = "select * from something where name = ?"
    val id = 1
    val rows = Rows(rows = Row("a" -> 1, "b" -> 2) :: Nil)
    val primeMatch = Some(Reply(rows))
    val consistency = Consistency.TWO
    when(primePreparedStore.apply(any[String], any[Execute])(any[ProtocolVersion])).thenReturn(primeMatch)

    val values = List(10)
    val variableTypes = List(DataType.Bigint)
    val execute = Execute(preparedIdBytes, parameters = QueryParameters(consistency = consistency,
      values = Some(values.map(v => QueryValue(None, Bytes(DataType.Bigint.codec.encode(v).require.bytes))))))
    underTest ! protocolMessage(execute)

    prepareHandlerTestProbe.expectMsg(PreparedStatementQuery(List(preparedId)))
    prepareHandlerTestProbe.reply(PreparedStatementResponse(Map(preparedId -> (query, Prepared(preparedIdBytes,
      preparedMetadata = PreparedMetadata(keyspace = Some("keyspace"), table = Some("table"),
        columnSpec = ColumnSpecWithoutTable("0", DataType.Bigint) :: Nil))))))

    activityLog.retrievePreparedStatementExecutions().size should equal(1)
    activityLog.retrievePreparedStatementExecutions().head should equal(PreparedStatementExecution(query, consistency, None, values, variableTypes, None))
  }

  test("Should record execution in activity log without variables when variables don't match prime") {
    val stream: Byte = 0x02
    val query = "select * from something where name = ? and something = ?"
    val preparedStatementId = 1
    val consistency = Consistency.TWO
    val primeMatch = Some(Reply(NoRows))
    when(primePreparedStore.apply(any[String], any[Execute])(any[ProtocolVersion])).thenReturn(primeMatch)


    // Execute statement with two BigInt variables.
    val variables = List(10, 20)
    val execute = Execute(preparedIdBytes, parameters = QueryParameters(consistency = consistency,
      serialConsistency = Some(Consistency.SERIAL),
      values = Some(variables.map(v => QueryValue(None, Bytes(DataType.Bigint.codec.encode(v).require.bytes)))),
      timestamp = Some(1000)
    ))

    underTest ! protocolMessage(execute)

    // The Prepared Prime expects 1 column with Varchar.
    prepareHandlerTestProbe.expectMsg(PreparedStatementQuery(List(preparedId)))
    prepareHandlerTestProbe.reply(PreparedStatementResponse(Map(preparedId -> (query, Prepared(preparedIdBytes,
      preparedMetadata = PreparedMetadata(keyspace = Some("keyspace"), table = Some("table"),
        columnSpec = ColumnSpecWithoutTable("0", DataType.Varchar) :: Nil))))))

    // The execution should still be recorded, but the variables not included.
    activityLog.retrievePreparedStatementExecutions().size should equal(1)
    activityLog.retrievePreparedStatementExecutions().head should equal(PreparedStatementExecution(query, consistency,
      Some(Consistency.SERIAL), List(), List(), Some(1000)))
  }

  test("Should record execution in activity log event if not primed") {
    // given - Prime store returns None.
    when(primePreparedStore.apply(any[String], any[Execute])(any[ProtocolVersion])).thenReturn(None)

    // when - Executing a query.
    val query = "Some query"
    val consistency = Consistency.TWO

    val execute = Execute(preparedIdBytes, QueryParameters(consistency=consistency))
    underTest ! protocolMessage(execute)

    prepareHandlerTestProbe.expectMsg(PreparedStatementQuery(List(preparedId)))
    prepareHandlerTestProbe.reply(PreparedStatementResponse(Map(preparedId -> (query, Prepared(preparedIdBytes)))))

    // then - activity log should record an execution even though there was no prime.
    activityLog.retrievePreparedStatementExecutions().size should equal(1)
    activityLog.retrievePreparedStatementExecutions().head should equal(PreparedStatementExecution(query, consistency, None, List(), List(), None))
  }

  test("Should return unprepared response if not prepared statement not found") {
    val errMsg = s"Could not find prepared statement with id: 0x${preparedIdBytes.toHex}"
    val consistency = Consistency.TWO

    val execute = Execute(preparedIdBytes, QueryParameters(consistency=consistency))
    underTest ! protocolMessage(execute)

    // when - no prepared statement is found

    prepareHandlerTestProbe.expectMsg(PreparedStatementQuery(List(preparedId)))
    prepareHandlerTestProbe.reply(PreparedStatementResponse(Map()))

    // then - recorded execution indicating no prepared statement was found and an Unprepared response emitted.

    activityLog.retrievePreparedStatementExecutions().size should equal(1)
    activityLog.retrievePreparedStatementExecutions().head should equal(PreparedStatementExecution(errMsg, consistency, None, List(), List(), None))

    expectMsgPF() {
      case ProtocolResponse(_, Unprepared(`errMsg`, `preparedIdBytes`)) => true
    }
  }

  test("Should delay message if fixedDelay primed") {

    val query = "select * from something where name = ?"
    val prime = Reply(VoidResult, fixedDelay = Some(FiniteDuration(1500, TimeUnit.MILLISECONDS)))
    when(primePreparedStore.apply(any[String], any[Execute])(any[ProtocolVersion])).thenReturn(Some(prime))

    val execute = Execute(preparedIdBytes)
    underTest ! protocolMessage(execute)

    prepareHandlerTestProbe.expectMsg(PreparedStatementQuery(List(preparedId)))
    prepareHandlerTestProbe.reply(PreparedStatementResponse(Map(preparedId -> (query, Prepared(preparedIdBytes)))))

    expectMsgPF() {
      case ProtocolResponse(_, VoidResult) => true
    }
  }
}
