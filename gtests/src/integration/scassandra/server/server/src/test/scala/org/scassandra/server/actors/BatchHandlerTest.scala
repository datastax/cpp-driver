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

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKitBase, TestProbe}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scassandra.codec._
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.messages._
import org.scassandra.server.actors.PrepareHandler.{PreparedStatementQuery, PreparedStatementResponse}
import org.scassandra.server.priming._
import org.scassandra.server.priming.batch.PrimeBatchStore
import org.scassandra.server.priming.BatchQuery
import org.scassandra.server.priming.prepared.PreparedStoreLookup
import scodec.bits.ByteVector

class BatchHandlerTest extends FunSuite with ProtocolActorTest with TestKitBase with ImplicitSender with MockitoSugar
  with Matchers with BeforeAndAfter {

  implicit lazy val system = ActorSystem()

  var underTest: ActorRef = _
  var prepareHandlerProbe: TestProbe = _
  val activityLog = new ActivityLog
  val primeBatchStore = mock[PrimeBatchStore]
  val primePreparedStore = mock[PreparedStoreLookup]

  before {
    prepareHandlerProbe = TestProbe()
    underTest = system.actorOf(Props(classOf[BatchHandler], activityLog, prepareHandlerProbe.ref, primeBatchStore,
      primePreparedStore))
    activityLog.clearBatchExecutions()
    when(primeBatchStore.apply(any(classOf[BatchExecution]))).thenReturn(None)
  }

  test("Should send back void response by default") {
    val batch = Batch(BatchType.UNLOGGED, List(
      SimpleBatchQuery("insert into something"), SimpleBatchQuery("insert into something else")
    ))

    underTest ! protocolMessage(batch)

    expectMsgPF() {
      case ProtocolResponse(_, VoidResult) => true
    }
  }

  test("Records batch statement with ActivityLog") {
    val batch = Batch(BatchType.LOGGED, List(
      SimpleBatchQuery("insert into something"), SimpleBatchQuery("insert into something else")
    ), serialConsistency = Some(Consistency.LOCAL_SERIAL), timestamp = Some(8675309))

    underTest ! protocolMessage(batch)

    expectMsgPF() {
      case ProtocolResponse(_, VoidResult) => true
    }

    activityLog.retrieveBatchExecutions() should equal(List(
      BatchExecution(List(
        BatchQuery("insert into something", BatchQueryKind.Simple),
        BatchQuery("insert into something else", BatchQueryKind.Simple)
      ), Consistency.ONE, Some(Consistency.LOCAL_SERIAL), BatchType.LOGGED, Some(8675309)))
    )
  }

  test("Records batch statement with ActivityLog - prepared statements") {
    val id = 1
    val idBytes = ByteVector(id)

    implicit val protocolVersion = ProtocolVersion.latest

    val batch = Batch(BatchType.LOGGED, List(
      PreparedBatchQuery(idBytes, List(QueryValue(1, DataType.Int).value))
    ))

    underTest ! protocolMessage(batch)

    prepareHandlerProbe.expectMsg(PreparedStatementQuery(List(id)))

    val prepared = Prepared(
      idBytes,
      PreparedMetadata(Nil, Some("keyspace"), Some("table"), List(ColumnSpecWithoutTable("0", DataType.Int))))

    prepareHandlerProbe.reply(PreparedStatementResponse(Map(id -> ("insert into something", prepared))))

    expectMsgPF() {
      case ProtocolResponse(_, VoidResult) => true
    }

    activityLog.retrieveBatchExecutions() should equal(List(
      BatchExecution(List(
        BatchQuery("insert into something", BatchQueryKind.Prepared, List(1), List(DataType.Int))
      ), Consistency.ONE, None, BatchType.LOGGED, None))
    )
  }

  // this isn't expected to happen but let's do something better than a NoSuchElementException if it does
  test("Records batch statement with ActivityLog - prepared statement not exist") {
    val id = 1
    val idBytes = ByteVector(id)

    val batch = Batch(BatchType.LOGGED, List(
      PreparedBatchQuery(idBytes)
    ))

    underTest ! protocolMessage(batch)

    prepareHandlerProbe.expectMsg(PreparedStatementQuery(List(1)))
    prepareHandlerProbe.reply(PreparedStatementResponse(Map()))

    expectMsgPF() {
      case ProtocolResponse(_, VoidResult) => true
    }

    activityLog.retrieveBatchExecutions() should equal(List(
      BatchExecution(List(
        BatchQuery("A prepared statement was in the batch but couldn't be found - did you prepare against a different  session?", BatchQueryKind.Prepared)
      ), Consistency.ONE, None, BatchType.LOGGED, None))
    )
  }
}
