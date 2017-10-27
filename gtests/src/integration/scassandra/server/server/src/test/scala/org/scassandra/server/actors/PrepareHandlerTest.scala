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
/*
* Copyright (C) 2014 Christopher Batey and Dogan Narinc
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

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKitBase}
import akka.util.Timeout
import org.mockito.ArgumentCaptor
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.messages.{ColumnSpecWithoutTable, NoRowMetadata, PreparedMetadata, RowMetadata}
import org.scassandra.codec.{Prepare, Prepared}
import org.scassandra.server.actors.PrepareHandler.{PreparedStatementQuery, PreparedStatementResponse}
import org.scassandra.server.priming._
import org.scassandra.server.priming.prepared.PrimePreparedStore
import org.scassandra.server.priming.query.Reply
import scodec.bits.ByteVector

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps


class PrepareHandlerTest extends FunSuite with ProtocolActorTest with ImplicitSender with Matchers with TestKitBase
  with BeforeAndAfter with MockitoSugar {
  implicit lazy val system = ActorSystem()

  var underTest: ActorRef = null
  val activityLog: ActivityLog = new ActivityLog
  val primePreparedStore = mock[PrimePreparedStore]

  def anyFunction() = any[Function2[PreparedMetadata, RowMetadata, Prepared]]

  val id = ByteVector(1)

  implicit val atMost: Duration = 1 seconds
  implicit val timeout: Timeout = 1 seconds

  before {
    reset(primePreparedStore)
    when(primePreparedStore.apply(any(classOf[Prepare]), anyFunction())).thenReturn(None)
    underTest = TestActorRef(new PrepareHandler(primePreparedStore, activityLog))

    receiveWhile(10 milliseconds) {
      case msg @ _ =>
    }
  }

  test("Should return prepared message on prepare - no params") {
    underTest ! protocolMessage(Prepare("select * from something"))

    expectMsgPF() {
      case ProtocolResponse(_, Prepared(_, PreparedMetadata(Nil, Some("keyspace"), Some("table"), Nil), NoRowMetadata)) => true
    }
  }

  test("Should return prepared message on prepare - single param") {
    underTest ! protocolMessage(Prepare("select * from something where name = ?"))

    expectMsgPF() {
      case ProtocolResponse(_, Prepared(_, PreparedMetadata(Nil, Some("keyspace"), Some("table"),
        List(ColumnSpecWithoutTable("0", DataType.Varchar))), NoRowMetadata)) => true
    }
  }

  test("Priming variable types - Should use types from Prime") {
    val query = "select * from something where name = ?"
    val prepare = Prepare("select * from something where name = ?")
    val prepared = Prepared(id, PreparedMetadata(Nil, Some("keyspace"), Some("table"),
      List(ColumnSpecWithoutTable("0", DataType.Int))))

    when(primePreparedStore.apply(any(classOf[Prepare]), any[Function2[PreparedMetadata, RowMetadata, Prepared]]))
      .thenReturn(Some(Reply(prepared)))

    underTest ! protocolMessage(Prepare("select * from something where name = ?"))

    val prepareCaptor = ArgumentCaptor.forClass(classOf[Prepare])

    verify(primePreparedStore).apply(prepareCaptor.capture(), anyFunction())

    prepareCaptor.getValue() shouldEqual prepare
  }

  test("Should use incrementing ids") {
    var lastId: Int = -1
    for (i <- 1 to 10) {
      val query = s"select * from something where name = ? and i = $i"

      underTest ! protocolMessage(Prepare(query))

      // The id returned should always be greater than the last id returned.
      expectMsgPF() {
        case ProtocolResponse(_, Prepared(id, _, _)) if id.toInt() > lastId =>
          lastId = id.toInt()
          true
      }
    }
  }

  test("Should record preparation in activity log") {
    activityLog.clearPreparedStatementPreparations()
    val query = "select * from something where name = ?"

    underTest ! protocolMessage(Prepare(query))

    activityLog.retrievePreparedStatementPreparations().size should equal(1)
    activityLog.retrievePreparedStatementPreparations().head should equal(PreparedStatementPreparation(query))
  }

  test("Should answer queries for prepared statement - not exist") {
    val response = (underTest ? PreparedStatementQuery(List(1))).mapTo[PreparedStatementResponse]

    Await.result(response, atMost) should equal(PreparedStatementResponse(Map()))
  }

  test("Should answer queries for prepared statement - exists") {
    val query = "select * from something where name = ?"
    val prepared = Prepared(id, PreparedMetadata(Nil, Some("keyspace"), Some("table"),
      List(ColumnSpecWithoutTable("0", DataType.Int))))
    when(primePreparedStore.apply(any(classOf[Prepare]), any[Function2[PreparedMetadata, RowMetadata, Prepared]]))
      .thenReturn(Some(Reply(prepared)))

    underTest ! protocolMessage(Prepare(query))

    val response = (underTest ? PreparedStatementQuery(List(id.toInt()))).mapTo[PreparedStatementResponse]

    Await.result(response, atMost) should equal(PreparedStatementResponse(Map(id.toInt() -> (query, prepared))))
  }
}
