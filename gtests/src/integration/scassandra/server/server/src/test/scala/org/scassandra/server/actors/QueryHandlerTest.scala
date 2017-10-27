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
import akka.testkit._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scassandra.codec._
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.messages.{QueryParameters, Row}
import org.scassandra.server.priming.query.{PrimeQueryStore, Reply}
import org.scassandra.server.priming.{ActivityLog, Query => RQuery}

import scala.concurrent.duration._
import scala.language.postfixOps

class QueryHandlerTest extends FunSuite with ImplicitSender with ProtocolActorTest with Matchers with BeforeAndAfter
  with TestKitBase with MockitoSugar {
  implicit lazy val system = ActorSystem()
  implicit val protocolVersion = ProtocolVersion.latest

  var underTest: ActorRef = null
  var testProbeForTcpConnection: TestProbe = null
  val mockPrimedResults = mock[PrimeQueryStore]
  val someCqlStatement = Query("some cql statement", QueryParameters(consistency=Consistency.ONE))
  val activityLog = new ActivityLog

  before {
    activityLog.clearQueries()
    underTest = TestActorRef(new QueryHandler(mockPrimedResults, activityLog))
    reset(mockPrimedResults)

    receiveWhile(10 milliseconds) {
      case msg @ _ =>
    }
  }

  test("Should return empty result when PrimeQueryStore returns None") {
    when(mockPrimedResults.apply(someCqlStatement)).thenReturn(None)

    underTest ! protocolMessage(someCqlStatement)

    expectMsgPF() {
      case ProtocolResponse(_, `NoRows`) => true
    }
  }

  test("Should return Prime message when PrimeQueryStore returns Prime") {
    val rows = Rows(rows = Row("a" -> 1, "b" -> 2) :: Nil)
    when(mockPrimedResults.apply(someCqlStatement)).thenReturn(Some(Reply(rows)))

    underTest ! protocolMessage(someCqlStatement)

    expectMsgPF() {
      case ProtocolResponse(_, `rows`) => true
    }
  }

  test("Should store query in the ActivityLog even if not primed") {
    //given
    activityLog.clearQueries()

    val queryText = "select * from people"
    val consistency = Consistency.TWO
    val query = Query(queryText, QueryParameters(consistency=consistency))
    when(mockPrimedResults.apply(query)).thenReturn(None)

    //when
    underTest ! protocolMessage(query)

    //then
    val recordedQueries = activityLog.retrieveQueries()
    recordedQueries.size should equal(1)
    val recordedQuery = recordedQueries.head
    recordedQuery should equal(RQuery(queryText, consistency, None))
  }

  test("Should record query parameter values from request in QueryLog if Prime contains variable types") {
    // given
    val consistency = Consistency.THREE

    val variableTypes: List[DataType] = DataType.Varchar :: DataType.Int :: Nil
    val values: List[Any] = "Hello" :: 42 :: Nil
    val rawValues: List[QueryValue] = values.zip(variableTypes).map {
      case (v, dataType) =>
        QueryValue(None, Bytes(dataType.codec.encode(v).require.toByteVector))
    }
    val query = Query("select * from sometable where k = ?", QueryParameters(consistency=consistency, values = Some(rawValues)))

    when(mockPrimedResults.apply(query)).thenReturn(Some(Reply(NoRows, variableTypes = Some(variableTypes))))

    // when
    underTest ! protocolMessage(query)

    // then
    expectMsgPF() {
      case ProtocolResponse(_, `NoRows`) => true
    }

    // check activityLog
    val recordedQueries = activityLog.retrieveQueries()
    recordedQueries.size should equal(1)
    val recordedQuery = recordedQueries.head
    recordedQuery should equal(RQuery(query.query, consistency, None, values, variableTypes))
  }
}