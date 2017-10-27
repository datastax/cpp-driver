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
package org.scassandra.server.priming.routes

import java.util.concurrent.TimeUnit

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.scassandra.codec.Consistency._
import org.scassandra.codec.datatype.DataType
import org.scassandra.server.priming._
import org.scassandra.server.priming.json._
import org.scassandra.server.priming.prepared._
import spray.http.StatusCodes
import spray.testkit.ScalatestRouteTest

import scala.concurrent.duration.FiniteDuration

class PrimingPreparedRouteTest extends FunSpec with Matchers with ScalatestRouteTest with PrimingPreparedRoute with MockitoSugar
 with BeforeAndAfter {

  import PrimingJsonImplicits._

  implicit def actorRefFactory = system
  implicit val primePreparedStore: PrimePreparedStore = mock[PrimePreparedStore]
  implicit val primePreparedPatternStore: PrimePreparedPatternStore = mock[PrimePreparedPatternStore]
  implicit val primePreparedMultiStore: PrimePreparedMultiStore = mock[PrimePreparedMultiStore]
  val primePreparedSinglePath = "/prime-prepared-single"
  val primePreparedMultiPath = "/prime-prepared-multi"

  before {
    reset(primePreparedStore)
    reset(primePreparedPatternStore)
    reset(primePreparedMultiStore)
  }

  describe("Priming multiple responses") {
    it("Should record it with the multi prime store") {
      val when: WhenPrepared = WhenPrepared(Some("select * from people where name = ?"))
      val thenDo = ThenPreparedMulti(Some(List(DataType.Text)), List(Outcome(Criteria(List(ExactMatch(Some("Chris")))), Action(None))))
      val prime = PrimePreparedMulti(when, thenDo)
      Post(primePreparedMultiPath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.OK)
        verify(primePreparedMultiStore).record(prime)
      }
    }

    it("Should allow primes to be deleted") {
      Delete(primePreparedMultiPath) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.OK)
        verify(primePreparedMultiStore).clear()
      }
    }

    //todo validation error if variable types length != outcome varaiable matcher length
  }

  describe("Priming") {
    it("Should take in query") {
      val when: WhenPrepared = WhenPrepared(Some("select * from people where name = ?"))
      val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
      val prime = PrimePreparedSingle(when, thenDo)
      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.OK)
        verify(primePreparedStore).record(prime)
      }
    }

    it("should store a fixedDelay") {
      val when: WhenPrepared = WhenPrepared(Some("select * from people where name = ?"))
      val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(List()), fixedDelay = Some(1500l))
      val prime = PrimePreparedSingle(when, thenDo)
      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.OK)
        verify(primePreparedStore).record(prime)
      }
    }

    it("Should use the pattern store if queryPattern specified") {
      val when: WhenPrepared = WhenPrepared(queryPattern = Some("select * from people where name = ?"))
      val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
      val prime = PrimePreparedSingle(when, thenDo)
      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.OK)
        verify(primePreparedPatternStore).record(prime)
      }
    }

    it("should allow primes to be deleted") {
      Delete(primePreparedSinglePath) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.OK)
        verify(primePreparedStore).clear()
        verify(primePreparedPatternStore).clear()
      }
    }
  }

  describe("Retrieving of primes") {
    it("should return empty list when there are no primes") {
      when(primePreparedStore.retrievePrimes()).thenReturn(Nil)

      Get(primePreparedSinglePath) ~> routeForPreparedPriming ~> check {
          responseAs[List[PrimePreparedSingle]].size should equal(0)
      }
    }

    it("should convert variable types in original Json Format") {
      val variableTypes = List(DataType.Varchar, DataType.Int)
      val existingPrimes : List[PrimePreparedSingle] = List(PrimePreparedSingle(WhenPrepared(), ThenPreparedSingle(None, variable_types = Some(variableTypes))))
      when(primePreparedStore.retrievePrimes()).thenReturn(existingPrimes)

      Get(primePreparedSinglePath) ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedSingle]]
        parsedResponse.size should equal(1)
        parsedResponse.head.thenDo.variable_types should equal(Some(variableTypes))
      }
    }

    it("should put query in original Json Format") {
      val query: String = "select * from people where name = ?"
      val existingPrimes : List[PrimePreparedSingle] = List(PrimePreparedSingle(WhenPrepared(Some(query)), ThenPreparedSingle(None)))
      when(primePreparedStore.retrievePrimes()).thenReturn(existingPrimes)

      Get(primePreparedSinglePath) ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedSingle]]
        parsedResponse.size should equal(1)
        parsedResponse.head.when.query should equal(Some(query) )
      }
    }

    it("should convert rows to the original Json Format") {
      val rows = List(Map("name" -> "Chris"))
      val existingPrimes : List[PrimePreparedSingle] = List(PrimePreparedSingle(WhenPrepared(), ThenPreparedSingle(Some(rows))))
      when(primePreparedStore.retrievePrimes()).thenReturn(existingPrimes)

      Get(primePreparedSinglePath) ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedSingle]]
        parsedResponse.size should equal(1)
        parsedResponse.head.thenDo.rows should equal(Some(rows))
      }
    }

    it("should convert column types to the original Json Format") {
      val columnTypes = Map("name" -> DataType.Varchar)
      val existingPrimes : List[PrimePreparedSingle] = List(PrimePreparedSingle(WhenPrepared(), ThenPreparedSingle(None, column_types = Some(columnTypes))))
      when(primePreparedStore.retrievePrimes()).thenReturn(existingPrimes)

      Get(primePreparedSinglePath) ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedSingle]]
        parsedResponse.size should equal(1)
        parsedResponse.head.thenDo.column_types should equal(Some(columnTypes))
      }
    }

    it("should convert result to the original Json Format") {
      val existingPrimes : List[PrimePreparedSingle] = List(PrimePreparedSingle(WhenPrepared(), ThenPreparedSingle(None, result = Some(ReadTimeout))))
      when(primePreparedStore.retrievePrimes()).thenReturn(existingPrimes)

      Get(primePreparedSinglePath) ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedSingle]]
        parsedResponse.size should equal(1)
        parsedResponse.head.thenDo.result should equal(Some(ReadTimeout))
      }
    }

    it("should convert outcome delay to the original Json Format") {
      val fixedDelay = Some(FiniteDuration(1500, TimeUnit.MILLISECONDS))
      val existingPrimes : List[PrimePreparedSingle] = List(PrimePreparedSingle(WhenPrepared(), ThenPreparedSingle(None, fixedDelay = fixedDelay.map(_.toMillis))))
      when(primePreparedStore.retrievePrimes()).thenReturn(existingPrimes)

      Get(primePreparedSinglePath) ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedSingle]]
        parsedResponse.size should equal(1)
        parsedResponse.head.thenDo.fixedDelay should equal(Some(fixedDelay.get.toMillis))
      }
    }

    it("should convert consistencies to the original Json Format") {
      val existingPrimes : List[PrimePreparedSingle] = List(PrimePreparedSingle(WhenPrepared(consistency=Some(List(ONE, TWO))), ThenPreparedSingle(None)))
      when(primePreparedStore.retrievePrimes()).thenReturn(existingPrimes)

      Get(primePreparedSinglePath) ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedSingle]]
        parsedResponse.size should equal(1)
        parsedResponse.head.when.consistency should equal(Some(List(ONE, TWO)))
      }
    }
  }

  describe("Priming errors") {
    it("Should convert Conflicting Primes to Bad Request") {
      when(primePreparedStore.record(any(classOf[PrimePreparedSingle]))).thenReturn(ConflictingPrimes(List()))

      val primeWhen: WhenPrepared = WhenPrepared(Some("select * from people where name = ?"))
      val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
      val prime = PrimePreparedSingle(primeWhen, thenDo)

      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.BadRequest)
      }
    }

    it("Should convert type mis match to Bad Request") {
      when(primePreparedStore.record(any(classOf[PrimePreparedSingle]))).thenReturn(TypeMismatches  (List()))

      val primeWhen: WhenPrepared = WhenPrepared(Some("select * from people where name = ?"))
      val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
      val prime = PrimePreparedSingle(primeWhen, thenDo)

      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.BadRequest)
      }
    }

    it("Should be Bad Request if both query and queryPattern specified") {
      val primeWhen: WhenPrepared = WhenPrepared(Some("select * from people where name = ?"), Some("Pattern as well"))
      val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
      val prime = PrimePreparedSingle(primeWhen, thenDo)

      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.BadRequest)
        responseAs[String] should equal("Must specify either query or queryPattern, not both")
      }
    }

    it("Should be Bad Request if neither query and queryPattern specified") {
      val primeWhen: WhenPrepared = WhenPrepared()
      val thenDo: ThenPreparedSingle = ThenPreparedSingle(Some(List()))
      val prime = PrimePreparedSingle(primeWhen, thenDo)

      Post(primePreparedSinglePath, prime) ~> routeForPreparedPriming ~> check {
        status should equal(StatusCodes.BadRequest)
        responseAs[String] should equal("Must specify either query or queryPattern, not both")
      }
    }
  }

  //todo test for specifying neither a query or queryPattern

  describe("Retrieving of multi primes") {
    it("should return empty list when there are no multi primes") {
      when(primePreparedMultiStore.retrievePrimes()).thenReturn(Nil)

      Get(primePreparedMultiPath) ~> routeForPreparedPriming ~> check {
        responseAs[List[PrimePreparedMulti]].size should equal(0)
      }
    }

    it("should convert variable types in original Json Format") {
      val variableTypes = List(DataType.Varchar, DataType.Int)
      val existingPrimes: List[PrimePreparedMulti] = List(PrimePreparedMulti(WhenPrepared(), ThenPreparedMulti(Some(variableTypes), Nil)))
      when(primePreparedMultiStore.retrievePrimes()).thenReturn(existingPrimes)

      Get(primePreparedMultiPath) ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedMulti]]
        parsedResponse.size should equal(1)
        parsedResponse.head.thenDo.variable_types should equal(Some(variableTypes))
      }
    }

    it("should put query in original Json Format") {
      val query: String = "select * from people where name = ?"
      val existingPrimes: List[PrimePreparedMulti] = List(PrimePreparedMulti(WhenPrepared(Some(query)), ThenPreparedMulti(None, Nil)))
      when(primePreparedMultiStore.retrievePrimes()).thenReturn(existingPrimes)

      Get(primePreparedMultiPath) ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedMulti]]
        parsedResponse.size should equal(1)
        parsedResponse.head.when.query should equal(Some(query))
      }
    }

    it("should convert outcome criteria variable matchers to the original Json Format") {
      val variableMatchers: List[VariableMatch] = List(AnyMatch, ExactMatch(Some("")))
      val outcomes: List[Outcome] = List(Outcome(Criteria(variableMatchers), Action(None)))
      val existingPrimes: List[PrimePreparedMulti] = List(PrimePreparedMulti(WhenPrepared(None), ThenPreparedMulti(None, outcomes)))
      when(primePreparedMultiStore.retrievePrimes()).thenReturn(existingPrimes)

      Get(primePreparedMultiPath) ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedMulti]]
        parsedResponse.size should equal(1)
        parsedResponse.head.thenDo.outcomes.size should equal(1)
        parsedResponse.head.thenDo.outcomes.head.criteria.variable_matcher should equal(variableMatchers)
      }
    }

    it("should convert outcome action rows to the original Json Format") {
      val rows = List(Map("name" -> "Chris"))
      val outcomes: List[Outcome] = List(Outcome(Criteria(Nil), Action(Some(rows))))
      val existingPrimes: List[PrimePreparedMulti] = List(PrimePreparedMulti(WhenPrepared(None), ThenPreparedMulti(None, outcomes)))
      when(primePreparedMultiStore.retrievePrimes()).thenReturn(existingPrimes)

      Get(primePreparedMultiPath) ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedMulti]]
        parsedResponse.head.thenDo.outcomes.size should equal(1)
        parsedResponse.head.thenDo.outcomes.head.action.rows should equal(Some(rows))
      }
    }

    it("should convert outcome action columns to the original Json Format") {
      val columnTypes = Map("name" -> DataType.Varchar)
      val outcomes: List[Outcome] = List(Outcome(Criteria(Nil), Action(None, column_types=Some(columnTypes))))
      val existingPrimes: List[PrimePreparedMulti] = List(PrimePreparedMulti(WhenPrepared(None), ThenPreparedMulti(None, outcomes)))
      when(primePreparedMultiStore.retrievePrimes()).thenReturn(existingPrimes)

      Get(primePreparedMultiPath) ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedMulti]]
        parsedResponse.head.thenDo.outcomes.size should equal(1)
        parsedResponse.head.thenDo.outcomes.head.action.column_types should equal(Some(columnTypes))
      }
    }

    it("should convert outcome result to the original Json Format") {
      val result = WriteTimeout
      val outcomes: List[Outcome] = List(Outcome(Criteria(Nil), Action(None, result=Some(result))))
      val existingPrimes: List[PrimePreparedMulti] = List(PrimePreparedMulti(WhenPrepared(None), ThenPreparedMulti(None, outcomes)))
      when(primePreparedMultiStore.retrievePrimes()).thenReturn(existingPrimes)

      Get(primePreparedMultiPath) ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedMulti]]
        parsedResponse.head.thenDo.outcomes.size should equal(1)
        parsedResponse.head.thenDo.outcomes.head.action.result should equal(Some(result))
      }
    }

    it("should convert outcome delay to the original Json Format") {
      val fixedDelay = Some(FiniteDuration(1500, TimeUnit.MILLISECONDS))
      val outcomes: List[Outcome] = List(Outcome(Criteria(Nil), Action(None, fixedDelay=fixedDelay.map(_.toMillis))))
      val existingPrimes: List[PrimePreparedMulti] = List(PrimePreparedMulti(WhenPrepared(None), ThenPreparedMulti(None, outcomes)))
      when(primePreparedMultiStore.retrievePrimes()).thenReturn(existingPrimes)

      Get(primePreparedMultiPath) ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedMulti]]
        parsedResponse.head.thenDo.outcomes.size should equal(1)
        parsedResponse.head.thenDo.outcomes.head.action.fixedDelay should equal(Some(fixedDelay.get.toMillis))
      }
    }

    it("should convert consistencies to the original Json Format") {
      val outcomes: List[Outcome] = List(Outcome(Criteria(Nil), Action(None)))
      val existingPrimes: List[PrimePreparedMulti] = List(PrimePreparedMulti(WhenPrepared(None, consistency=Some(List(ONE, TWO))), ThenPreparedMulti(None, outcomes)))
      when(primePreparedMultiStore.retrievePrimes).thenReturn(existingPrimes)

      Get(primePreparedMultiPath) ~> routeForPreparedPriming ~> check {
        val parsedResponse = responseAs[List[PrimePreparedSingle]]
        parsedResponse.size should equal(1)
        parsedResponse.head.when.consistency should equal(Some(List(ONE, TWO)))
      }
    }
  }
}
