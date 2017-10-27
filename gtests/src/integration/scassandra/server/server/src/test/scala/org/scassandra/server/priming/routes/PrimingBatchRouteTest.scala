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
package org.scassandra.server.priming.routes

import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FunSpec}
import org.scassandra.server.priming.batch.{BatchWhen, BatchPrimeSingle, PrimeBatchStore}
import org.scassandra.server.priming.json.{PrimingJsonImplicits, Success}
import org.scassandra.server.priming.query.Then
import spray.http.StatusCodes
import spray.testkit.ScalatestRouteTest

class PrimingBatchRouteTest extends FunSpec with Matchers with ScalatestRouteTest with PrimingBatchRoute with MockitoSugar {

  import PrimingJsonImplicits._

  implicit def actorRefFactory = system
  implicit val primeBatchStore: PrimeBatchStore = mock[PrimeBatchStore]
  val primeBatchSinglePath = "/prime-batch-single"

  describe("Priming") {
    it("Should store the prime in the prime store") {
      val when = BatchWhen(List())
      val thenDo = Then(rows = Some(List()), result = Some(Success))
      val prime = BatchPrimeSingle(when, thenDo)
      Post(primeBatchSinglePath, prime) ~> batchRoute ~> check {
        status should equal(StatusCodes.OK)
        verify(primeBatchStore).record(prime)
      }
    }

    it("Should allow primes to be deleted") {
      Delete(primeBatchSinglePath) ~> batchRoute ~> check {
        status should equal(StatusCodes.OK)
        verify(primeBatchStore).clear()
      }
    }
  }
}
