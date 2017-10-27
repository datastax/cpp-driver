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
package org.scassandra.server.priming.batch

import org.scalatest.{FunSpec, Matchers}
import org.scassandra.codec.Consistency
import org.scassandra.codec.Consistency._
import org.scassandra.codec.messages.BatchQueryKind._
import org.scassandra.codec.messages.BatchType._
import org.scassandra.server.priming._
import org.scassandra.server.priming.json.{Success, WriteTimeout}
import org.scassandra.server.priming.query.Then

class PrimeBatchStoreTest extends FunSpec with Matchers {

  val primeRequest = BatchPrimeSingle(BatchWhen(List(BatchQueryPrime("select * blah", Simple))), Then(result = Some(Success)))

  describe("Recording and matching primes") {
    it("Should match a single query") {
      val underTest = new PrimeBatchStore()

      underTest.record(primeRequest)

      val ts = System.currentTimeMillis()
      val prime = underTest(BatchExecution(Seq(BatchQuery("select * blah", Simple)), ONE, Some(Consistency.SERIAL), LOGGED, Some(ts)))

      prime should equal(Some(primeRequest.prime))
    }

    it("Should fail if single query does not match") {
      val underTest = new PrimeBatchStore()

      underTest.record(primeRequest)

      val prime = underTest(BatchExecution(Seq(BatchQuery("I am do different", Simple)), ONE, None, LOGGED, None))

      prime should equal(None)
    }

    it("Should fail if any query does not match") {
      val underTest = new PrimeBatchStore()

      underTest.record(BatchPrimeSingle(
        BatchWhen(List(BatchQueryPrime("select * blah", Simple),
          BatchQueryPrime("select * wah", Prepared))),
        Then(result = Some(Success))))

      val prime = underTest(BatchExecution(Seq(BatchQuery("select * blah", Simple)), ONE, None, LOGGED, None))

      prime should equal(None)
    }

    it("Should match if all queries match") {
      val underTest = new PrimeBatchStore()

      val batchRequest = BatchPrimeSingle(
        BatchWhen(List(BatchQueryPrime("select * blah", Simple),
          BatchQueryPrime("select * wah", Prepared))),
        Then(result = Some(Success)))

      underTest.record(batchRequest)

      val prime = underTest(BatchExecution(Seq(
        BatchQuery("select * blah", Simple),
        BatchQuery("select * wah", Prepared)), ONE, None, LOGGED, None))

      prime should equal(Some(batchRequest.prime))
    }

    it("Should use consistency - no match") {
      val underTest = new PrimeBatchStore()

      underTest.record(BatchPrimeSingle(
        BatchWhen(List(BatchQueryPrime("select * blah", Simple),
          BatchQueryPrime("select * wah", Prepared)),
          Some(List(TWO))),
        Then(result = Some(Success))))

      val prime = underTest.apply(BatchExecution(Seq(
        BatchQuery("select * blah", Simple),
        BatchQuery("select * wah", Prepared)), ONE, None, LOGGED, None))

      prime should equal(None)
    }

    it("Should use consistency - match") {
      val underTest = new PrimeBatchStore()

      val batchRequest = BatchPrimeSingle(
        BatchWhen(List(BatchQueryPrime("select * blah", Simple),
          BatchQueryPrime("select * wah", Prepared)),
          Some(List(TWO))),
        Then(result = Some(Success)))


      underTest.record(batchRequest)

      val prime = underTest.apply(BatchExecution(Seq(
        BatchQuery("select * blah", Simple),
        BatchQuery("select * wah", Prepared)), TWO, None, LOGGED, None))

      prime should equal(Some(batchRequest.prime))
    }

    it("Should match on batch type - no match") {
      val underTest = new PrimeBatchStore()

      underTest.record(BatchPrimeSingle(
        BatchWhen(List(BatchQueryPrime("select * blah", Simple)), batchType = Some(COUNTER)),
        Then(result = Some(Success))))

      val prime = underTest(BatchExecution(Seq(BatchQuery("select * blah", Simple)), ONE, None, LOGGED, None))

      prime should equal(None)
    }

    it("Should match on batch type - match") {
      val underTest = new PrimeBatchStore()

      val batchRequest = BatchPrimeSingle(
        BatchWhen(List(BatchQueryPrime("select * blah", Simple)), batchType = Some(COUNTER)),
        Then(result = Some(Success)))

      underTest.record(batchRequest)

      val prime = underTest(BatchExecution(Seq(BatchQuery("select * blah", Simple)), ONE, None, COUNTER, None))

      prime should equal(Some(batchRequest.prime))
    }

    it("Should record a batch WriteTimeout with default configuration") {
      val underTest = new PrimeBatchStore()

      val batchRequest = BatchPrimeSingle(
        BatchWhen(List(BatchQueryPrime("select * blah", Simple))),
        Then(result = Some(WriteTimeout)))

      underTest.record(BatchPrimeSingle(
        BatchWhen(List(BatchQueryPrime("select * blah", Simple))),
        Then(result = Some(WriteTimeout))))

      val prime = underTest(BatchExecution(Seq(BatchQuery("select * blah", Simple)), ONE, None, LOGGED, None))

      prime should equal(Some(batchRequest.prime))
    }

    it("Should record a batch WriteTimeout with custom configuration") {
      val underTest = new PrimeBatchStore()

      val config: Map[String, String] = Map(ErrorConstants.WriteType -> WriteType.BATCH.toString)
      val batchRequest = BatchPrimeSingle(
        BatchWhen(List(BatchQueryPrime("select * blah", Simple))),
        Then(result = Some(WriteTimeout), config = Option(config)))

      underTest.record(batchRequest)

      val prime = underTest(BatchExecution(Seq(BatchQuery("select * blah", Simple)), ONE, None, LOGGED, None))

      prime should equal(Some(batchRequest.prime))
    }
  }
}
