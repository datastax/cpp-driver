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

import java.util.concurrent.TimeUnit

import org.scalatest.{FunSuite, Matchers}
import org.scassandra.codec
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.{Consistency, Rows}
import org.scassandra.server.priming._
import org.scassandra.server.priming.json._
import org.scassandra.server.priming.query._
import scodec.bits.ByteVector

import scala.concurrent.duration.FiniteDuration

/**
  * PrimeQueryResultExtractor was "extracted" and thus is primarily tested
  * via the PrimeQueryRoute.
  *
  * Starting to add direct tests as the PrimingQueryRoute test is getting large
  */
class PrimingJsonHelperTest extends FunSuite with Matchers {

  test("Should extract PrimeCriteria from When query") {
    val queryString = "select * from world"
    val when = When(Some(queryString), consistency = Some(List(Consistency.TWO, Consistency.THREE)))
    val thenDo = Then(None, None, None, fixedDelay = None)
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val criteria = PrimingJsonHelper.extractPrimeCriteria(primeRequest).get

    criteria shouldEqual PrimeCriteria(queryString, List(Consistency.TWO, Consistency.THREE), patternMatch = false)
  }

  test("Should extract PrimeCriteria from When queryPattern") {
    val queryPattern = "select.*"
    val when = When(None, Some(queryPattern))
    val thenDo = Then(None, None, None, fixedDelay = None)
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val criteria = PrimingJsonHelper.extractPrimeCriteria(primeRequest).get

    criteria shouldEqual PrimeCriteria(queryPattern, Consistency.all, patternMatch = true)
  }

  test("Should fail to extract PrimeCriteria when both query and queryPattern provided") {
    val query = "select * from world"
    val queryPattern = "select.*"
    val when = When(Some(query), Some(queryPattern))
    val thenDo = Then(None, None, None, fixedDelay = None)
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val criteria = PrimingJsonHelper.extractPrimeCriteria(primeRequest)

    criteria.isFailure should equal(true)
  }

  test("Should default fixedDelay to None") {
    val when = When()
    val thenDo = Then(None, None, None, fixedDelay = None)
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val primeResult: Prime = PrimingJsonHelper.extractPrime(primeRequest.thenDo)

    primeResult.fixedDelay should equal(None)
  }

  test("Should record fixedDelay if present") {
    val when = When()
    val fixedDelay: Some[Long] = Some(500)
    val thenDo = Then(None, None, None, fixedDelay)
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val primeResult: Prime = PrimingJsonHelper.extractPrime(primeRequest.thenDo)

    primeResult.fixedDelay should equal(Some(FiniteDuration(500, TimeUnit.MILLISECONDS)))
  }

  test("Extracting Success result as NoRows") {
    val when = When()
    val thenDo = Then(None, Some(Success), None, fixedDelay = None)
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val primeResult: Prime = PrimingJsonHelper.extractPrime(primeRequest.thenDo)

    // Should expect an empty rows result.
    primeResult should matchPattern {
      case Reply(Rows(_, Nil), _, _) =>
    }
  }

  test("Extracting ReadRequestTimeout result") {
    val when = When()
    val properties = Map[String, String](
      ErrorConstants.ReceivedResponse -> "2",
      ErrorConstants.RequiredResponse -> "3",
      ErrorConstants.DataPresent -> "true")
    val thenDo = Then(None, Some(ReadTimeout), config = Some(properties))
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val primeResult: Prime = PrimingJsonHelper.extractPrime(primeRequest.thenDo)

    primeResult should matchPattern {
      case Reply(codec.ReadTimeout(_, _, 2, 3, true), _, _) =>
    }
  }

  test("Extracting ReadFailure result") {
    val when = When()
    val properties = Map[String, String](
      ErrorConstants.ReceivedResponse -> "2",
      ErrorConstants.RequiredResponse -> "3",
      ErrorConstants.NumberOfFailures -> "4",
      ErrorConstants.DataPresent -> "true")
    val thenDo = Then(None, Some(ReadFailure), config = Some(properties))
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val primeResult: Prime = PrimingJsonHelper.extractPrime(primeRequest.thenDo)

    primeResult should matchPattern {
      case Reply(codec.ReadFailure(_, _, 2, 3, 4, true), _, _) =>
    }
  }

  test("Extracting WriteRequestTimeout result") {
    val when = When()
    val properties = Map[String, String](
      ErrorConstants.ReceivedResponse -> "2",
      ErrorConstants.RequiredResponse -> "3",
      ErrorConstants.WriteType -> "BATCH")
    val thenDo = Then(None, Some(WriteTimeout), config = Some(properties))
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val primeResult: Prime = PrimingJsonHelper.extractPrime(primeRequest.thenDo)

    primeResult should matchPattern {
      case Reply(codec.WriteTimeout(_, _, 2, 3, "BATCH"), _, _) =>
    }
  }

  test("Extracting WriteFailure result") {
    val when = When()
    val properties = Map[String, String](
      ErrorConstants.ReceivedResponse -> "2",
      ErrorConstants.RequiredResponse -> "3",
      ErrorConstants.NumberOfFailures -> "5",
      ErrorConstants.WriteType -> "BATCH")
    val thenDo = Then(None, Some(WriteFailure), config = Some(properties))
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val primeResult: Prime = PrimingJsonHelper.extractPrime(primeRequest.thenDo)

    primeResult should matchPattern {
      case Reply(codec.WriteFailure(_, _, 2, 3, 5, "BATCH"), _, _) =>
    }
  }

  test("Extracting FunctionFailure result") {
    val when = When()
    val properties = Map[String, String](
      ErrorConstants.Keyspace -> "myks",
      ErrorConstants.Function -> "myfun",
      ErrorConstants.Argtypes -> "int,float")
    val thenDo = Then(None, Some(FunctionFailure), config = Some(properties))
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val primeResult: Prime = PrimingJsonHelper.extractPrime(primeRequest.thenDo)

    primeResult should matchPattern {
      case Reply(codec.FunctionFailure("Function Failure", "myks", "myfun", "int" :: "float" :: Nil), _, _) =>
    }
  }

  test("Extracting Unavailable result") {
    val properties = Map[String, String](
      ErrorConstants.Alive -> "2",
      ErrorConstants.RequiredResponse -> "3")
    val when = When()
    val thenDo = Then(None, Some(Unavailable), config = Some(properties))
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val primeResult: Prime = PrimingJsonHelper.extractPrime(primeRequest.thenDo)

    primeResult should matchPattern {
      case Reply(codec.Unavailable(_, _, 3, 2), _, _) =>
    }
  }

  test("Extracting Overloaded result") {
    val when = When()
    val thenDo = Then(None, Some(Overloaded))
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val primeResult: Prime = PrimingJsonHelper.extractPrime(primeRequest.thenDo)

    primeResult should matchPattern {
      case Reply(codec.Overloaded("Overloaded"), _, _) =>
    }
  }

  test("Extracting TruncateError result") {
    val when = When()
    val thenDo = Then(None, Some(TruncateError))
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val primeResult: Prime = PrimingJsonHelper.extractPrime(primeRequest.thenDo)

    primeResult should matchPattern {
      case Reply(codec.TruncateError("Truncate Error"), _, _) =>
    }
  }

  test("Extracting SyntaxError result") {
    val when = When()
    val thenDo = Then(None, Some(SyntaxError))
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val primeResult: Prime = PrimingJsonHelper.extractPrime(primeRequest.thenDo)

    primeResult should matchPattern {
      case Reply(codec.SyntaxError("Syntax Error"), _, _) =>
    }
  }

  test("Extracting Unauthorized result") {
    val when = When()
    val thenDo = Then(None, Some(Unauthorized))
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val primeResult: Prime = PrimingJsonHelper.extractPrime(primeRequest.thenDo)

    primeResult should matchPattern {
      case Reply(codec.Unauthorized("Unauthorized"), _, _) =>
    }
  }

  test("Extracting Invalid result") {
    val when = When()
    val thenDo = Then(None, Some(Invalid))
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val primeResult: Prime = PrimingJsonHelper.extractPrime(primeRequest.thenDo)

    primeResult should matchPattern {
      case Reply(codec.Invalid("Invalid"), _, _) =>
    }
  }

  test("Extracting ConfigError result") {
    val when = When()
    val thenDo = Then(None, Some(ConfigError))
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val primeResult: Prime = PrimingJsonHelper.extractPrime(primeRequest.thenDo)

    primeResult should matchPattern {
      case Reply(codec.ConfigError("Config Error"), _, _) =>
    }
  }

  test("Extracting AlreadyExists result") {
    val when = When()
    val properties = Map[String, String](
      ErrorConstants.Keyspace -> "myks",
      ErrorConstants.Table -> "mytbl")
    val thenDo = Then(None, Some(AlreadyExists), config = Some(properties))
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val primeResult: Prime = PrimingJsonHelper.extractPrime(primeRequest.thenDo)

    primeResult should matchPattern {
      case Reply(codec.AlreadyExists("Already Exists", "myks", "mytbl"), _, _) =>
    }
  }

  test("Extracting Unprepared result") {
    val when = When()
    val properties = Map[String, String](
      ErrorConstants.PrepareId -> "0xCAFECAFE")
    val thenDo = Then(None, Some(Unprepared), config = Some(properties))
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val primeResult: Prime = PrimingJsonHelper.extractPrime(primeRequest.thenDo)

    val bytes = ByteVector.fromValidHex("0xCAFECAFE")
    primeResult should matchPattern {
      case Reply(codec.Unprepared("Unprepared", `bytes`), _, _) =>
    }
  }

  test("Should extract variableTypes for prime") {
    val when = When()
    val thenDo = Then(None, None, None, variable_types = Some(List(DataType.Text)))
    val primeRequest: PrimeQuerySingle = PrimeQuerySingle(when, thenDo)

    val primeResult: Prime = PrimingJsonHelper.extractPrime(primeRequest.thenDo)

    primeResult.variableTypes should equal(Some(List(DataType.Text)))
  }
}