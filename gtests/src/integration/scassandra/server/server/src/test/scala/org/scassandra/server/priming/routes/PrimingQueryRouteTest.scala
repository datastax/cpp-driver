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

import org.scalatest._
import org.scassandra.codec.Consistency._
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.messages.ColumnSpec.column
import org.scassandra.codec.messages.{QueryParameters, RowMetadata}
import org.scassandra.codec.{Rows, Query => CQuery}
import org.scassandra.server.priming.json._
import org.scassandra.server.priming.query.{PrimeQuerySingle, Then, When, _}
import org.scassandra.server.priming.{ConflictingPrimes, TypeMismatch, TypeMismatches}
import spray.http.StatusCodes.{BadRequest, OK}
import spray.testkit.ScalatestRouteTest


class PrimingQueryRouteTest extends FunSpec with BeforeAndAfter with Matchers with ScalatestRouteTest with PrimingQueryRoute {

  import PrimingJsonImplicits._

  implicit def actorRefFactory = system

  implicit val primeQueryStore = new PrimeQueryStore

  val primeQuerySinglePath: String = "/prime-query-single"

  after {
    primeQueryStore.clear()
  }

  describe("Priming with exact queryText") {

    it("should return all primes for get") {
      Get(primeQuerySinglePath) ~> queryRoute ~> check {
        status should equal(OK)
      }
    }

    it("should return OK on valid request") {
      val query = "select * from users"
      val whenQuery = When(query = Some(query))
      val thenResults =
        List(
          Map("name" -> "Mickey", "age" -> "99"),
          Map(
            "name" -> "Mario",
            "age" -> "12"
          )
        )

      Post(primeQuerySinglePath, PrimeQuerySingle(whenQuery, Then(Some(thenResults)))) ~> queryRoute ~> check {
        status should equal(OK)
      }
    }

    it("should populate PrimedResults on valid request") {
      val query = "select * from users"
      val whenQuery = When(query = Some(query))
      val thenResults =
        List(
          Map(
            "name" -> "Mickey",
            "age" -> "99"
          ),
          Map(
            "name" -> "Mario",
            "age" -> "12"
          )
        )
      val prime = PrimeQuerySingle(whenQuery, Then(Some(thenResults)))

      Post(primeQuerySinglePath, prime) ~> queryRoute ~> check {
        primeQueryStore(CQuery(whenQuery.query.get, QueryParameters(consistency = ONE))).get should equal(prime.prime)
      }
    }

    it("should populate PrimedResults with ReadTimeout when result is read_request_timeout") {
      val query = "select * from users"
      val whenQuery = When(query = Some(query))
      val thenResults = List[Map[String, String]]()
      val result = Some(ReadTimeout)
      val prime = PrimeQuerySingle(whenQuery, Then(Some(thenResults), result))

      Post(primeQuerySinglePath, PrimeQuerySingle(whenQuery, Then(Some(thenResults), result))) ~> queryRoute ~> check {
        primeQueryStore(CQuery(whenQuery.query.get, QueryParameters(consistency = ONE))).get should equal(prime.prime)
      }
    }

    it("should populate PrimedResults with Success for result success") {
      val query = "select * from users"
      val whenQuery = When(query = Some(query))
      val thenResults = List[Map[String, String]]()
      val result = Some(Success)

      val prime = PrimeQuerySingle(whenQuery, Then(Some(thenResults), result))

      Post(primeQuerySinglePath, prime) ~> queryRoute ~> check {
        primeQueryStore(CQuery(whenQuery.query.get, QueryParameters(consistency = ONE))).get should equal(prime.prime)
      }
    }

    it("should delete all primes for a HTTP delete") {
      val prime = PrimeQuerySingle(When(query = Some("anything")), Then(None, Some(ReadTimeout)))
      primeQueryStore.add(prime)

      Delete(primeQuerySinglePath) ~> queryRoute ~> check {
        primeQueryStore(CQuery("anything", QueryParameters(consistency = ONE))) should equal(None)
      }
    }

    it("should accept a map as a column type") {
      val query = "select * from users"
      val whenQuery = When(query = Some(query))
      val thenResults =
        List(
          Map("mapValue" -> Map())
        )
      val columnTypes = Some(Map[String, DataType]("mapValue" -> DataType.Map(DataType.Varchar, DataType.Varchar)))

      Post(primeQuerySinglePath, PrimeQuerySingle(whenQuery, Then(Some(thenResults), column_types = columnTypes))) ~> queryRoute ~> check {
        println(body)
        status should equal(OK)
      }
    }
  }

  describe("Priming with a queryPattern") {
    it("should accept a prime with a queryPattern") {
      val query = "select \\* from users"
      val whenQuery = When(queryPattern = Some(query))
      val thenResults =
        List(
          Map("name" -> "Mickey", "age" -> "99")
        )

      val prime = PrimeQuerySingle(whenQuery, Then(Some(thenResults)))

      Post(primeQuerySinglePath, PrimeQuerySingle(whenQuery, Then(Some(thenResults)))) ~> queryRoute ~> check {
        status should equal(OK)
        primeQueryStore(CQuery("select * from users")) should equal(Some(prime.prime))
      }
    }

    it("should give a bad request if both query and queryPattern specified") {
      val query = "select * from users"
      val whenQuery = When(query = Some(query), queryPattern = Some(query))
      val thenResults =
        List(
          Map("name" -> "Mickey", "age" -> "99")
        )

      Post(primeQuerySinglePath, PrimeQuerySingle(whenQuery, Then(Some(thenResults)))) ~> queryRoute ~> check {
        status should equal(BadRequest)
      }
    }
  }

  describe("Priming incorrectly") {

    it("should reject conflicting primes as bad request") {
      val consistencies: List[Consistency] = List(ONE, TWO)
      val query: String = "select * from people"
      primeQueryStore.add(PrimeQuerySingle(When(Some(query), consistency = Some(consistencies)), Then(None)))

      val whenQuery = When(query = Some("select * from people"))
      val thenResults = List[Map[String, String]]()
      val result = Some(Success)

      Post(primeQuerySinglePath, PrimeQuerySingle(whenQuery, Then(Some(thenResults), result))) ~> queryRoute ~> check {
        status should equal(BadRequest)
        responseAs[ConflictingPrimes] should equal(ConflictingPrimes(existingPrimes = List(PrimeCriteria(query, consistencies))))
      }
    }

    it("should reject type mismatch as bad request") {
      val when = When(query = Some("select * from people"))

      val thenResults: List[Map[String, String]] =
        List(
          Map(
            "name" -> "Mickey",
            "age" -> "99"
          ),
          Map(
            "name" -> "Mario",
            "age" -> "12"
          )
        )
      val thenColumnTypes = Map("age" -> DataType.Boolean)

      val thenDo = Then(Some(thenResults), Some(Success), Some(thenColumnTypes))

      Post(primeQuerySinglePath, PrimeQuerySingle(when, thenDo)) ~> queryRoute ~> check {
        status should equal(BadRequest)
        responseAs[TypeMismatches] should equal(TypeMismatches(List(TypeMismatch("99", "age", "boolean"), TypeMismatch("12", "age", "boolean"))))
      }
    }
  }

  describe("Setting optional values in 'when'") {
    describe("keyspace") {
      it("should correctly populate PrimedResults with empty string if keyspace name not set") {
        val defaultKeyspace = ""
        val query = "select * from users"
        val whenQuery = When(query = Some(query))
        val thenRows = List()

        val primePayload = PrimeQuerySingle(whenQuery, Then(Some(thenRows)))

        Post(primeQuerySinglePath, primePayload) ~> queryRoute ~> check {
          val prime = primeQueryStore(CQuery(query, QueryParameters(consistency = ONE)))
          prime should matchPattern {
            case Some(Reply(Rows(RowMetadata(_, Some(`defaultKeyspace`), _, _), _), _, _)) =>
          }
        }
      }

      it("should correctly populate PrimedResults if keyspace name is set") {
        val expectedKeyspace = "keyspace"
        val query = "select * from users"
        val whenQuery = When(query = Some(query), keyspace = Some(expectedKeyspace))
        val thenRows = List()

        val primePayload = PrimeQuerySingle(whenQuery, Then(Some(thenRows)))

        Post(primeQuerySinglePath, primePayload) ~> queryRoute ~> check {
          val prime = primeQueryStore(CQuery(query, QueryParameters(consistency = ONE)))
          prime should matchPattern {
            case Some(Reply(Rows(RowMetadata(_, Some(`expectedKeyspace`), _, _), _), _, _)) =>
          }
        }
      }
    }

    describe("table") {
      it("should correctly populate PrimedResults with empty string if table name not set") {
        val defaultTable = ""
        val query = "select * from users"
        val whenQuery = When(query = Some(query))
        val thenRows = List()

        val primePayload = PrimeQuerySingle(whenQuery, Then(Some(thenRows)))

        Post(primeQuerySinglePath, primePayload) ~> queryRoute ~> check {
          val prime = primeQueryStore(CQuery(query, QueryParameters(consistency = ONE)))
          prime should matchPattern {
            case Some(Reply(Rows(RowMetadata(_, _, Some(`defaultTable`), _), _), _, _)) =>
          }
        }
      }

      it("should correctly populate PrimedResults if table name is set") {
        val expectedTable = "mytable"
        val query = "select * from users"
        val whenQuery = When(Some(query), table = Some(expectedTable))
        val thenRows = List()

        val primePayload = PrimeQuerySingle(whenQuery, Then(Some(thenRows)))

        Post(primeQuerySinglePath, primePayload) ~> queryRoute ~> check {
          val prime = primeQueryStore(CQuery(query, QueryParameters(consistency = ONE)))
          prime should matchPattern {
            case Some(Reply(Rows(RowMetadata(_, _, Some(`expectedTable`), _), _), _, _)) =>
          }
        }
      }
    }
  }

  describe("Priming of types") {
    it("Should pass column types to store") {
      val query = "select * from users"
      val whenQuery = When(query = Some(query))
      val thenRows =
        List(
          Map(
            "age" -> "99"
          ),
          Map(
            "age" -> "12"
          )
        )
      val expectedColumnTypes = column("age", DataType.Int) :: Nil
      val thenColumnTypes = Map("age" -> DataType.Int)
      val primePayload = PrimeQuerySingle(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post(primeQuerySinglePath, primePayload) ~> queryRoute ~> check {
        val prime = primeQueryStore(CQuery(whenQuery.query.get, QueryParameters(consistency = ONE)))

        prime should matchPattern {
          case Some(Reply(Rows(RowMetadata(_, _, _, Some(`expectedColumnTypes`)), _), _, _)) =>
        }
      }
    }

    it("Should pass column types to store even if there are no rows that contain them") {
      val query = "select * from users"
      val whenQuery = When(query = Some(query))
      val thenRows =
        List(
          Map(
            "age" -> "99"
          ),
          Map(
            "age" -> "12"
          )
        )
      val expectedColumnTypes = column("age", DataType.Int) :: column("abigD", DataType.Bigint) :: Nil
      val thenColumnTypes = Map("age" -> DataType.Int, "abigD" -> DataType.Bigint)
      val primePayload = PrimeQuerySingle(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post(primeQuerySinglePath, primePayload) ~> queryRoute ~> check {
        val prime = primeQueryStore(CQuery(whenQuery.query.get, QueryParameters(consistency = ONE)))

        prime should matchPattern {
          case Some(Reply(Rows(RowMetadata(_, _, _, Some(`expectedColumnTypes`)), _), _, _)) =>
        }
      }
    }

    it("Should handle floats as JSON strings") {
      val query = "select * from users"
      val whenQuery = When(query = Some(query))
      val thenRows = List(Map("age" -> "7.7"))
      val expectedColumnTypes = column("age", DataType.Double) :: Nil
      val thenColumnTypes = Map("age" -> DataType.Double)
      val primePayload = PrimeQuerySingle(whenQuery, Then(Some(thenRows), column_types = Some(thenColumnTypes)))

      Post(primeQuerySinglePath, primePayload) ~> queryRoute ~> check {
        val prime = primeQueryStore(CQuery(whenQuery.query.get, QueryParameters(consistency = ONE)))

        prime should matchPattern {
          case Some(Reply(Rows(RowMetadata(_, _, _, Some(`expectedColumnTypes`)), _), _, _)) =>
        }
      }
    }
  }

  describe("Retrieving of primes") {
    it("should convert a prime back to the original JSON format") {
      val query = "select * from users"
      val whenQuery = When(query = Some(query))
      val thenRows = Some(List(Map("field" -> "2c530380-b9f9-11e3-850e-338bb2a2e74f",
        "set_field" -> List("one", "two"))))
      val thenColumnTypes = Some(Map("field" -> DataType.Timeuuid, "set_field" -> DataType.Set(DataType.Varchar)))
      val primePayload = PrimeQuerySingle(whenQuery, Then(thenRows, column_types = thenColumnTypes))

      Post(primeQuerySinglePath, primePayload) ~> queryRoute

      Get(primeQuerySinglePath) ~> queryRoute ~> check {
        val response = responseAs[List[PrimeQuerySingle]]
        response.size should equal(1)
        response.head should equal(primePayload.withDefaults)
      }
    }

    it("should convert result back to original JSON format") {
      val query = "select * from users"
      val whenQuery = When(query = Some(query))
      val thenRows = Some(List(Map("one" -> "two")))
      val result = ReadTimeout
      val primePayload = PrimeQuerySingle(whenQuery, Then(thenRows, Some(result)))
      Post(primeQuerySinglePath, primePayload) ~> queryRoute

      Get(primeQuerySinglePath) ~> queryRoute ~> check {
        val response = responseAs[List[PrimeQuerySingle]]
        response.size should equal(1)
        response.head should equal(primePayload.withDefaults)
      }
    }

    it("Should return keyspace passed into prime") {
      val query = Some("select * from users")
      val whenQuery = When(query, keyspace = Some("myKeyspace"))
      val thenRows = Some(List(Map("one" -> "two")))
      val columnTypes = Map("one" -> DataType.Varchar)
      val primePayload = PrimeQuerySingle(whenQuery, Then(thenRows, column_types = Some(columnTypes)))

      Post(primeQuerySinglePath, primePayload) ~> queryRoute

      Get(primeQuerySinglePath) ~> queryRoute ~> check {
        val response = responseAs[List[PrimeQuerySingle]]
        response.size should equal(1)
        response.head should equal(primePayload.withDefaults)
      }
    }

    it("Should return table passed into prime") {
      val query = "select * from users"
      val whenQuery = When(query = Some(query), table = Some("tablename"))
      val thenRows = Some(List(Map("one" -> "two")))
      val primePayload = PrimeQuerySingle(whenQuery, Then(thenRows))

      Post(primeQuerySinglePath, primePayload) ~> queryRoute

      Get(primeQuerySinglePath) ~> queryRoute ~> check {
        val response = responseAs[List[PrimeQuerySingle]]
        response.size should equal(1)
        response.head should equal(primePayload.withDefaults)
      }
    }

    it("Should return delay passed into prime") {
      val query = "select * from users"
      val consistencies = Some(List(ONE, ALL))
      val whenQuery = When(query = Some(query), consistency = consistencies)
      val thenRows = Some(List(Map("one" -> "two")))
      val primePayload = PrimeQuerySingle(whenQuery, Then(rows = thenRows, fixedDelay = Some(123l)))

      Post(primeQuerySinglePath, primePayload) ~> queryRoute

      Get(primeQuerySinglePath) ~> queryRoute ~> check {
        val response = responseAs[List[PrimeQuerySingle]]
        response.size should equal(1)
        response.head.thenDo.fixedDelay should equal(Some(123l))
      }
    }

    it("Should return consistencies passed into prime") {
      val query = "select * from users"
      val consistencies = Some(List(ONE, ALL))
      val whenQuery = When(query = Some(query), consistency = consistencies)
      val thenRows = Some(List(Map("one" -> "two")))
      val primePayload = PrimeQuerySingle(whenQuery, Then(thenRows))

      Post(primeQuerySinglePath, primePayload) ~> queryRoute

      Get(primeQuerySinglePath) ~> queryRoute ~> check {
        val response = responseAs[List[PrimeQuerySingle]]
        response.size should equal(1)
        response.head should equal(primePayload.withDefaults)
      }
    }

  }
}
