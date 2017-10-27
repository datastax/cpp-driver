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
package org.scassandra.server.e2e.prepared

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util
import java.util.{Date, UUID}

import akka.util.ByteString
import com.datastax.driver.core.utils.UUIDs
import com.datastax.driver.core.{ConsistencyLevel, Row}
import dispatch.Defaults._
import dispatch._
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.ScalaFutures
import org.scassandra.codec.Consistency._
import org.scassandra.codec.datatype.DataType
import org.scassandra.server.priming.ConflictingPrimes
import org.scassandra.server.priming.json.PrimingJsonImplicits
import org.scassandra.server.priming.prepared.{PrimePreparedSingle, ThenPreparedSingle, WhenPrepared}
import org.scassandra.server.{AbstractIntegrationTest, PrimingHelper}
import spray.json._

class PreparedStatementsTest extends AbstractIntegrationTest with BeforeAndAfter with ScalaFutures {

  import PrimingJsonImplicits._

  before {
    val svc = url("http://localhost:8043/prime-prepared-single").DELETE
    val response = Http(svc OK as.String)
    response()
  }

  test("Prepared statement without priming - no params") {
    //given
    val preparedStatement = session.prepare("select * from people")
    val boundStatement = preparedStatement.bind()

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(0)
  }

  test("Prepared statement without priming - single params") {
    //given
    val preparedStatement = session.prepare("select * from people where name = ?")
    val boundStatement = preparedStatement.bind("name")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(0)
  }

  test("Prepared statement for schema change") {
    //given
    val preparedStatement = session.prepare("CREATE KEYSPACE ? WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': ?, 'dc2': ?};")
    val boundStatement = preparedStatement.bind("keyspaceName","3","1")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(0)
  }

  test("Prepared statement with priming - empty rows") {
    val preparedStatementText: String = "select * from people where name = ?"
    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
      ThenPreparedSingle(Some(List()))
    )

    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(0)
  }

  test("Prepared statement with priming - single row") {
    val preparedStatementText: String = "select * from people where name = ?"
    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
      ThenPreparedSingle(Some(List(Map("name" -> "Chris"))))
    )

    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")
  }

  test("Prime for a specific consistency. Expecting no results for a different consistency.") {
    //given
    val preparedStatementText: String = "select * from people where name = ?"
    val consistencyToPrime = List(TWO)
    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText), consistency = Some(consistencyToPrime)),
      ThenPreparedSingle(Some(List(Map("name" -> "Chris"))))
    )
    val preparedStatement = session.prepare(preparedStatementText)
    preparedStatement.setConsistencyLevel(ConsistencyLevel.ONE)
    val boundStatement = preparedStatement.bind("Chris")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(0)
  }

  test("prime for a specific consistency. Get results back.") {
    //given
    val preparedStatementText: String = "select * from people where name = ?"
    val consistencyToPrime = List(QUORUM)
    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText), None, Some(consistencyToPrime)),
      ThenPreparedSingle(Some(List(Map("name" -> "Chris"))))
    )
    val preparedStatement = session.prepare(preparedStatementText)
    preparedStatement.setConsistencyLevel(ConsistencyLevel.QUORUM)
    val boundStatement = preparedStatement.bind("Chris")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")
  }
  
  test("Conflicting primes") {
    //given
    val preparedStatementText = "select * from people where name = ?"
    val consistencyOneAndTwo = List(ONE, TWO)
    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText), consistency = Some(consistencyOneAndTwo)),
      ThenPreparedSingle(Some(List(Map("name" -> "Chris"))))
    )

    //when
    val consistencyTwoAndThree = List(TWO, THREE)
    val prime = PrimePreparedSingle(WhenPrepared(Some(preparedStatementText), consistency = Some(consistencyTwoAndThree)),
      ThenPreparedSingle(Some(List(Map("name" -> "Chris"))))).toJson
    val svc = url("http://localhost:8043/prime-prepared-single") <<
      prime.toString <:<
      Map("Content-Type" -> "application/json")

    val response = Http(svc > as.String)

    whenReady(response) {
      result =>
        val conflictingPrime = JsonParser(result).convertTo[ConflictingPrimes]
        conflictingPrime.existingPrimes.size should equal(1)
    }
  }

  test("Prepared statement - priming numeric parameters") {
    //given
    val preparedStatementText = "insert into people(bigint, counter, decimal, double, float, int, varint) = (?, ?,?,?,?,?,?)"
    val resultColumnTypes = Map[String, DataType]("bigint" -> DataType.Bigint,
      "counter" -> DataType.Counter,
      "decimal" -> DataType.Decimal,
      "double" -> DataType.Double,
      "float" -> DataType.Float,
      "int" -> DataType.Int,
      "varint" -> DataType.Varint)
    val bigInt : java.lang.Long = 1234L
    val counter : java.lang.Long = 2345L
    val decimal : java.math.BigDecimal = new java.math.BigDecimal("1")
    val double : java.lang.Double = 1.5
    val float : java.lang.Float = 2.5f
    val int : java.lang.Integer = 3456
    val varint : java.math.BigInteger = new java.math.BigInteger("123")

    val rows: List[Map[String, Any]] = List(
      Map(
        "bigint" -> bigInt.toString,
        "counter" -> counter.toString,
        "decimal" -> decimal.toString,
        "double" -> double.toString,
        "float" -> float.toString,
        "int" -> int.toString,
        "varint" -> varint.toString
      )
    )
    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
      ThenPreparedSingle(Some(rows),
        Some(List[DataType](DataType.Bigint, DataType.Counter, DataType.Decimal, DataType.Double, DataType.Float, DataType.Int, DataType.Varint)),
        Some(resultColumnTypes))
    )

    //when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(bigInt, counter, decimal, double, float, int, varint)
    val result = session.execute(boundStatement)

    //then
    val all: util.List[Row] = result.all()
    all.size() should equal(1)
    val resultRow = all.get(0)
    resultRow.getLong("bigint") should equal(bigInt)
    resultRow.getLong("counter") should equal(counter)
    resultRow.getDecimal("decimal") should equal(decimal)
    resultRow.getDouble("double") should equal(double)
    resultRow.getFloat("float") should equal(float)
    resultRow.getInt("int") should equal(int)
    resultRow.getVarint("varint") should equal(varint)
  }

  test("Prepared statement - priming non-numeric parameters") {
    //given
    val preparedStatementText = "insert into people(ascii, blob, boolean, timestamp, uuid, varchar, timeuuid, inet) = (?,?,?,?,?,?,?,?)"
    val resultColumnTypes: Map[String, DataType] = Map[String, DataType](
      "ascii" -> DataType.Ascii,
      "blob" -> DataType.Blob,
      "boolean" -> DataType.Boolean,
      "timestamp" -> DataType.Timestamp,
      "uuid" -> DataType.Uuid,
      "varchar" -> DataType.Varchar,
      "timeuuid" -> DataType.Timeuuid,
      "inet" -> DataType.Inet
    )

    val ascii : String = "ascii"
    val blob : ByteBuffer = ByteString().toByteBuffer
    val boolean : java.lang.Boolean = true
    val timestamp : java.util.Date = new Date();
    val uuid : UUID = UUID.randomUUID()
    val varchar : String = "varchar"
    val timeuuid : UUID = UUIDs.timeBased()
    val inet : InetAddress = InetAddress.getByName("127.0.0.1")

    val primedRow = Map(
      "ascii" -> ascii.toString,
      "blob" -> "0x",
      "boolean" -> boolean,
      "timestamp" -> timestamp.getTime,
      "uuid" -> uuid.toString,
      "varchar" -> varchar,
      "timeuuid" -> timeuuid.toString,
      "inet" -> "127.0.0.1"
    )

    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
      ThenPreparedSingle(Some(List(primedRow)),
        variable_types = Some(List[DataType](DataType.Ascii, DataType.Blob, DataType.Boolean, DataType.Timestamp, DataType.Uuid, DataType.Varchar, DataType.Timeuuid, DataType.Inet)),
        column_types = Some(resultColumnTypes))
    )

    //when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(ascii, blob, boolean, timestamp, uuid, varchar, timeuuid, inet)
    val result = session.execute(boundStatement)

    //then
    val all: util.List[Row] = result.all()
    all.size() should equal(1)
    val resultRow = all.get(0)

    resultRow.getString("ascii") should equal(ascii)
    resultRow.getBytes("blob") should equal(blob)
    resultRow.getBool("boolean") should equal(boolean)
    resultRow.getTimestamp("timestamp") should equal(timestamp)
    resultRow.getUUID("uuid") should equal(uuid)
    resultRow.getString("varchar") should equal(varchar)
    resultRow.getUUID("timeuuid") should equal(timeuuid)
    resultRow.getInet("inet") should equal(inet)
  }
}
