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
package org.scassandra.server.e2e.query

import java.math.BigInteger
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util
import java.util.{Date, UUID}

import com.datastax.driver.core.DataType
import dispatch.Defaults._
import dispatch._
import org.scalatest.concurrent.ScalaFutures
import org.scassandra.codec.datatype.{DataType => DType}
import org.scassandra.server.AbstractIntegrationTest
import org.scassandra.server.priming.json.Success
import org.scassandra.server.priming.query.When

class PrimingCqlTypesTest extends AbstractIntegrationTest with ScalaFutures {

  before {
    val svc = url("http://localhost:8043/prime-query-single").DELETE
    val response = Http(svc OK as.String)
    response()
  }

  test("Priming with missing type defaults to CqlVarchar") {
    // priming
    val whenQuery = "select * from people"
    val rows: List[Map[String, String]] = List(Map("name" -> "Chris", "age" -> "19"))
    val columnTypes = Map("name" -> DType.Varchar)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")
    results.get(0).getString("age") should equal("19")
  }

  test("Priming a CQL Text") {
    // priming
    val whenQuery = "select * from people"
    val rows: List[Map[String, String]] = List(Map("name" -> "Chris"))
    val columnTypes = Map("name" -> DType.Text)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")
    results.get(0).getColumnDefinitions.getType("name") should equal(DataType.text())

  }

  test("Priming a CQL int") {
    // priming
    val whenQuery = "Test prime and query with a cql int"
    val rows: List[Map[String, String]] = List(Map("age" -> "29"))
    val columnTypes  = Map("age" -> DType.Int)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getInt("age") should equal(29)

  }

  test("Priming a CQL boolean") {
    // priming
    val whenQuery = "Test prime with cql boolean"
    val rows: List[Map[String, String]] = List(Map("booleanTrue" -> "true", "booleanFalse" -> "false"))
    val columnTypes  = Map("booleanTrue" -> DType.Boolean, "booleanFalse" -> DType.Boolean)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getBool("booleanTrue") should equal(true)
    results.get(0).getBool("booleanFalse") should equal(false)
  }

  test("Priming a CQL ASCII") {
    // priming
    val whenQuery = "Test prime with cql ascii"
    val rows: List[Map[String, String]] = List(Map("asciiField" -> "Hello There"))
    val columnTypes  = Map("asciiField" -> DType.Ascii)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("asciiField") should equal("Hello There")
    results.get(0).getColumnDefinitions.getType("asciiField") should equal(DataType.ascii())
  }

  test("Priming a CQL Bigint") {
    // priming
    val whenQuery = "Test prime with cql bigint"
    val rows: List[Map[String, String]] = List(Map("bigIntField" -> "1234"))
    val columnTypes  = Map("bigIntField" -> DType.Bigint)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getLong("bigIntField") should equal(1234)
    results.get(0).getColumnDefinitions.getType("bigIntField") should equal(DataType.bigint())
  }

  test("Priming a CQL Counter") {
    // priming
    val whenQuery = "Test prime with cql bigint"
    val rows: List[Map[String, String]] = List(Map("field" -> "1234"))
    val columnTypes  = Map("field" -> DType.Counter)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.counter())
    results.get(0).getLong("field") should equal(1234)
  }

  test("Priming a CQL BLOB") {
    // priming
    val whenQuery = "Test prime with cql blob"
    val rows: List[Map[String, String]] = List(Map("field" -> "0x48656c6c6f"))
    val columnTypes  = Map("field" -> DType.Blob)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.blob())
    val byteBuffer: ByteBuffer = results.get(0).getBytes("field")
    val bytes = new Array[Byte](byteBuffer.remaining())
    byteBuffer.get(bytes)
    bytes should equal(Array[Byte](0x48, 0x65, 0x6c, 0x6c, 0x6f))
  }

  test("Priming a CQL Decimal") {
    // priming
    val whenQuery = "Test prime with cql decimal"
    val rows: List[Map[String, String]] = List(Map("field" -> "4.3456"))
    val columnTypes  = Map("field" -> DType.Decimal)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.decimal())

    results.get(0).getDecimal("field") should equal(new java.math.BigDecimal("4.3456"))
  }

  test("Priming a CQL Double") {
    // priming
    val whenQuery = "Test prime with cql double"
    val rows: List[Map[String, String]] = List(Map("field" -> "4.3456"))
    val columnTypes  = Map("field" -> DType.Double)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.cdouble())

    results.get(0).getDouble("field") should equal(4.3456)
  }

  test("Priming a CQL Float") {
    // priming
    val whenQuery = "Test prime with cql double"
    val rows: List[Map[String, String]] = List(Map("field" -> "4.3456"))
    val columnTypes  = Map("field" -> DType.Float)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.cfloat())

    results.get(0).getFloat("field") should equal(4.3456f)
  }

  test("Priming a CQL Timestamp - send as string") {
    // priming
    val date = new Date()
    val whenQuery = "Test prime with cql timestamp"
    val rows: List[Map[String, String]] = List(Map("field" -> s"${date.getTime}"))
    val columnTypes  = Map("field" -> DType.Timestamp)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.timestamp())

    results.get(0).getTimestamp("field") should equal(date)
  }

  test("Priming a CQL Timestamp - send as long") {
    // priming
    val date = new Date()
    val long : Long = date.getTime
    val whenQuery = "Test prime with cql timestamp"
    val rows: List[Map[String, Any]] = List(Map("atimestamp" -> long))
    val columnTypes  = Map("atimestamp" -> DType.Timestamp)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("atimestamp") should equal(DataType.timestamp())

    results.get(0).getTimestamp("atimestamp") should equal(date)
  }

  test("Priming a CQL UUID") {
    // priming
    val uuid = UUID.randomUUID()
    val whenQuery = "Test prime with cql uuid"
    val rows: List[Map[String, String]] = List(Map("field" -> s"${uuid.toString}"))
    val columnTypes  = Map("field" -> DType.Uuid)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.uuid())

    results.get(0).getUUID("field") should equal(uuid)
  }

  test("Priming a CQL TimeUUID") {
    // priming
    val uuid = UUID.fromString("2c530380-b9f9-11e3-850e-338bb2a2e74f")
    val whenQuery = "Test prime with cql timeuuid"
    val rows: List[Map[String, String]] = List(Map("field" -> s"${uuid.toString}"))
    val columnTypes  = Map("field" -> DType.Timeuuid)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.timeuuid())

    val uuidFromServerStub: UUID = results.get(0).getUUID("field")
    uuidFromServerStub should equal(uuid)
    uuidFromServerStub.version() should equal(1)
  }

  test("Priming a CQL inet") {
    // priming
    val inet = InetAddress.getByAddress(Array[Byte](127,0,0,1))
    val whenQuery = "Test prime with cql uuid"
    val rows: List[Map[String, String]] = List(Map("field" -> "127.0.0.1"))
    val columnTypes  = Map("field" -> DType.Inet)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.inet())

    results.get(0).getInet("field") should equal(inet)
  }

  test("Priming a CQL varint") {
    // priming
    val varint = new BigInteger("10111213141516171819")
    val whenQuery = "Test prime with cql varint"
    val rows: List[Map[String, String]] = List(Map("field" -> varint.toString()))
    val columnTypes  = Map("field" -> DType.Varint)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.varint())

    results.get(0).getVarint("field") should equal(varint)
  }

  test("Priming a CQL set of text") {
    // priming
    val set = Set("one", "two", "three")
    val whenQuery = "Test prime with cql set"
    val rows: List[Map[String, Any]] = List(Map("field" -> set))
    val setOfVarchar = DType.Set(DType.Varchar)
    val columnTypes  = Map("field" -> setOfVarchar)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.set(DataType.varchar()))

    val c: Class[_] = Class.forName("java.lang.String")
    val expectedSet = new util.HashSet[String]() // comes back as a java set
    expectedSet.add("one")
    expectedSet.add("two")
    expectedSet.add("three")
    results.get(0).getSet("field",c) should equal(expectedSet)
  }
}
