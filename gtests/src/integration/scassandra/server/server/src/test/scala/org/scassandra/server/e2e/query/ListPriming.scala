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

import com.datastax.driver.core.{DataType, TypeTokens}
import com.google.common.reflect.TypeToken
import org.scassandra.codec.datatype.{DataType => DType}
import org.scassandra.server.AbstractIntegrationTest
import org.scassandra.server.priming.json.Success
import org.scassandra.server.priming.query.When

import scala.collection.JavaConverters._

class ListPriming extends AbstractIntegrationTest {

  test("Test a list of varchars") {
    val list = List("one", "two", "three")
    val whenQuery = "Test prime with cql list"
    val rows: List[Map[String, Any]] = List(Map("field" -> list))
    val columnTypes  = Map("field" -> DType.List(DType.Varchar))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.list(DataType.varchar()))

    val expectedList = util.Arrays.asList("one","two","three")
    singleRow.getList("field", Class.forName("java.lang.String")) should equal(expectedList)
  }

  test("Test a list of ascii") {
    val list = List("one", "two", "three")
    val whenQuery = "Test prime with cql list"
    val rows: List[Map[String, Any]] = List(Map("field" -> list))
    val columnTypes  = Map("field" -> DType.List(DType.Ascii))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.list(DataType.ascii()))

    val expectedList = util.Arrays.asList("one","two","three")
    singleRow.getList("field", Class.forName("java.lang.String")) should equal(expectedList)
  }

  test("Test a list of text") {
    val list = List("one", "two", "three")
    val whenQuery = "Test prime with cql list"
    val rows: List[Map[String, Any]] = List(Map("field" -> list))
    val columnTypes  = Map("field" -> DType.List(DType.Text))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.list(DataType.text()))

    val expectedList = util.Arrays.asList("one","two","three")
    singleRow.getList("field", Class.forName("java.lang.String")) should equal(expectedList)
  }

  test("Test a list of int") {
    val list = List(1, 2, 3)
    val whenQuery = "Test prime with cql list"
    val rows: List[Map[String, Any]] = List(Map("field" -> list))
    val columnTypes  = Map("field" -> DType.List(DType.Int))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.list(DataType.cint()))

    val expectedList = util.Arrays.asList(1,2,3)
    singleRow.getList("field", Class.forName("java.lang.Integer")) should equal(expectedList)
  }

  test("Test a list of bigint (java long)") {
    val list = List(1l, 2l, 3l)
    val whenQuery = "Test prime with cql list"
    val rows: List[Map[String, Any]] = List(Map("field" -> list))
    val columnTypes  = Map("field" -> DType.List(DType.Bigint))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.list(DataType.bigint()))

    val expectedList = util.Arrays.asList(1l,2l,3l)
    singleRow.getList("field", Class.forName("java.lang.Long")) should equal(expectedList)
  }

  test("Test a list of boolean") {
    val list = List(true, false, true)
    val whenQuery = "Test prime with cql list"
    val rows: List[Map[String, Any]] = List(Map("field" -> list))
    val columnTypes  = Map("field" -> DType.List(DType.Boolean))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.list(DataType.cboolean()))

    val expectedList = util.Arrays.asList(true, false, true)
    singleRow.getList("field", Class.forName("java.lang.Boolean")) should equal(expectedList)
  }

  test("Test a list of counter") {
    val list = List(1l, 2l, 3l)
    val whenQuery = "Test prime with cql list"
    val rows: List[Map[String, Any]] = List(Map("field" -> list))
    val columnTypes  = Map("field" -> DType.List(DType.Counter))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.list(DataType.counter()))

    val expectedList = util.Arrays.asList(1l, 2l, 3l)
    singleRow.getList("field", Class.forName("java.lang.Long")) should equal(expectedList)
  }

  test("Test a list of decimal") {
    val list = List(BigDecimal("1.2"), BigDecimal("2.3"), BigDecimal("3.4"))
    val whenQuery = "Test prime with cql list"
    val rows: List[Map[String, Any]] = List(Map("field" -> list))
    val columnTypes  = Map("field" -> DType.List(DType.Decimal))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.list(DataType.decimal()))

    val expectedList = util.Arrays.asList(new java.math.BigDecimal("1.2"), new java.math.BigDecimal("2.3"), new java.math.BigDecimal("3.4"))
    singleRow.getList("field", Class.forName("java.math.BigDecimal")) should equal(expectedList)
  }

  test("Test a list of double") {
    val list = List(1.0, 2.0, 3.0)
    val whenQuery = "Test prime with cql list"
    val rows: List[Map[String, Any]] = List(Map("field" -> list))
    val columnTypes  = Map("field" -> DType.List(DType.Double))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.list(DataType.cdouble()))

    val expectedList = util.Arrays.asList(1.0, 2.0, 3.0)
    singleRow.getList("field", Class.forName("java.lang.Double")) should equal(expectedList)
  }

  test("Test a list of float") {
    val list = List(1.0f, 2.0f, 3.0f)
    val whenQuery = "Test prime with cql list"
    val rows: List[Map[String, Any]] = List(Map("field" -> list))
    val columnTypes  = Map("field" -> DType.List(DType.Float))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.list(DataType.cfloat()))

    val expectedList = util.Arrays.asList(1.0f, 2.0f, 3.0f)
    singleRow.getList("field", Class.forName("java.lang.Float")) should equal(expectedList)
  }

  test("Test a list of inet") {
    val list = List(InetAddress.getLocalHost)
    val whenQuery = "Test prime with cql list"
    val rows: List[Map[String, Any]] = List(Map("field" -> list))
    val columnTypes  = Map("field" -> DType.List(DType.Inet))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.list(DataType.inet()))

    val expectedList = util.Arrays.asList(InetAddress.getLocalHost)
    singleRow.getList("field", Class.forName("java.net.InetAddress")) should equal(expectedList)
  }

  test("Test a list of date") {
    val date = new Date
    val list = List(date.getTime)
    val whenQuery = "Test prime with cql list"
    val rows: List[Map[String, Any]] = List(Map("field" -> list))
    val columnTypes  = Map("field" -> DType.List(DType.Timestamp))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.list(DataType.timestamp()))

    val expectedList = util.Arrays.asList(date)
    singleRow.getList("field", Class.forName("java.util.Date")) should equal(expectedList)
  }

  test("Test a list of uuid") {
    val uuid = UUID.randomUUID()
    val list = List(uuid)
    val whenQuery = "Test prime with cql list"
    val rows: List[Map[String, Any]] = List(Map("field" -> list))
    val columnTypes  = Map("field" -> DType.List(DType.Uuid))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.list(DataType.uuid()))

    val expectedList = util.Arrays.asList(uuid)
    singleRow.getList("field", Class.forName("java.util.UUID")) should equal(expectedList)
  }

  test("Test a list of timeuuid") {
    val uuid = UUID.fromString("1c0e8c70-754b-11e4-ac06-4b05b98cc84c")
    val list = List(uuid)
    val whenQuery = "Test prime with cql list"
    val rows: List[Map[String, Any]] = List(Map("field" -> list))
    val columnTypes  = Map("field" -> DType.List(DType.Timeuuid))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.list(DataType.timeuuid()))

    val expectedList = util.Arrays.asList(uuid)
    singleRow.getList("field", Class.forName("java.util.UUID")) should equal(expectedList)
  }

  test("Test a list of varint") {
    val list = List(BigInt("1"), BigInt("2"), BigInt("3"))
    val whenQuery = "Test prime with cql list"
    val rows: List[Map[String, Any]] = List(Map("field" -> list))
    val columnTypes  = Map("field" -> DType.List(DType.Varint))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.list(DataType.varint()))

    val expectedList = util.Arrays.asList(new BigInteger("1"), new BigInteger("2"), new BigInteger("3"))
    singleRow.getList("field", Class.forName("java.math.BigInteger")) should equal(expectedList)
  }

  test("Test a list of blob") {
    val blob = Array[Byte](1,2,3,4,5)
    val list = List(blob, blob)
    val whenQuery = "Test prime with cql list"
    val rows: List[Map[String, Any]] = List(Map("field" -> list))
    val columnTypes  = Map("field" -> DType.List(DType.Blob))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.list(DataType.blob()))

    val expectedList = util.Arrays.asList(ByteBuffer.wrap(blob), ByteBuffer.wrap(blob))
    singleRow.getList("field", Class.forName("java.nio.ByteBuffer")) should equal(expectedList)
  }

  test("Test a list of set<text>") {
    val list = List(Set("one", "two", "three"), Set("four", "five", "six"))
    val whenQuery = "Test prime with cql list of sets"
    val rows: List[Map[String, Any]] = List(Map("field" -> list))
    val columnTypes = Map("field" -> DType.List(DType.Set(DType.Varchar)))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()
    singleRow.getColumnDefinitions.getType("field") should equal(DataType.list(DataType.set(DataType.varchar())))

    val expectedList = list.map(_.asJava).asJava
    singleRow.getList("field", TypeTokens.setOf(TypeToken.of(Class.forName("java.lang.String")))) should equal(expectedList)
  }
}