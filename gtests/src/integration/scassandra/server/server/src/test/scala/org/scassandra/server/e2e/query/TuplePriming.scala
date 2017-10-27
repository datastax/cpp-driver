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

import java.net.InetAddress
import java.util.{Date, UUID}

import com.datastax.driver.core._
import org.scassandra.codec.datatype.{DataType => DType}
import org.scassandra.server.AbstractIntegrationTest
import org.scassandra.server.priming.json.Success
import org.scassandra.server.priming.query.When

import scala.collection.JavaConverters._

class TuplePriming extends AbstractIntegrationTest {

  def tt(dataType: DataType*): TupleType =
    TupleType.of(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE, dataType: _*)

  test("Test a tuple<varchar, ascii>") {
    val tuple = ("one", "two")
    val whenQuery = "Test prime with tuple<varchar, ascii>"
    val rows: List[Map[String, Any]] = List(Map("field" -> tuple))
    val columnTypes = Map("field" -> DType.Tuple(DType.Varchar, DType.Ascii))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()

    val tupleType = tt(DataType.varchar(), DataType.ascii())
    val expectedTuple = tupleType.newValue(tuple._1, tuple._2)

    singleRow.getColumnDefinitions.getType("field") should equal(tupleType)
    singleRow.getTupleValue("field") should equal(expectedTuple)
  }

  test("Test a tuple<int, long, boolean, counter, decimal, double, float>") {
    // A reasonably wide tuple.
    val tuple = (1, 2L, false, 3L, BigDecimal("1.2"), 3.02, 3.01f)
    val whenQuery = "test prime with tuple<int, long, boolean, counter, decimal, double, float>"
    val rows: List[Map[String, Any]] = List(Map("field" -> tuple))
    val columnTypes = Map("field" -> DType.Tuple(DType.Int, DType.Bigint, DType.Boolean, DType.Counter, DType.Decimal, DType.Double, DType.Float))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()

    val tupleType: TupleType = tt(DataType.cint(), DataType.bigint(), DataType.cboolean(), DataType.counter(), DataType.decimal(),
      DataType.cdouble(), DataType.cfloat())
    val expectedTuple = tupleType.newValue(
      new java.lang.Integer(tuple._1),
      new java.lang.Long(tuple._2),
      new java.lang.Boolean(tuple._3),
      new java.lang.Long(tuple._4),
      tuple._5.bigDecimal,
      new java.lang.Double(tuple._6),
      new java.lang.Float(tuple._7))

    singleRow.getColumnDefinitions.getType("field") should equal(tupleType)
    singleRow.getTupleValue("field") should equal(expectedTuple)
  }

  test("Test a tuple<inet, timestamp, uuid, timeuuid, varint> using a list as input") {
    val inet = InetAddress.getLocalHost
    val date = new Date().getTime
    val uuid = UUID.randomUUID
    val timeuuid = UUID.fromString("1c0e8c70-754b-11e4-ac06-4b05b98cc84c")
    val varint = BigInt("2")

    val tuple = inet :: date :: uuid :: timeuuid :: varint :: Nil
    val whenQuery = "test prime with tuple<inet, timestamp, uuid, timeuuid, varint>"
    val rows: List[Map[String, Any]] = List(Map("field" -> tuple))
    val columnTypes = Map("field" -> DType.Tuple(DType.Inet, DType.Timestamp, DType.Uuid, DType.Timeuuid, DType.Varint))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()

    val tupleType: TupleType = tt(DataType.inet(), DataType.timestamp(), DataType.uuid(), DataType.timeuuid(), DataType.varint())
    val expectedTuple = tupleType.newValue(
      inet,
      new Date(date),
      uuid,
      timeuuid,
      varint.bigInteger
    )

    singleRow.getColumnDefinitions.getType("field") should equal(tupleType)
    singleRow.getTupleValue("field") should equal(expectedTuple)
  }

  test("Test a tuple<tuple<date,list<smallint>>,map<time,tinyint>>") {
    // A somewhat complicated tuple with nested collections.
    val date = LocalDate.fromYearMonthDay(2014, 9, 17)
    val smallints = List[Short](10, 28, 38)
    val timemap = Map[Long, Byte](10L -> 0x5.toByte, 8674L -> 0x7.toByte)
    // Date is centered on epoch being 2^31
    val dateVal = math.pow(2, 31).toLong + date.getDaysSinceEpoch

    val tuple = ((dateVal, smallints), timemap)
    val whenQuery = "test prime with tuple<tuple<date,list<smallint>>,map<time,tinyint>>"
    val rows: List[Map[String, Any]] = List(Map("field" -> tuple))
    val columnTypes = Map("field" -> DType.Tuple(DType.Tuple(DType.Date, DType.List(DType.Smallint)), DType.Map(DType.Time, DType.Tinyint)))
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val singleRow = result.one()

    val innerTuple = tt(DataType.date(), DataType.list(DataType.smallint()))
    val tupleType: TupleType = tt(innerTuple, DataType.map(DataType.time(), DataType.tinyint()))
    val expectedTuple = tupleType.newValue(
      innerTuple.newValue(date, smallints.asJava),
      timemap.asJava
    )

    singleRow.getColumnDefinitions.getType("field") should equal(tupleType)
    singleRow.getTupleValue("field") should equal(expectedTuple)
  }
}
