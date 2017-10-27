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

import com.datastax.driver.core.{DataType, TypeTokens}
import com.google.common.reflect.TypeToken
import dispatch.Defaults._
import dispatch._
import org.scassandra.codec.datatype.{DataType => DType}
import org.scassandra.server.AbstractIntegrationTest
import org.scassandra.server.priming.json.Success
import org.scassandra.server.priming.query.When

import scala.collection.JavaConverters._

class MapPriming extends AbstractIntegrationTest {

  before {
    val svc = url("http://localhost:8043/prime-query-single").DELETE
    val response = Http(svc OK as.String)
    response()
  }

  test("Test a map of strings") {
    val map = Map("one" -> "valueOne", "two" -> "valueTwo", "three" -> "valueThree")
    val whenQuery = "Test prime with cql map"
    val rows: List[Map[String, Any]] = List(Map("field" -> map))
    val mapOfVarcharToVarchar = DType.Map(DType.Varchar, DType.Varchar)
    val columnTypes  = Map("field" -> mapOfVarcharToVarchar)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.map(DataType.varchar(), DataType.varchar()))

    val c: Class[_] = Class.forName("java.lang.String")
    val expectedMap = map.asJava
    results.get(0).getMap("field", c, c) should equal(expectedMap)
  }

  test("Test a map of string key, list<varchar> value") {
    val map = Map("one" -> List("valueOne", "valueOne1"), "two" -> List("valueTwo"), "three" -> List("valueThree"))
    val whenQuery = "Test prime with cql map"
    val rows: List[Map[String, Any]] = List(Map("field" -> map))
    val mapOfVarcharToListVarchar = DType.Map(DType.Varchar, DType.List(DType.Varchar))
    val columnTypes  = Map("field" -> mapOfVarcharToListVarchar)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(1)
    results.get(0).getColumnDefinitions.getType("field") should equal(DataType.map(DataType.varchar(), DataType.list(DataType.varchar())))

    val expectedMap = map.mapValues(_.asJava).asJava
    val stringToken = TypeToken.of(Class.forName("java.lang.String"))
    results.get(0).getMap("field", stringToken, TypeTokens.listOf(stringToken)) should equal(expectedMap)
  }
}
