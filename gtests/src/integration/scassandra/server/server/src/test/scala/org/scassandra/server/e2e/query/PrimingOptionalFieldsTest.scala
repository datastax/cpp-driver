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

import org.scalatest.concurrent.ScalaFutures
import org.scassandra.codec.datatype.DataType
import org.scassandra.server.AbstractIntegrationTest
import org.scassandra.server.priming.json.Success
import org.scassandra.server.priming.query.When

class PrimingOptionalFieldsTest extends AbstractIntegrationTest with ScalaFutures {

  //
  //
  // keyspace name
  //
  //

  test("Not priming keyspace name should return empty keyspace name") {

    // given
    val whenQuery = "select * from people"
    val rows: List[Map[String, String]] = List(Map("name" -> "Chris", "age" -> "19"))
    val columnTypes  = Map("name" -> DataType.Varchar)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    // when
    val result = session.execute(whenQuery)

    // then
    val actualKeyspace = result.getColumnDefinitions().getKeyspace(0)
    actualKeyspace should equal("")
  }

  test("Priming keyspace name should return expected keyspace name") {

    // given
    val whenQuery = "select * from people"
    val expectedKeyspace = "myKeyspace"
    val rows: List[Map[String, String]] = List(Map("name" -> "Chris", "age" -> "19"))
    val columnTypes  = Map("name" -> DataType.Varchar)
    prime(When(query = Some(whenQuery), keyspace = Some(expectedKeyspace)), rows, Success, columnTypes)

    // when
    val result = session.execute(whenQuery)

    // then
    val actualKeyspace = result.getColumnDefinitions().getKeyspace(0)
    actualKeyspace should equal(expectedKeyspace)
  }


  //
  //
  // table name
  //
  //

  test("Not priming table name should return empty table names") {

    // given
    val whenQuery = "select * from people"
    val rows: List[Map[String, String]] = List(Map("name" -> "Chris", "age" -> "19"))
    val columnTypes  = Map("name" -> DataType.Varchar)
    prime(When(query = Some(whenQuery)), rows, Success, columnTypes)

    // when
    val result = session.execute(whenQuery)

    // then
    val actualTable = result.getColumnDefinitions().getTable(0)
    actualTable should equal("")
  }

  test("Priming table name should return expected table names") {

    // given
    val whenQuery = "select * from people"
    val expectedTable = "mytable"
    val rows: List[Map[String, String]] = List(Map("name" -> "Chris", "age" -> "19"))
    val columnTypes  = Map("name" -> DataType.Varchar)
    prime(When(query = Some(whenQuery), table = Some(expectedTable)), rows, Success, columnTypes)

    // when
    val result = session.execute(whenQuery)

    // then
    val actualTable = result.getColumnDefinitions().getTable(0)
    actualTable should equal(expectedTable)
  }
}
