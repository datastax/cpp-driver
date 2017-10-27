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
package org.scassandra.server.e2e.querybuilder

import org.scassandra.codec.Consistency._
import org.scassandra.codec.datatype.DataType
import org.scassandra.server.AbstractIntegrationTest
import org.scassandra.server.PrimingHelper._
import org.scassandra.server.priming.Query
import org.scassandra.server.priming.query.{Then, When}

class QueryWithParametersTest extends AbstractIntegrationTest {

  before {
    clearQueryPrimes()
    clearRecordedQueries()
  }

  test("Prime using text parameter") {
    val query = "select * from people where name = ?"
    val rowOne = Map("name" -> "Chris", "age" -> 15)
    val columnTypes = Map(
      "name" -> DataType.Varchar,
      "age" -> DataType.Int)
    val variableTypes = List(DataType.Text)
    val thenDo = Then(rows = Some(List(rowOne)), column_types = Some(columnTypes), variable_types = Some(variableTypes))
    prime(When(query = Some(query)), thenDo)

    val result = session.execute(query, "chris")

    val queries = getRecordedQueries()
    queries.size shouldEqual 2 // 1 for use keyspace, another for the actual query
    queries(1) should matchPattern {
      case Query(`query`, ONE, None, List("chris"), List(DataType.Text), Some(_)) =>
    }
  }

  test("Prime using int parameter") {
    val query = "select * from people where age = ?"
    val rowOne = Map("name" -> "Chris", "age" -> 15)
    val columnTypes = Map(
      "name" -> DataType.Varchar,
      "age" -> DataType.Int)
    val variableTypes = List(DataType.Int)
    val thenDo = Then(Some(List(rowOne)), column_types = Some(columnTypes), variable_types = Some(variableTypes))
    prime(When(query = Some(query)), thenDo)

    val result = session.execute(query, new Integer(15))

    val queries = getRecordedQueries()
    queries.size shouldEqual 2
    queries(1) should matchPattern {
      case Query(`query`, ONE, None, List(15), List(DataType.Int), Some(_)) =>
    }
  }
}
