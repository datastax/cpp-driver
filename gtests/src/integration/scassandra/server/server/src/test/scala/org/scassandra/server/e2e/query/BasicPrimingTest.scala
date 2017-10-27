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

import org.scassandra.codec.datatype.DataType
import org.scassandra.server.AbstractIntegrationTest
import org.scassandra.server.priming.query.When

class BasicPrimingTest extends AbstractIntegrationTest {
  test("Priming Rows With Different Columns") {
    val query = "select * from people"
    val rowOne = Map("name" -> "Chris")
    val rowTwo = Map("age" -> 15)
    val rows = List(rowOne, rowTwo)
    val columnTypes = Map(
      "name" -> DataType.Varchar,
      "age" -> DataType.Int)
    prime(When(query = Some(query)), rows, columnTypes = columnTypes)

    val result = session.execute(query)

    val allRows = result.all()
    allRows.size() should equal(2)
    allRows.get(0).getString("name") should equal("Chris")
    allRows.get(1).getInt("age") should equal(15)
  }
}
