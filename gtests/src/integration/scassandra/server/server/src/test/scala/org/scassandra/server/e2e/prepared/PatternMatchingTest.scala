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

import org.scassandra.codec.datatype.DataType
import org.scassandra.server.priming.prepared.{ThenPreparedSingle, WhenPrepared}
import org.scassandra.server.{AbstractIntegrationTest, PrimingHelper}

class PatternMatchingTest extends AbstractIntegrationTest {

  test("Prepared statement should match using a .* without specifying variable types") {
    val preparedStatementText: String = "select * from people where name = ?"
    val preparedStatementRegex: String = "select .* from people .*"
    PrimingHelper.primePreparedStatement(
      WhenPrepared(None, Some(preparedStatementRegex)),
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

  test("Prepared statement should match using a .*  specifying all variable types") {
    val preparedStatementText: String = "select * from people where name = ? and age = ?"
    val preparedStatementRegex: String = "select .* from people .*"
    PrimingHelper.primePreparedStatement(
      WhenPrepared(None, Some(preparedStatementRegex)),
      ThenPreparedSingle(
        Some(List(Map("name" -> "Chris"))),
        Some(List(DataType.Text, DataType.Int))
        )
    )

    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris", new Integer(15))

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")
  }

  test("Prepared statement should match using a .*  specifying a subset variable types") {
    val preparedStatementText: String = "select * from people where age = ? and name = ?"
    val preparedStatementRegex: String = "select .* from people .*"
    PrimingHelper.primePreparedStatement(
      WhenPrepared(None, Some(preparedStatementRegex)),
      ThenPreparedSingle(
        Some(List(Map("name" -> "Chris"))),
        Some(List(DataType.Int))
      )
    )

    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(new Integer(15), "Chris")

    //when
    val result = session.execute(boundStatement)

    //then
    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString("name") should equal("Chris")
  }
}
