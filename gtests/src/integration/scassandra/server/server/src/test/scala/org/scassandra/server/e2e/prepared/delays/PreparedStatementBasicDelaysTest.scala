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
package org.scassandra.server.e2e.prepared.delays

import dispatch.Defaults._
import dispatch._
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.ScalaFutures
import org.scassandra.server.priming.prepared.{ThenPreparedSingle, WhenPrepared}
import org.scassandra.server.{AbstractIntegrationTest, PrimingHelper}

class PreparedStatementBasicDelaysTest extends AbstractIntegrationTest with BeforeAndAfter with ScalaFutures {

  before {
    val svc = url("http://localhost:8043/prime-prepared-single").DELETE
    val response = Http(svc OK as.String)
    response()
  }

  test("Prepared statement with delay") {
    val fixedDelay: Long = 1500l
    val preparedStatementText: String = "select * from people where name = ?"
    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
      ThenPreparedSingle(Some(List(Map("name" -> "Chris"))), fixedDelay = Some(fixedDelay))
    )
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind("Chris")

    //when
    val timeBefore = System.currentTimeMillis()
    val result = session.execute(boundStatement)
    val difference: Long = System.currentTimeMillis() - timeBefore

    //then
    val allRows = result.all()
    allRows.size() should equal(1)
    difference should be > fixedDelay
  }

  test("Prepared statement with delay and query pattern") {
    val fixedDelay: Long = 1500l
    val preparedStatementText: String = ".*"
    PrimingHelper.primePreparedStatement(
      WhenPrepared(queryPattern = Some(preparedStatementText)),
      ThenPreparedSingle(Some(List(Map("name" -> "Chris"))), fixedDelay = Some(fixedDelay))
    )
    val preparedStatement = session.prepare("select * from person where name = ?")
    val boundStatement = preparedStatement.bind("Chris")

    //when
    val timeBefore = System.currentTimeMillis()
    val result = session.execute(boundStatement)
    val difference: Long = System.currentTimeMillis() - timeBefore

    //then
    val allRows = result.all()
    allRows.size() should equal(1)
    difference should be > fixedDelay
  }


}
