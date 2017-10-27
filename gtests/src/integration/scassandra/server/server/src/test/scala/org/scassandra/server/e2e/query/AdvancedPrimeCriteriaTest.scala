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

import com.datastax.driver.core.{ConsistencyLevel, SimpleStatement}
import dispatch.Defaults._
import dispatch._
import org.scalatest.concurrent.ScalaFutures
import org.scassandra.codec.Consistency._
import org.scassandra.server.AbstractIntegrationTest
import org.scassandra.server.priming.json.Success
import org.scassandra.server.priming.query.When

class AdvancedPrimeCriteriaTest extends AbstractIntegrationTest with ScalaFutures {

  val whenQuery = Some("select * from people")
  val name = "Chris"
  val nameColumn = "name"
  val rows: List[Map[String, String]] = List(Map(nameColumn -> name))

  before {
    val svc = url("http://localhost:8043/prime-query-single").DELETE
    val response = Http(svc OK as.String)
    response()
  }

  test("Priming by default should apply to the query regardless of consistency") {
    // priming
    prime(When(whenQuery), rows, Success)
  
    executeQueryAndVerifyAtConsistencyLevel(ConsistencyLevel.ONE)
    executeQueryAndVerifyAtConsistencyLevel(ConsistencyLevel.TWO)
    executeQueryAndVerifyAtConsistencyLevel(ConsistencyLevel.ALL)
  }

  test("Priming for a specific consistency should only return results for that consistency") {
    // priming
    prime(When(whenQuery, consistency = Some(List(ONE))), rows, Success)

    executeQueryAndVerifyAtConsistencyLevel(ConsistencyLevel.ONE)
    executeQueryAndVerifyNoResultsConsistencyLevel(ConsistencyLevel.TWO)
  }

  test("Priming for a multiple consistencies should only return results for those consistencies") {
    // priming
    prime(When(whenQuery, consistency = Some(List(ONE, ALL))), rows, Success)

    executeQueryAndVerifyAtConsistencyLevel(ConsistencyLevel.ONE)
    executeQueryAndVerifyAtConsistencyLevel(ConsistencyLevel.ALL)
    executeQueryAndVerifyNoResultsConsistencyLevel(ConsistencyLevel.TWO)
  }

  test("Priming for same query with different consistencies - success for both") {
    // priming
    val anotherName: String = "anotherName"
    val someDifferentRows: List[Map[String, String]] = List(Map(nameColumn -> anotherName))
    prime(When(whenQuery, consistency = Some(List(ONE, ALL))), rows, Success)
    prime(When(whenQuery, consistency = Some(List(TWO, THREE))), someDifferentRows, Success)

    executeQueryAndVerifyAtConsistencyLevel(ConsistencyLevel.ONE, name)
    executeQueryAndVerifyAtConsistencyLevel(ConsistencyLevel.ALL, name)
    executeQueryAndVerifyAtConsistencyLevel(ConsistencyLevel.TWO, anotherName)
    executeQueryAndVerifyAtConsistencyLevel(ConsistencyLevel.THREE, anotherName)
  }

  def executeQueryAndVerifyAtConsistencyLevel(consistency: ConsistencyLevel, name : String = name) {
    val statement = new SimpleStatement(whenQuery.get)
    statement.setConsistencyLevel(consistency)
    val result = session.execute(statement)
    val results = result.all()
    results.size() should equal(1)
    results.get(0).getString(nameColumn) should equal(name)
  }

  def executeQueryAndVerifyNoResultsConsistencyLevel(consistency: ConsistencyLevel) {
    val statement = new SimpleStatement(whenQuery.get)
    statement.setConsistencyLevel(consistency)
    val result = session.execute(statement)
    val results = result.all()
    results.size() should equal(0)
  }
}
