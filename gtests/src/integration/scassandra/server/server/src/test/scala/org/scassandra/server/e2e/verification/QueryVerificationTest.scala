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
package org.scassandra.server.e2e.verification

import com.datastax.driver.core.{ConsistencyLevel, SimpleStatement}
import org.scalatest.concurrent.ScalaFutures
import org.scassandra.codec.Consistency
import org.scassandra.server.AbstractIntegrationTest
import org.scassandra.server.PrimingHelper._
import org.scassandra.server.priming.Query

class QueryVerificationTest extends AbstractIntegrationTest with ScalaFutures {

  before {
    clearQueryPrimes()
    clearRecordedQueries()
  }

  test("Test clearing of query results") {
    session.execute("select * from people")

    clearRecordedQueries()

    getRecordedQueries() shouldEqual List()
  }

  test("Test verification of a single query") {
    val queryString: String = "select * from people"
    val statement = new SimpleStatement(queryString)
    statement.setConsistencyLevel(ConsistencyLevel.TWO)

    session.execute(statement)

    val queryList = getRecordedQueries()
    queryList.size shouldEqual 2 // 1 for use keyspace, another for actual query
    queryList(1) should matchPattern {
      case Query(`queryString`, Consistency.TWO, None, List(), List(), Some(_)) =>
    }
  }

  test("Test verification of a single query - with serial CL") {
    val queryString: String = "select * from people"
    val statement = new SimpleStatement(queryString)
    statement.setConsistencyLevel(ConsistencyLevel.TWO)
    statement.setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL)

    session.execute(statement)

    val queryList = getRecordedQueries()
    queryList.size shouldEqual 2 // 1 for use keyspace, another for actual query
    queryList(1) should matchPattern {
      case Query(`queryString`, Consistency.TWO, Some(Consistency.LOCAL_SERIAL), List(), List(), Some(_)) =>
    }
  }
}
