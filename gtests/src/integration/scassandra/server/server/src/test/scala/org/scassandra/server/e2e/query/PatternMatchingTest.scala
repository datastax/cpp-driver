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

import org.scassandra.server.AbstractIntegrationTest
import org.scassandra.server.priming.query.When

class PatternMatchingTest extends AbstractIntegrationTest {

  test("Query should match using a *") {
    val queryWithStar = "insert into blah with ttl = .*"
    val rowOne = Map("name" -> "Chris")
    prime(When(queryPattern = Some(queryWithStar)), List(rowOne))

    val result = session.execute("insert into blah with ttl = 1234")

    val allRows = result.all()
    allRows.size() should equal(1)
    allRows.get(0).getString("name") should equal("Chris")
  }
}
