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
package org.scassandra.server.e2e.query.delays

import org.scassandra.server.AbstractIntegrationTest
import org.scassandra.server.priming.query.{Then, When}

class BasicDelaysPrimingTest extends AbstractIntegrationTest {
   test("Priming Rows with delay") {
    val query = "select * from people"
    val rowOne = Map("name" -> "Chris")
    val rows = List(rowOne)
    val thenDo = Then(Some(rows), fixedDelay = Some(2000l))
    prime(When(query = Some(query)), thenDo)

    val timeBefore = System.currentTimeMillis()
    val result = session.execute(query)
    val difference: Long = System.currentTimeMillis() - timeBefore

    val allRows = result.all()
    allRows.size() should equal(1)
    difference should be > 2000l
  }

  //todo #50 delay an error response
}
