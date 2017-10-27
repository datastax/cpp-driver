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
package org.scassandra.server.e2e

import dispatch.Defaults._
import dispatch._
import org.scalatest.concurrent.ScalaFutures
import org.scassandra.server.AbstractIntegrationTest
import org.scassandra.server.priming.PreparedStatementPreparation
import org.scassandra.server.priming.json.{PrimingJsonImplicits}
import spray.json._

class PreparedStatementPreparationVerificationTest extends AbstractIntegrationTest with ScalaFutures {

  import PrimingJsonImplicits._

  val primePreparedSinglePath = "http://localhost:8043/prepared-statement-preparation"

  before {
    val svc = url(primePreparedSinglePath).DELETE
    val response = Http(svc OK as.String)
    response()
  }

  test("Test clearing of prepared statement preparations") {
    //todo: clean activity log
    val queryString = "select * from people where name = ?"
    val preparedStatement = session.prepare(queryString)
    val boundStatement = preparedStatement.bind("Chris")
    session.execute(boundStatement)
    val svc: Req = url(primePreparedSinglePath)
    val delete = svc.DELETE
    val deleteResponse = Http(delete OK as.String)
    deleteResponse()

    val listOfPreparedStatementPreparations = Http(svc OK as.String)
    whenReady(listOfPreparedStatementPreparations) { result =>
      JsonParser(result).convertTo[List[PreparedStatementPreparation]].size should equal(0)
    }
  }

  test("Test verification of a single prepared statement preparation") {
    val queryString: String = "select * from people where name = ?"
    val preparedStatement = session.prepare(queryString)
    val boundStatement = preparedStatement.bind("Chris")
    session.execute(boundStatement)

    val svc: Req = url(primePreparedSinglePath)
    val response = Http(svc OK as.String)

    whenReady(response) { result =>
      val preparations = JsonParser(result).convertTo[List[PreparedStatementPreparation]]
      preparations.size should equal(1)
      preparations.head should equal(PreparedStatementPreparation(queryString))
    }
  }
}
