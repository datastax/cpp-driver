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
package org.scassandra.server.priming.routes

import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.scassandra.codec.Consistency.ONE
import org.scassandra.codec.messages.BatchQueryKind.Simple
import org.scassandra.codec.messages.BatchType._
import org.scassandra.server.priming._
import org.scassandra.server.priming.json.PrimingJsonImplicits
import spray.json.JsonParser
import spray.testkit.ScalatestRouteTest

class ActivityVerificationRouteTest extends FunSpec with BeforeAndAfter with Matchers with ScalatestRouteTest with ActivityVerificationRoute {

  implicit def actorRefFactory = system
  implicit val activityLog = new ActivityLog

  import PrimingJsonImplicits._

  before {
    activityLog.clearConnections()
    activityLog.clearPreparedStatementPreparations()
    activityLog.clearPreparedStatementExecutions()
    activityLog.clearQueries()
  }

  describe("Retrieving connection activity") {
    it("Should return connection count from ActivityLog for single connection") {
      activityLog.recordConnection()

      Get("/connection") ~> activityVerificationRoute ~> check {
        val response: String = responseAs[String]
        val connectionList = JsonParser(response).convertTo[List[Connection]]
        connectionList.size should equal(1)
      }
    }

    it("Should return connection count from ActivityLog for no connections") {

      Get("/connection") ~> activityVerificationRoute ~> check {
        val response: String = responseAs[String]
        val connectionList = JsonParser(response).convertTo[List[Connection]]
        connectionList.size should equal(0)
      }
    }

    it("Should clear connections for a delete") {
      //todo clear activity

      Delete("/connection") ~> activityVerificationRoute ~> check {
        activityLog.retrieveConnections().size should equal(0)
      }
    }
  }

  describe("Retrieving query activity") {

    it("Should return queries from ActivityLog - no queries") {

      Get("/query") ~> activityVerificationRoute ~> check {
        val response: String = responseAs[String]
        val queryList = JsonParser(response).convertTo[List[Query]]
        queryList.size should equal(0)
      }
    }

    it("Should return queries from ActivityLog - single query") {
      val query: String = "select * from people"
      activityLog.recordQuery(query, ONE)

      Get("/query") ~> activityVerificationRoute ~> check {
        val response: String = responseAs[String]
        val queryList = JsonParser(response).convertTo[List[Query]]
        queryList.size should equal(1)
        queryList.head.query should equal(query)
      }
    }

    it("Should clear queries for a delete") {
      activityLog.recordQuery("select * from people", ONE)

      Delete("/query") ~> activityVerificationRoute ~> check {
        activityLog.retrieveQueries().size should equal(0)
      }
    }
  }

  describe("Primed statement preparations") {
    it("Should return prepared statement preparations from ActivityLog - no activity") {
      activityLog.clearPreparedStatementPreparations()

      Get("/prepared-statement-preparation") ~> activityVerificationRoute ~> check {
        val response = responseAs[List[PreparedStatementPreparation]]
        response.size should equal(0)
      }
    }

    it("Should return prepared statement preparations from ActivityLog - single") {
      activityLog.clearPreparedStatementPreparations()
      val preparedStatementText = "cat"
      activityLog.recordPreparedStatementPreparation(PreparedStatementPreparation(preparedStatementText))

      Get("/prepared-statement-preparation") ~> activityVerificationRoute ~> check {
        val response = responseAs[List[PreparedStatementPreparation]]

        response.size should equal(1)
        response.head.preparedStatementText should equal(preparedStatementText)
      }
    }

    it("Should clear prepared statement preparations for a delete") {
      val preparedStatementText = "dog"
      activityLog.recordPreparedStatementPreparation(PreparedStatementPreparation(preparedStatementText))

      Delete("/prepared-statement-preparation") ~> activityVerificationRoute ~> check {
        activityLog.retrievePreparedStatementPreparations().size should equal(0)
      }
    }
  }

  describe("Primed statement execution") {
    it("Should return prepared statement executions from ActivityLog - no activity") {
      activityLog.clearPreparedStatementExecutions()

      Get("/prepared-statement-execution") ~> activityVerificationRoute ~> check {
        val response = responseAs[List[PreparedStatementExecution]]
        response.size should equal(0)
      }
    }

    it("Should return prepared statement executions from ActivityLog - single") {
      activityLog.clearPreparedStatementExecutions()
      val preparedStatementText: String = ""
      activityLog.recordPreparedStatementExecution(preparedStatementText, ONE, None, List(), List(), None)

      Get("/prepared-statement-execution") ~> activityVerificationRoute ~> check {
        val response = responseAs[List[PreparedStatementExecution]]

        response.size should equal(1)
        response.head.preparedStatementText should equal(preparedStatementText)
      }
    }

    it("Should clear prepared statement executions for a delete") {
      activityLog.recordPreparedStatementExecution("", ONE, None, List(), List(), None)

      Delete("/prepared-statement-execution") ~> activityVerificationRoute ~> check {
        activityLog.retrievePreparedStatementExecutions().size should equal(0)
      }
    }
  }

  describe("Batch execution") {
    it("Should return executions from ActivityLog") {
      activityLog.clearBatchExecutions()
      activityLog.recordBatchExecution(BatchExecution(List(BatchQuery("Query", Simple)), ONE, None, LOGGED, None))

      Get("/batch-execution") ~> activityVerificationRoute ~> check {
        val response = responseAs[List[BatchExecution]]

        response.size should equal(1)
        response.head.batchQueries should equal(List(BatchQuery("Query", Simple)))
        response.head.consistency should equal(ONE)
        response.head.batchType should equal(LOGGED)
      }
    }

    it("Should clear batch executions for a delete") {
      activityLog.recordBatchExecution(BatchExecution(List(), ONE, None, UNLOGGED, None))

      Delete("/batch-execution") ~> activityVerificationRoute ~> check {
        activityLog.retrieveBatchExecutions().size should equal(0)
      }
    }
  }
}
