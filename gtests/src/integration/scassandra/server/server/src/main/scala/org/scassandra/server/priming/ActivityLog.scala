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
package org.scassandra.server.priming

import com.typesafe.scalalogging.LazyLogging
import org.scassandra.codec.Consistency.Consistency
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.messages.BatchQueryKind.BatchQueryKind
import org.scassandra.codec.messages.BatchType.BatchType

class ActivityLog extends LazyLogging {

  var connections: List[Connection] = List()
  var queries: List[Query] = List()
  var preparedStatementPreparations: List[PreparedStatementPreparation] = List()
  var preparedStatementExecutions: List[PreparedStatementExecution] = List()
  var batchExecutions: List[BatchExecution] = List()

  /*  Query activity logging */

  def clearQueries() = {
    queries = List()
  }

  def retrieveQueries() : List[Query] = queries

  def recordQuery(query: String, consistency: Consistency, serialConsistency: Option[Consistency] = None, variables: List[Any] = List(), variableTypes: List[DataType] = List(), timestamp: Option[Long] = None) = {
    queries = queries ::: Query(query, consistency, serialConsistency, variables, variableTypes, timestamp) :: Nil
  }

  /*  Connection activity logging */

  def recordConnection() = {
    connections = connections ::: Connection() :: Nil
  }

  def retrieveConnections(): List[Connection] = connections

  def clearConnections(): Unit = {
    connections = List()
  }

  /*  PreparedStatementPreparation activity logging */

  def recordPreparedStatementPreparation(activity: PreparedStatementPreparation) = {
    logger.info("Recording {}", activity)
    preparedStatementPreparations = preparedStatementPreparations ::: activity :: Nil
  }

  def retrievePreparedStatementPreparations(): List[PreparedStatementPreparation] = {
    preparedStatementPreparations
  }

  def clearPreparedStatementPreparations() = {
    logger.info("Clearing prepared statement preparations")
    preparedStatementPreparations = List()
  }

  /*  PreparedStatementExecution activity logging */

  def recordPreparedStatementExecution(preparedStatementText: String, consistency: Consistency, serialConsistency: Option[Consistency], variables: List[Any], variableTypes: List[DataType], timestamp: Option[Long]): Unit = {
    val execution: PreparedStatementExecution = PreparedStatementExecution(preparedStatementText, consistency,
      serialConsistency, variables, variableTypes, timestamp)
    logger.info("Recording {}",execution)
    preparedStatementExecutions = preparedStatementExecutions ::: execution :: Nil
  }

  def recordPreparedStatementExecution(execution: PreparedStatementExecution): Unit = {
    logger.info("Recording {}", execution)
    preparedStatementExecutions = preparedStatementExecutions ::: execution :: Nil
  }

  def retrievePreparedStatementExecutions(): List[PreparedStatementExecution] = {
    preparedStatementExecutions
  }

  def clearPreparedStatementExecutions() = {
    preparedStatementExecutions = List()
  }

  /*  BatchExecution activity logging */

  def recordBatchExecution(batchExecution: BatchExecution): Unit = {
    logger.info("Recording {}", batchExecutions)
    batchExecutions = batchExecutions ::: batchExecution :: Nil
  }

  def retrieveBatchExecutions(): List[BatchExecution] = {
    logger.info("Retrieving batch executions {}", batchExecutions)
    batchExecutions
  }

  def clearBatchExecutions(): Unit = {
    batchExecutions = List()
  }
}

case class Query(query: String, consistency: Consistency, serialConsistency: Option[Consistency],
                 variables: List[Any] = List(), variableTypes: List[DataType] = List(), timestamp: Option[Long] = None)
case class Connection(result: String = "success")
case class PreparedStatementExecution(preparedStatementText: String, consistency: Consistency,
                                      serialConsistency: Option[Consistency], variables: List[Any],
                                      variableTypes: List[DataType], timestamp: Option[Long])
case class BatchQuery(query: String, batchQueryKind: BatchQueryKind, variables: List[Any] = List(), variableTypes: List[DataType] = List())
case class BatchExecution(batchQueries: Seq[BatchQuery], consistency: Consistency,
                          serialConsistency: Option[Consistency], batchType: BatchType, timestamp: Option[Long])
case class PreparedStatementPreparation(preparedStatementText: String)
