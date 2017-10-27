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
package org.scassandra.server.actors

import akka.actor.ActorRef
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import org.scassandra.codec._
import org.scassandra.server.actors.ExecuteHandler.HandleExecute
import org.scassandra.server.actors.PrepareHandler.{PreparedStatementQuery, PreparedStatementResponse}
import org.scassandra.server.priming._
import org.scassandra.server.priming.prepared.PreparedStoreLookup
import org.scassandra.server.priming.query.Reply

import scala.concurrent.duration._
import scala.language.postfixOps

class ExecuteHandler(primePreparedStore: PreparedStoreLookup, activityLog: ActivityLog, prepareHandler: ActorRef) extends ProtocolActor {

  import context.dispatcher
  implicit val timeout: Timeout = 1 second

  def receive: Receive = {
    case ProtocolMessage(Frame(header, e: Execute)) =>
      val id = e.id.toInt()
      val recipient = sender
      // Lookup the associated prepared statement for this execute and on completion
      // send a message to self indicating to handle the request.
      val executeRequest = (prepareHandler ? PreparedStatementQuery(List(id)))
        .mapTo[PreparedStatementResponse]
        .map(res => HandleExecute(res.prepared.get(id), header, e, recipient))
      executeRequest.pipeTo(self)
    case HandleExecute(query, header, execute, connection) =>
      handleExecute(query, header, execute, connection)
  }

  def handleExecute(preparedStatement: Option[(String, Prepared)], header: FrameHeader, execute: Execute, connection: ActorRef) = {
    implicit val protocolVersion = header.version.version
    preparedStatement match {
      case Some((queryText, prepared)) =>
        val prime = primePreparedStore(queryText, execute)
        prime.foreach(p => log.info("Found prime {}", p))

        // Decode query parameters using the prepared statement metadata.
        val dataTypes = prepared.preparedMetadata.columnSpec.map(_.dataType)

        val values = extractQueryVariables(queryText, execute.parameters.values.map(_.map(_.value)), dataTypes)
        values match {
          case Some(v) =>
            activityLog.recordPreparedStatementExecution(queryText, execute.parameters.consistency,
              execute.parameters.serialConsistency, v, dataTypes, execute.parameters.timestamp)
          case None =>
            activityLog.recordPreparedStatementExecution(queryText, execute.parameters.consistency,
              execute.parameters.serialConsistency, Nil, Nil, execute.parameters.timestamp)
        }


        writePrime(execute, prime, header, Some(connection), alternative=Some(Reply(VoidResult)), consistency = Some(execute.parameters.consistency))
      case None =>
        val errMsg = s"Could not find prepared statement with id: 0x${execute.id.toHex}"
        activityLog.recordPreparedStatementExecution(errMsg, execute.parameters.consistency,
          execute.parameters.serialConsistency, Nil, Nil, execute.parameters.timestamp)
        val unprepared = Unprepared(errMsg, execute.id)
        write(unprepared, header, Some(connection))
    }
  }
}

object ExecuteHandler {
  case class HandleExecute(query: Option[(String, Prepared)], header: FrameHeader, execute: Execute, connection: ActorRef)
}
