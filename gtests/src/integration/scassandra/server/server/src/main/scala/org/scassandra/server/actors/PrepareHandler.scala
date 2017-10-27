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

import akka.actor.Actor
import com.google.common.annotations.VisibleForTesting
import org.scassandra.codec._
import org.scassandra.codec.messages.{PreparedMetadata, RowMetadata}
import org.scassandra.server.actors.PrepareHandler.{PreparedStatementQuery, PreparedStatementResponse}
import org.scassandra.server.priming._
import org.scassandra.server.priming.prepared.PreparedStoreLookup
import org.scassandra.server.priming.query.{Fatal, Reply}
import scodec.bits.ByteVector


class PrepareHandler(primePreparedStore: PreparedStoreLookup, activityLog: ActivityLog) extends ProtocolActor {

  private var nextId: Int = 1
  private var idToStatement: Map[Int, (String, Prepared)] = Map()

  @VisibleForTesting
  val generatePrepared = (p: PreparedMetadata, r: RowMetadata) => {
    val prepared = Prepared(ByteVector(nextId), preparedMetadata = p, resultMetadata = r)
    nextId += 1
    prepared
  }

  def receive: Actor.Receive = {
    case ProtocolMessage(Frame(header, p: Prepare)) =>
      activityLog.recordPreparedStatementPreparation(PreparedStatementPreparation(p.query))
      handlePrepare(header, p)
    case PreparedStatementQuery(ids) =>
      sender() ! PreparedStatementResponse(ids.flatMap(id => idToStatement.get(id) match {
        case Some(p) => Seq(id -> p)
        case None => Seq()
      }) toMap)
  }

  private def handlePrepare(header: FrameHeader, prepare: Prepare) = {
    val prime = primePreparedStore(prepare, generatePrepared)
      .getOrElse(PreparedStoreLookup.defaultPrepared(prepare, generatePrepared))

    prime match {
      case Reply(p: Prepared, _, _) =>
        idToStatement += (p.id.toInt() -> (prepare.query, p))
        log.info(s"Prepared Statement has been prepared: |$prepare.query|. Prepared result is: $p")
      case Reply(m: Message, _, _) =>
        log.info(s"Got non-prepared response for query: |$prepare.query|. Result was: $m")
      case f: Fatal =>
        log.info(s"Got non-prepared response for query: |$prepare.query|. Result was: $f")
    }

    writePrime(prepare, Some(prime), header)
  }
}

object PrepareHandler {
  case class PreparedStatementQuery(id: List[Int])
  case class PreparedStatementResponse(prepared: Map[Int, (String, Prepared)])
}