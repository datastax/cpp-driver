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

import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.{Frame, NoRows, Query}
import org.scassandra.server.priming._
import org.scassandra.server.priming.query.{PrimeQueryStore, Reply}

class QueryHandler(primeQueryStore: PrimeQueryStore, activityLog: ActivityLog) extends ProtocolActor {

  val noRows = Some(Reply(NoRows))

  def receive = {
    case ProtocolMessage(Frame(header, query: Query)) =>
      implicit val protocolVersion = header.version.version
      val prime = primeQueryStore(query)

      // Attempt to extract values if variable types are present in the prime.
      val typesAndValues: Option[(List[DataType], List[Any])] = prime
        .flatMap(_.variableTypes)
        .flatMap { variableTypes =>
          Some(variableTypes).zip(extractQueryVariables(query.query, query.parameters.values.map(_.map(_.value)), variableTypes)).headOption
        }

      typesAndValues match {
        case Some((variableTypes, values)) =>
          activityLog.recordQuery(query.query, query.parameters.consistency, query.parameters.serialConsistency, values,
            variableTypes, query.parameters.timestamp)
        case None =>
          activityLog.recordQuery(query.query, query.parameters.consistency, query.parameters.serialConsistency,
            timestamp = query.parameters.timestamp)
      }

      writePrime(query, prime, header, alternative = noRows, consistency = Some(query.parameters.consistency))
      log.info(s"Incoming query: $query")
  }
}
