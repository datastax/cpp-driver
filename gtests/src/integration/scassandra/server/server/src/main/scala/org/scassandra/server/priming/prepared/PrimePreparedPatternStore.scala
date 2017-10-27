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
package org.scassandra.server.priming.prepared

import com.typesafe.scalalogging.LazyLogging
import org.scassandra.codec._
import org.scassandra.codec.messages.{PreparedMetadata, RowMetadata}
import org.scassandra.server.priming.query.{Prime, PrimeCriteria}

class PrimePreparedPatternStore extends PreparedStore[PrimePreparedSingle] with LazyLogging {

  override def apply(prepare: Prepare, preparedFactory: (PreparedMetadata, RowMetadata) => Prepared) : Option[Prime] = {
    // Find prime by pattern.
    val prime = primes.find(_._1.query.r.findFirstIn(prepare.query).isDefined).map(_._2)
    prepared(prepare, prime, preparedFactory)
  }

  def apply(queryText: String, execute: Execute)(implicit protocolVersion: ProtocolVersion) : Option[Prime] = {
    // Find prime with query pattern matching queryText and execute's consistency.
    val prime = primes.find { case (criteria, _) =>
      // if no consistency specified in the prime, allow all
      criteria.query.r.findFirstIn(queryText).isDefined && criteria.consistency.contains(execute.parameters.consistency)
    }.map(_._2)

    prime.map(_.thenDo.prime)
  }

  override def primeCriteria(prime: PrimePreparedSingle): PrimeCriteria =
    PrimeCriteria(prime.when.queryPattern.get, prime.when.consistency.get)
}
