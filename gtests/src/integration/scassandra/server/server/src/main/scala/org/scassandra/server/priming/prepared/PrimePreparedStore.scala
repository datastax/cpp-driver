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
import org.scassandra.server.priming.query.{Prime, PrimeCriteria}
import org.scassandra.server.priming.{PrimeAddResult, PrimeAddSuccess, PrimeValidator}

class PrimePreparedStore extends PreparedStore[PrimePreparedSingle] with LazyLogging {

  val validator: PrimeValidator = new PrimeValidator

  override def record(prime: PrimePreparedSingle) = {
    val p = prime.withDefaults
    val criteria = primeCriteria(p)
    validator.validate(criteria, p.thenDo.prime, primes.keys.toList) match {
      case PrimeAddSuccess =>
        logger.info(s"Storing prime for prepared statement $p with prime criteria $criteria")
        primes += (criteria -> p)
        PrimeAddSuccess
      case notSuccess: PrimeAddResult =>
        logger.info(s"Storing prime for prepared statement $p failed due to $notSuccess")
        notSuccess
    }
  }

  def apply(queryText: String, execute: Execute)(implicit protocolVersion: ProtocolVersion) : Option[Prime] = {
    // Find prime matching queryText and execute's consistency.
    val prime = primes.find { case (criteria, _) =>
      // if no consistency specified in the prime, allow all
      criteria.query.equals(queryText) && criteria.consistency.contains(execute.parameters.consistency)
    }.map(_._2)

    prime.map(_.thenDo.prime)
  }

  override def primeCriteria(prime: PrimePreparedSingle): PrimeCriteria =
    PrimeCriteria(prime.when.query.get, prime.when.consistency.get)
}
