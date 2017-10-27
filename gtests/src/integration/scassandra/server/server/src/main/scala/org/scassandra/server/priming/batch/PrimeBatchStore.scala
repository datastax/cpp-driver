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
package org.scassandra.server.priming.batch

import com.typesafe.scalalogging.LazyLogging
import org.scassandra.codec.Consistency.Consistency
import org.scassandra.codec.messages.BatchType.BatchType
import org.scassandra.server.priming.BatchExecution
import org.scassandra.server.priming.query.Prime

class PrimeBatchStore extends LazyLogging {

  var primes: Map[BatchCriteria, BatchPrimeSingle] = Map()

  def record(prime: BatchPrimeSingle): Unit = {
    val p = prime.withDefaults
    val criteria = BatchCriteria(p.when.queries, p.when.consistency.get, p.when.batchType.get)
    primes += (criteria -> p)
  }

  def apply(primeMatch: BatchExecution): Option[Prime] = {
    logger.debug("Batch Prime Match {} current primes {}", primeMatch, primes)
    primes.find {
      case (batchCriteria, _) =>
        batchCriteria.queries == primeMatch.batchQueries.map(bq => BatchQueryPrime(bq.query, bq.batchQueryKind)) &&
          batchCriteria.consistency.contains(primeMatch.consistency) &&
          batchCriteria.batchType == primeMatch.batchType
    } map(_._2.prime)
  }
  def clear() = {
    primes = Map()
  }
}

case class BatchCriteria(queries: Seq[BatchQueryPrime], consistency: List[Consistency], batchType: BatchType)