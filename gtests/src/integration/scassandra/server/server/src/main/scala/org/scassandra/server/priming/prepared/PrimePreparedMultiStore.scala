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
import org.scassandra.codec.datatype.DataType
import org.scassandra.server.priming.Defaulter
import org.scassandra.server.priming.query.{Prime, PrimeCriteria}

class PrimePreparedMultiStore extends PreparedStore[PrimePreparedMulti] with LazyLogging {

  def apply(queryText: String, execute: Execute)(implicit protocolVersion: ProtocolVersion): Option[Prime] = {
    // Find prime matching queryText and execute's consistency.
    val prime = primes.find { case (criteria, _) =>
      // if no consistency specified in the prime, allow all
      criteria.query.equals(queryText) && criteria.consistency.contains(execute.parameters.consistency)
    }.map(_._2)

    // Find the outcome action matching the execute parameters.
    val action = prime.flatMap { p =>
      // decode query parameters using the variable types of the prime.
      val dataTypes = Defaulter.defaultVariableTypesToVarChar(Some(queryText), p.thenDo.variable_types).getOrElse(Nil)
      val queryValues = execute.parameters.values.getOrElse(Nil)
      // TODO: handle named and unset values
      val values: List[Option[Any]] = dataTypes.zip(queryValues).map { case (dataType: DataType, queryValue: QueryValue) =>
        queryValue.value match {
          case Null => Some(null)
          case Unset => Some(null)
          case Bytes(bytes) => dataType.codec.decode(bytes.toBitVector).toOption.map(_.value)
        }
      }

      // Try to find an outcome whose criteria maps to the query parameters.
      p.thenDo.outcomes.find { outcome =>
        val matchers = outcome.criteria.variable_matcher
        // ensure lists are of the same length.
        logger.info(s"Outcome: $outcome matchers $matchers dataTypes $dataTypes")
        if (values.size == matchers.size) {
          // Iterate through each value and check that it matches against the value.
          (values, dataTypes, matchers).zipped.forall {
            case (value, dataType, matcher) =>
              val m = matcher.test(value, dataType)
              logger.info(s"$value $dataType $matcher $m")
              m
          }
        } else {
          false
        }
      }.map(_.action)
    }

    action.map(_.prime)
  }

  override def primeCriteria(prime: PrimePreparedMulti): PrimeCriteria =
    PrimeCriteria(prime.when.query.get, prime.when.consistency.get)
}
