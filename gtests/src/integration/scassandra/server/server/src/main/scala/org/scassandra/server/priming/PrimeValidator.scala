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
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.{ProtocolVersion, Rows}
import org.scassandra.server.priming.query.{Prime, PrimeCriteria, Reply}
import scodec.Attempt
import scodec.Attempt.{Failure, Successful}
import scodec.bits.BitVector

class PrimeValidator extends LazyLogging {
  def validate(criteria: PrimeCriteria, prime: Prime, existingCriteria: List[PrimeCriteria]): PrimeAddResult = {
    // 1. Validate consistency
    validateConsistency(criteria, existingCriteria) match {
      case c: ConflictingPrimes => c
      // 2. Then validate column types
      case _ => validateColumnTypes(prime)
    }
  }

  private def validateConsistency(criteria: PrimeCriteria, existingCriteria: List[PrimeCriteria]): PrimeAddResult = {

    def intersectsExistingCriteria: (PrimeCriteria) => Boolean = {
      existing => existing.query == criteria.query && existing.consistency.intersect(criteria.consistency).nonEmpty
    }

    val intersectingCriteria = existingCriteria.filter(intersectsExistingCriteria)
    intersectingCriteria match {
      // exactly one intersecting criteria: if the criteria is the newly passed one, this is just an override. Otherwise, conflict.
      case list @ head :: Nil if head != criteria => ConflictingPrimes(list)
      // two or more intersecting criteria: this means one or more conflicts
      case list @ head :: second :: rest => ConflictingPrimes(list)
      // all other cases: carry on
      case _ => PrimeAddSuccess
    }
  }

  private def validateColumnTypes(prime: Prime): PrimeAddResult = prime match {
    case Reply(message, _, _) =>
      val typeMismatches = message match {
        case Rows(metadata, rows) =>
          val columnSpec = metadata.columnSpec.getOrElse(Nil).map(spec => (spec.name, spec.dataType)).toMap
          for {
            row <- rows
            (column, value) <- row.columns
            columnType = columnSpec.getOrElse(column, DataType.Varchar)
            if isTypeMismatch(value, columnType)
          } yield TypeMismatch(value, column, columnType.stringRep)
        case _ => Nil
      }

      typeMismatches match {
        case Nil => PrimeAddSuccess
        case l: List[TypeMismatch] => TypeMismatches(typeMismatches)
      }
    case _ => PrimeAddSuccess
  }

  private def isTypeMismatch(value: Any, dataType: DataType): Boolean = {
    convertValue(value, dataType) match {
      case Successful(_) => false
      case Failure(x) =>
        logger.warn(s"Unable to use provided prime value '$value' for given type ${dataType.stringRep}, Error Message: ${x.message}")
        true
    }
  }

  private def convertValue(value: Any, columnType: DataType): Attempt[BitVector] = {
    // Attempt to encode value, chances are that it will succeed in almost all cases.  The protocol version
    // here doesn't matter too much since we're not actually using the payload.
    columnType.codec(ProtocolVersion.latest).encode(value)
  }
}

abstract class PrimeAddResult

case class TypeMismatches(typeMismatches: List[TypeMismatch]) extends PrimeAddResult

case object PrimeAddSuccess extends PrimeAddResult

case class ConflictingPrimes(existingPrimes: List[PrimeCriteria]) extends PrimeAddResult

case class TypeMismatch(value: Any, name: String, columnType: String)

case class BadCriteria(message: String) extends PrimeAddResult
