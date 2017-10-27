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

import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.messages._
import org.scassandra.codec.{Execute, Prepare, Prepared, ProtocolVersion}
import org.scassandra.server.priming.query.{Prime, PrimeCriteria, Reply}
import org.scassandra.server.priming.{Defaulter, PrimeAddResult, PrimeAddSuccess}

trait PreparedStoreLookup {
  def apply(prepare: Prepare, preparedFactory: (PreparedMetadata, RowMetadata) => Prepared) : Option[Prime]
  def apply(queryText: String, execute: Execute)(implicit protocolVersion: ProtocolVersion) : Option[Prime]
}

object PreparedStoreLookup {
  def defaultPrepared(prepare: Prepare, preparedFactory: (PreparedMetadata, RowMetadata) => Prepared): Prime = {
    val numberOfParameters = prepare.query.toCharArray.count(_ == '?')
    val variableTypes = (0 until numberOfParameters)
      .map(num => ColumnSpecWithoutTable(num.toString, DataType.Varchar).asInstanceOf[ColumnSpec]).toList

    val metadata = PreparedMetadata(
      keyspace = Some("keyspace"),
      table = Some("table"),
      columnSpec = variableTypes
    )

    Reply(preparedFactory(metadata, NoRowMetadata))
  }
}

trait PreparedStore[I <: PreparedPrimeIncoming] extends PreparedStoreLookup {
  protected var primes: Map[PrimeCriteria, I] = Map()

  def primeCriteria(prime: I): PrimeCriteria

  def record(prime: I): PrimeAddResult = {
    val p: I = prime.withDefaults.asInstanceOf[I]
    primes += primeCriteria(p) -> p
    PrimeAddSuccess
  }

  def retrievePrimes(): List[I] = primes.values.toList
  def clear() = primes = Map()

  def apply(prepare: Prepare, preparedFactory: (PreparedMetadata, RowMetadata) => Prepared) : Option[Prime] = {
    // Find prime by text.
    val prime = primes.find { case (criteria, _) =>
      criteria.query.equals(prepare.query)
    }.map(_._2)

    prepared(prepare, prime, preparedFactory)
  }

  def prepared(prepare: Prepare, prime: Option[I], preparedFactory: (PreparedMetadata, RowMetadata) => Prepared) : Option[Prime] = {
    prime.map { p =>
      // Prefill variable types with the rows column spec data types + varchars for any extra variables in the query.
      val dataTypes = Defaulter.defaultVariableTypesToVarChar(Some(prepare.query), p.thenDo.variable_types).getOrElse(Nil)

      // build column spec from data types, we name the columns by indices.
      val variableSpec = dataTypes.indices
        .map(i => ColumnSpecWithoutTable(i.toString, dataTypes(i)).asInstanceOf[ColumnSpec])
        .toList

      val preparedMetadata = PreparedMetadata(
        keyspace = Some("keyspace"),
        table = Some("table"),
        columnSpec = variableSpec
      )
      Reply(preparedFactory(preparedMetadata, NoRowMetadata))
    }
  }
}
