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
package org.scassandra.codec.messages

import org.scassandra.codec.Notations.{longString, shortBytes, value, short => cshort}
import org.scassandra.codec.Value
import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs._

/**
  * Defines the type of batch.  See section 4.1.7 of spec.
  */
object BatchType extends Enumeration {
  type BatchType = Value
  val LOGGED, UNLOGGED, COUNTER = Value

  implicit val codec: Codec[BatchType] = enumerated(uint8, BatchType)
}

/**
  * Specifies the kind of query in a batch which is either a simple of prepared
  * statement.  A Batch can use a mixture of these.
  */
object BatchQueryKind extends Enumeration {
  type BatchQueryKind = Value
  val Simple, Prepared = Value

  implicit val codec: Codec[BatchQueryKind] = enumerated(uint8, BatchQueryKind)
}

private[codec] case class BatchFlags(
  namesForValues: Boolean = false,
  withDefaultTimestamp: Boolean = false,
  withSerialConsistency: Boolean = false
)

private [codec] object BatchFlags {
  implicit val codec: Codec[BatchFlags] = {
    ("reserved"          | ignore(1)) ::
    ("namesForValues"    | bool)      :: // Note that namesForValues is not currently used, see CASSANDRA-10246
    ("defaultTimestamp"  | bool)      ::
    ("serialConsistency" | bool)      ::
    ("reserved"          | ignore(4)) // the first 4 bits are unused.
  }.as[BatchFlags]
}

private[codec] object DefaultBatchFlags extends BatchFlags()

sealed trait BatchQuery {
  val values: List[Value]
}

object BatchQuery {
  /**
    * Codec for [[BatchQuery]] which parses based on whether or not a query is simple or prepared.
    */
  implicit val codec: Codec[BatchQuery] = discriminated[BatchQuery].by(BatchQueryKind.codec)
    .typecase(BatchQueryKind.Simple, Codec[SimpleBatchQuery])
    .typecase(BatchQueryKind.Prepared, Codec[PreparedBatchQuery])
}

/**
  * Defines a query as part of a batch that is a simple statement.
  * @param query The CQL query text.
  * @param values The values associated with this statement.
  */
case class SimpleBatchQuery(query: String, values: List[Value] = Nil) extends BatchQuery

object SimpleBatchQuery {
  /**
    * Codec for [[SimpleBatchQuery]] which parses the query string as a [long string] and then the values
    * as a list.
    */
  implicit val codec: Codec[SimpleBatchQuery] = (longString :: listOfN(cshort, value)).as[SimpleBatchQuery]
}

/**
  * Defines a query as part of a batch that is a bound prepared statement.
  * @param id The id associated with the prepared statement.
  * @param values The values associated with this statement.
  */
case class PreparedBatchQuery(id: ByteVector, values: List[Value] = Nil) extends BatchQuery

object PreparedBatchQuery {
  /**
    * Codec for [[PreparedBatchQuery]] that pareses the prepared id as [short bytes] and the values as a list.
    */
  implicit val codec: Codec[PreparedBatchQuery] = (shortBytes :: listOfN(cshort, value)).as[PreparedBatchQuery]
}
