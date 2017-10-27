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

import org.scassandra.codec.Notations.{int => cint, short => cshort, string => cstring}
import org.scassandra.codec.ProtocolVersion
import scodec.Codec
import scodec.codecs._

/**
  * The metadata returned in a [[org.scassandra.codec.Prepared]] result.  See section 4.2.5.4 of the spec.
  *
  * @param partitionKeyIndices Indicates which indices in the column spec are partition keys. v4+ only.
  * @param keyspace The keyspace name for the given query.  Only present if global_tables_spec in flags are true.
  * @param table The table name for the given query.  Only present if global_tables_spec in flags are true.
  * @param columnSpec The column spec for the result set of the query.
  */
case class PreparedMetadata(
  partitionKeyIndices: List[Int] = Nil,
  keyspace: Option[String] = None,
  table: Option[String] = None,
  columnSpec: List[ColumnSpec] = Nil
)

/**
  * Default prepared metadata with empty partition key indices, column spec and no global table spec.
  * This is to avoid repeated creation of default prepared metadata.
  */
object NoPreparedMetadata extends PreparedMetadata()

object PreparedMetadata {

  /**
    * @param protocolVersion protocol version to use to encode/decode.
    * @return Codec for [[PreparedMetadata]] which parses the query flags to determine which fields are present.
    */
  implicit def codec(implicit protocolVersion: ProtocolVersion): Codec[PreparedMetadata] =
    protocolVersion.preparedMetadataCodec

  private[codec] def codecForVersion(implicit protocolVersion: ProtocolVersion) = {
    // parse the flags as an int and pass it for the fields to determine presence.
    ("flags"               | cint).consume { (flags: Int) =>
    // parse the column count and pass it to sub fields to determine size of column spec list.
    ("columnCount"         | cint).consume { (columnCount: Int) =>
    ("partitionKeyIndices" | withDefaultValue(conditional(protocolVersion.version >= 4, listOfN(cint, cshort)), Nil)) ::
    ("keyspace"            | conditional(flags == 1, cstring))                                 ::
    ("table"               | conditional(flags == 1, cstring))                                 ::
    ("columnSpec"          | listOfN(provide(columnCount), ColumnSpec.codec(flags == 0)))
  // derive column count from the number of entries in column spec.
  }(_.tail.tail.tail.head.size)
  // derive flags from whether or not the keyspace is defined to determine global_tables_spec.
  }(data => if (data.tail.head.isDefined) 1 else 0)
  }.as[PreparedMetadata]
}