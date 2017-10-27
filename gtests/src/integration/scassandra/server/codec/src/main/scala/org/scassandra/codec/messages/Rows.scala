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

import org.scassandra.codec.Notations.{value, bytes => cbytes, int => cint, string => cstring}
import org.scassandra.codec._
import org.scassandra.codec.datatype.DataType
import scodec.Attempt.{Failure, Successful}
import scodec.bits.{BitVector, ByteVector}
import scodec.codecs._
import scodec.{Attempt, Codec, DecodeResult, SizeBound}
import shapeless.{::, HNil}

import scala.collection.immutable
import scala.util.control.Breaks._

/**
  * Defines row metadata included in [[Rows]] result.  See section 4.2.5.2 of the spec.
  * @param pagingState The paging state to continue paging in the next query.
  * @param keyspace The keyspace name for the given query.  Only present if global_tables_spec in flags are true.
  * @param table The table name for the given query.  Only present if global_tables_spec in flags are true.
  * @param columnSpec The column spec for the result set of the query.  Only present if no_metadata flag is false.
  */
case class RowMetadata(
  pagingState: Option[ByteVector] = None,
  keyspace: Option[String] = None,
  table: Option[String] = None,
  columnSpec: Option[List[ColumnSpec]] = None
)

/**
  * A response that indicates that there is no row metadata, however global_tables_spec is true.  This is just to
  * indicate that there is no metadata included in the response.  This should only be used in the case when a
  * non-prepared statement is made.  Otherwise [[NoRowMetadataRequested]] should be used.
  */
object NoRowMetadata extends RowMetadata(columnSpec = Some(Nil))

/**
  * A response that indicates there was no row metadata requested by the client.
  */
object NoRowMetadataRequested extends RowMetadata()

object RowMetadata {
  /**
    * @param protocolVersion protocol version to use to encode/decode.
    * @return Codec for [[RowMetadata]] which parses the query flags to determine which fields are present.
    */
  implicit def codec(implicit protocolVersion: ProtocolVersion): Codec[RowMetadata] = protocolVersion.rowMetadataCodec

  private[codec] def codecForVersion(implicit protocolVersion: ProtocolVersion) = {
    // parse the flags and pass it for the fields to determine presence.
    ("flags"       | Codec[RowMetadataFlags]).consume { flags =>
    ("columnCount" | cint).consume { count =>
    ("pagingState" | conditional(flags.hasMorePages, cbytes))     ::
    ("keyspace"    | conditional(!flags.noMetadata && flags.globalTableSpec, cstring)) ::
    ("table"       | conditional(!flags.noMetadata && flags.globalTableSpec, cstring)) ::
    ("columnSpec"  | conditional(!flags.noMetadata, listOfN(provide(count), ColumnSpec.codec(!flags.globalTableSpec))))
  // derive column count from the number of entries in column spec
  }(_.tail.tail.tail.head.getOrElse(Nil).size)
  // derive flags from presence of fields
  }{ case pagingState :: keyspace :: table :: columnSpec :: HNil =>
     RowMetadataFlags(
       noMetadata = keyspace.isEmpty && table.isEmpty && columnSpec.isEmpty,
       hasMorePages = pagingState.isDefined,
       globalTableSpec = keyspace.isDefined && table.isDefined
     )
  }}.as[RowMetadata]
}

private[this] case class RowMetadataFlags(noMetadata: Boolean = false, hasMorePages: Boolean = false, globalTableSpec: Boolean = false)

private[this] object RowMetadataFlags {
  implicit val codec: Codec[RowMetadataFlags] = {
    ("reserved"        | ignore(29))::
    ("noMetadata"      | bool)      ::
    ("hasMorePages"    | bool)      ::
    ("globalTableSpec" | bool)
  }.dropUnits.as[RowMetadataFlags]
}

/**
  * Defines a column spec which contains the column/result name and its [[DataType]].
  */
sealed trait ColumnSpec {
  val name: String
  val dataType: DataType
}

/**
  * Defines a column spec that doesn't have the keyspace and table name.  These column specs exist when [[RowMetadata]]
  * has a global_table_spec, so keyspace and table names are not needed on every column spec.
  * @param name Name of the column/result alias.
  * @param dataType The [[DataType]] of the column.
  */
case class ColumnSpecWithoutTable(override val name: String, override val dataType: DataType) extends ColumnSpec

/**
  * Defines a column spec that includes keyspace and table name.  These column specs exist when [[RowMetadata]] does
  * not have a global table spec, so keyspace and table names are needed on every column spec.
  * @param keyspace The name of the keyspace for the table the column is part of.
  * @param table The name of the table the column is part of.
  * @param name Name of the column/result alias.
  * @param dataType The [[DataType]] of the column.
  */
case class ColumnSpecWithTable(keyspace: String, table: String, override val name: String, override val dataType: DataType) extends ColumnSpec

object ColumnSpec {

  /**
    * @param withTable Whether or not the [[ColumnSpec]] should include keyspace and table name.
    * @param protocolVersion protocol version to use to encode/decode.
    * @return Codec for either [[ColumnSpecWithoutTable]] or [[ColumnSpecWithTable]].
    */
  def codec(withTable: Boolean)(implicit protocolVersion: ProtocolVersion): Codec[ColumnSpec] = {
    if(withTable) {
      protocolVersion.columnSpecWithTableCodec
    } else {
      protocolVersion.columnSpecWithoutTableCodec
    }
  }

  /**
    * Creates a [[ColumnSpec]] with the given name and data type.
    * @param name Name of the column/result alias.
    * @param dataType The [[DataType]] of the column.
    * @return [[ColumnSpec]] with given name and data type.
    */
  def column(name: String, dataType: DataType) = ColumnSpecWithoutTable(name, dataType)

  private[codec] def codecForVersion(withTable: Boolean)(implicit protocolVersion: ProtocolVersion): Codec[ColumnSpec] = {
    if(withTable) {
      (cstring :: cstring :: cstring  :: Codec[DataType]).as[ColumnSpecWithTable].upcast[ColumnSpec]
    } else {
      (cstring :: Codec[DataType]).as[ColumnSpecWithoutTable].upcast[ColumnSpec]
    }
  }
}

/**
  * Defines an individual row in a [[Rows]] result.  A row effectively a map of column names to column values.
  * @param columns Map of column names to values.  The values are regular objects that have not been encoded in any way.
  */
case class Row(columns: Map[String, Any])

object Row {
  /**
    * @param spec Column spec for the rows columns.
    * @param protocolVersion protocol version to use to encode/decode.
    * @return Codec for [[Row]] with the given column spec.
    */
  def withColumnSpec(spec: List[ColumnSpec])(implicit protocolVersion: ProtocolVersion): Codec[Row] = RowCodec(spec)

  /**
    * Convenience function to create a [[Row]] for a listing of (Name, Value) pairs.
    * @param colPairs (Name, Value) pairs of [[Row]] columns.
    * @return The created [[Row]].
    */
  def apply(colPairs: (String, Any)*): Row = Row(colPairs.toMap)
}

/**
  * Defines a [[Codec]] for a [[Row]] with the given [[ColumnSpec]].
  *
  * To decode it consumes a [[BitVector]] by
  * processing each [[ColumnSpec]] in order and uses that spec's [[DataType]]'s codec to figure out how to decode
  * a value.
  *
  * To encode it processes the [[ColumnSpec]] of a [[Row]] in order and uses the [[DataType]] of the spec to
  * figure out how to encode bits from the value.  It then joins all of those bits into one [[BitVector]] and
  * returns it.
  *
  * @param columnSpecs [[ColumnSpec]] for a row definition.
  * @param protocolVersion protocol version to use to encode/decode.
  */
case class RowCodec(columnSpecs: List[ColumnSpec])(implicit protocolVersion: ProtocolVersion) extends Codec[Row] {

  /**
    * Wraps each [[ColumnSpec]]'s [[DataType]] [[Codec]] with a [value] to read the size of the each column
    * before parsing it.
    */
  private[this] lazy val columnsWithCodecs: List[(String, Codec[Any])] = columnSpecs.map { (spec: ColumnSpec) =>
    (spec.name, variableSizeBytes(cint, spec.dataType.codec))
  }

  override def decode(bits: BitVector): Attempt[DecodeResult[Row]] = {
    var remaining = bits
    val data = immutable.List.newBuilder[(String, _ <: Any)]
    var result: Option[Attempt[DecodeResult[Row]]] = None

    // for each column spec, attempt to decode value
    // if for some reason we fail to decode a value, we need to break out of the loop.
    breakable {
      for (spec <- columnSpecs) {
        val codec = spec.dataType.codec
        // first parse using value codec to detect any possible Null/Unset values.
        value.decode(bits) match {
          case Successful(DecodeResult(v, r)) =>
            remaining = r
            val value: Option[Any] = v match {
              // Decode using actual codec.
              case Bytes(b) => codec.decode(b.bits) match {
                case Successful(DecodeResult(aV, _)) => Some(aV)
                case f: Failure =>
                  // Failure to decode, break out and set error.
                  result = Some(f)
                  break
              }
              // Null value will store null in map.
              case Null => Some(null)
              // Unset value will do nothing.
              case Unset => None
            }
            // If there was a set value or null add it to map.
            value match {
              case Some(s) => data += ((spec.name, s))
              case None => ()
            }
          // Failure to decode into bytes, it is unlikely decoding a Value will fail but handle nonetheless.
          case f: Failure =>
            result = Some(f)
            break
        }
      }
    }

    // If any failure is encountered return that, otherwise build the map and create a result.
    result match {
      case Some(err) => err
      case None => Successful(DecodeResult(Row(data.result:_*), remaining))
    }
  }

  override def encode(row: Row): Attempt[BitVector] = {
    // Encode each column as it appears in the column spec.
    val attempts: List[Attempt[BitVector]] = columnsWithCodecs.map { (column) =>
      val codec = column._2
      val columnName = column._1
      row.columns.get(columnName) match {
        case Some(null) => value.encode(Null)
        case Some(data) => codec.encode(data)
        case None => value.encode(Unset)
      }
    }

    // fold all spec encodings into one.
    foldAttempts(attempts)
  }

  private [this] lazy val sizes = {
    // Iterate over each spec codec to determine the size bound of it and then sum it up.
    columnsWithCodecs.map(_._2.sizeBound).fold(SizeBound.exact(0)) { (acc: SizeBound, s: SizeBound) =>
      acc + s
    }
  }

  override def sizeBound: SizeBound = sizes
}
