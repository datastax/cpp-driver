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
package org.scassandra.codec

import java.net.{InetAddress, InetSocketAddress, UnknownHostException}

import org.scassandra.codec.datatype.DataType
import scodec.Attempt.{Failure, Successful}
import scodec.bits.{BitVector, ByteVector}
import scodec.codecs._
import scodec.{Attempt, Codec, DecodeResult, Err, SizeBound}

/**
  * Represents a column Value, which can either be Null (-1), Unset (-2) or have content (length + bytes)
  */
sealed trait Value
case object Null extends Value
case object Unset extends Value
case class Bytes(bytes: ByteVector) extends Value

/**
  * Represents a [[Value]] that is provided as part of a query can optionally have a named (protocol v2+)
  * @param name The name/bindmarker for this value.
  * @param value The actual value.
  */
case class QueryValue(name: Option[String], value: Value)

object QueryValue {
  /**
    * Creates an unnamed [[QueryValue]] with a given value and [[DataType]] that is used to encode it.
    * @param value The value
    * @param dataType The [[DataType]] to use to encode the value
    * @param protocolVersion [[ProtocolVersion]] to serialize with
    * @return The value encoded and wrapped with [[QueryValue]]
    */
  def apply(value: Any, dataType: DataType)(implicit protocolVersion: ProtocolVersion): QueryValue =
    QueryValue(None, Bytes(dataType.codec.encode(value).require.bytes))

  /**
    * Creates a named [[QueryValue]] with a given value and [[DataType]] that is use dto encode it.
    * @param name The name of the [[QueryValue]]
    * @param value The value
    * @param dataType The [[DataType]] to use to encode the value
    * @param protocolVersion [[ProtocolVersion]] to serialize with
    * @return The value encoded and wrapped with [[QueryValue]]
    */
  def apply(name: String, value: Any, dataType: DataType)(implicit protocolVersion: ProtocolVersion): QueryValue =
    QueryValue(Some(name), Bytes(dataType.codec.encode(value).require.bytes))
}

/**
  * The [[Codec]] for [[Value]].  Will encode [[Null]] as a 32-bit int with value of -1.  Will encode [[Unset]] as a
  * 32-bit int with value of -2.  [[Bytes]] will be encoded with 32-bit length + the actual bytes.
  */
object ValueCodec extends Codec[Value] {
  import Notations.int

  override def decode(bits: BitVector): Attempt[DecodeResult[Value]] = {
    int.decode(bits) match {
      case Successful(DecodeResult(count, remainder)) => count match {
        case -1          => Successful(DecodeResult(Null, remainder))
        case -2          => Successful(DecodeResult(Unset, remainder))
        case i if i >= 0 => variableSizeBytes(provide(i), bytes).decode(remainder).map(_.map(Bytes))
        case _           => Failure(Err(s"Invalid [value] identifier $count"))
      }
      case f: Failure => f
    }
  }

  override def encode(value: Value): Attempt[BitVector] = value match {
    case Null     => int.encode(-1)
    case Unset    => int.encode(-2)
    case Bytes(b) => int.encode(b.length.toInt).map(_ ++ b.toBitVector)
  }

  // The smallest value is int length
  // the highest value is int length + max int.
  override val sizeBound: SizeBound = SizeBound.bounded(int.sizeBound.lowerBound, int.sizeBound.lowerBound + Int.MaxValue.toLong*8)
}

/**
  * A [[Codec]] for serializing [[InetSocketAddress]] as defined by the native protocol spec.   See [inet] in the
  * native protocol specification for more details.
  */
private[codec] object InetAddressCodec extends Codec[InetSocketAddress] {
  import Notations.int

  override def decode(bits: BitVector): Attempt[DecodeResult[InetSocketAddress]] = {
    variableSizeBytes(uint8, bytes).decode(bits) match {
      case Successful(DecodeResult(bytes, remainder)) =>
        try {
          val address = InetAddress.getByAddress(bytes.toArray)
          int.decode(remainder) match {
            case Successful(DecodeResult(port, r)) => Successful(DecodeResult(new InetSocketAddress(address, port), r))
            case f: Failure => f
          }
        } catch {
          // handle case where given an invalid address by wrapping UHE exception in a failure.
          case u: UnknownHostException => Failure(Err(u.getMessage))
        }
      case f: Failure => f
    }
  }

  override def encode(value: InetSocketAddress): Attempt[BitVector] = {
    val address = value.getAddress.getAddress
    for {
      size <- uint8.encode(address.length.toByte)
      bytes = BitVector(address)
      port <- int.encode(value.getPort)
    } yield size ++ bytes ++ port
  }

  override def sizeBound: SizeBound = {
    // base is the size of the address size + size of the port.
    val baseSize = uint8.sizeBound.lowerBound + int.sizeBound.lowerBound
    // min would be ipv4, max ipv6.
    SizeBound(baseSize + 4, Some(baseSize + 16))
  }
}

/**
  * Represents consistency levels.
  */
object Consistency extends Enumeration {
  type Consistency = Value
  val ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, LOCAL_ONE = Value

  val all = Consistency.values.iterator.toList

  implicit val codec: Codec[Consistency] = enumerated(Notations.short, Consistency)
}

/**
  * Representations of the notations defined in section 3 of the native protocol specification.
  */
object Notations {
  /**
    * [byte]
    */
  val byte = scodec.codecs.byte
  /**
    * [int]
    */
  val int = int32
  /**
    * [long]
    */
  val long = int64
  /**
    * [short]
    */
  val short = uint16
  /**
    * [string]
    */
  val string = variableSizeBytes(short, utf8)
  /**
    * [long string]
    */
  val longString = variableSizeBytes(int, utf8)
  /**
    * [uuid]
    */
  val uuid = scodec.codecs.uuid
  /**
    * [string list]
    */
  val stringList = listOfN(short, string)
  /**
    * [bytes]
    */
  val bytes = variableSizeBytes(int, scodec.codecs.bytes)
  /**
    * [value]
    */
  val value: Codec[Value] = ValueCodec
  /**
    * [short bytes]
    */
  val shortBytes = variableSizeBytes(short, scodec.codecs.bytes)
  /**
    * [inet]
    */
  val inet: Codec[InetSocketAddress] = InetAddressCodec
  /**
    * [consistency]
    */
  val consistency = Consistency.codec
  /**
    * [string map]
   */
  val stringMap = map(short, string, string)
  /**
    * [string multimap]
    */
  val stringMultimap = map(short, string, stringList)
  /**
    * [bytes map]
    */
  val bytesMap = map(short, string, bytes)

  private[this] val queryValueWithName = (conditional(included=true, cstring) :: value).as[QueryValue]
  private[this] val queryValueWithoutName = (conditional(included=false, cstring) :: value).as[QueryValue]

  /**
    * Used to parse a query value.
    * @param includeName Whether or not the values should be named.
    * @return the appropriate [[QueryValue]] [[Codec]].
    */
  def queryValue(includeName: Boolean) = if(includeName) queryValueWithName else queryValueWithoutName

  /**
    * [map]
    * @param countCodec How to measure the number of entries in the map.
    * @param keyCodec Used for parsing the key values.
    * @param valCodec Used for parsing the values.
    * @tparam K Type of the keys
    * @tparam V Type of the values
    * @return [[Codec]] for the requested map.
    */
  def map[K,V](countCodec: Codec[Int], keyCodec: Codec[K], valCodec: Codec[V]): Codec[Map[K,V]] =
    listOfN(countCodec, (keyCodec :: valCodec).as[(K, V)]).xmap[Map[K,V]](_.toMap, _.toList)
}