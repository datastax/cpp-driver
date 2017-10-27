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
package org.scassandra.codec.datatype

import java.net.{InetAddress, UnknownHostException}
import java.nio.ByteBuffer
import java.util.UUID

import com.google.common.net.InetAddresses
import org.scassandra.codec.Notations.{map, int => cint, short => cshort}
import org.scassandra.codec._
import scodec.Attempt.{Failure, Successful}
import scodec.bits.{BitVector, ByteVector}
import scodec.codecs._
import scodec.{Codec, DecodeResult, Err, SizeBound}

/**
  * Represents a [[DataType]] defined by the protocol.  See section 6 for a description of the various data types.
  */
sealed trait DataType {

  /**
    * A partial function defining hwo to transform a value into a format that the data type's [[Codec]] knows
    * how to interpret.
    */
  val native: PartialFunction[Any, Any]

  /**
    * @param protocolVersion protocol version to use to encode/decode.
    * @return The [[Codec]] of the data type that knows how to encode/decode the data type's value.
    */
  protected[codec] def baseCodec(implicit protocolVersion: ProtocolVersion): Codec[_]

  /**
    * @param protocolVersion protocol version to use to encode/decode.
    * @return The [[Codec]] of teh data type that knows how to encode/decode the data type from any format that
    *         [[native]] accepts.
    */
  def codec(implicit protocolVersion: ProtocolVersion): Codec[Any] = protocolVersion.dataTypeCodec(this)

  /**
    * The string representation of the data type in cql.
    */
  val stringRep: String
}

object DataType {

  /**
    * A data type that is primitive in that its definition is not configurable.
    */
  sealed trait PrimitiveType extends DataType

  /**
    * An implicit that allows decorating a codec in a way that uses the given partial function to go from
    * the type the function allows to a type that the underlying codec understands.   For example if a 'Int' is
    * passed into the codec, but it only understands 'String', the partial function could convert the int value
    * into a string.
    * @param codec the codec to decorate with the partial function.
    */
  implicit class AnyCodecDecorators(codec: Codec[Any]) {
    def withAny(f: PartialFunction[Any, Any]): Codec[Any] = {
      // Widen the codec on the encode side by allowing it to accept what the partial function accepts.
      codec.widen(
        x => x,
        (a: Any) => {
          // Can only be successful if the partial function accepts the value.
          if (f.isDefinedAt(a)) {
            try {
              val res = f(a)
              Successful(res)
            } catch {
              case(e: Throwable) => Failure(Err(s"${e.getClass.getName}(${e.getMessage})"))
            }
          } else {
            Failure(Err(s"Unsure how to encode $a"))
          }
        }
      )
    }
  }

  // TODO: Could use macro instead to resolve all the primitive types instead of keeping a separate list.
  private [this] lazy val primitiveTypes = scala.List(
    Ascii, Bigint, Blob, Boolean, Counter, Decimal, Double, Float, Int, Text, Timestamp, Uuid, Varchar, Varint,
    Timeuuid, Inet, Date, Time, Smallint, Tinyint
  )

  /**
    * A listing of all primtive data types.
    */
  lazy val primitiveTypeMap = {
    primitiveTypes.map(t => (t.stringRep, t)).toMap
  }

  /**
    * @param protocolVersion protocol version to use to encode/decode.
    * @return Codec for [[DataType]].
    */
  implicit def codec(implicit protocolVersion: ProtocolVersion): Codec[DataType] = protocolVersion.dataTypeCodec

  case class Custom(className: String) extends DataType {
    val stringRep = s"'$className'"

    /**
      * @see [[Blob#native]]
      */
    val native: PartialFunction[Any, Any] = Blob.native

    def baseCodec(implicit protocolVersion: ProtocolVersion) = bytes
  }

  case object Ascii extends PrimitiveType {
    val stringRep = "ascii"

    /**
      * Accepts any String as is, and converts anything that isn't a collection to its toString implementation.
      */
    val native: PartialFunction[Any, Any] = {
      case s: String => s
      case x: Any if !x.isInstanceOf[Iterable[_]] && !x.isInstanceOf[scala.Predef.Map[_,_]] => x.toString
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = ascii
  }

  case object Bigint extends PrimitiveType {
    val stringRep = "bigint"

    /**
      * Converts String using [[String#toLong]] and any Number using [[Number#longValue]]
      */
    val native: PartialFunction[Any, Any] = {
      case s: String => s.toLong
      case x: Number => x.longValue()
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = int64
  }

  case object Blob extends PrimitiveType {
    val stringRep = "blob"

    /**
      * Accepts [[Array]] of [[Byte]], [[ByteVector]], [[BitVector]], [[ByteBuffer]] and converts [[String]] using
      * [[ByteVector#fromValidHex]] if it is valid hex.
      */
    val native: PartialFunction[Any, Any] = {
      case b: Array[Byte] => ByteVector(b)
      case b: ByteVector => b
      case b: BitVector => b.bytes
      case b: ByteBuffer => ByteVector(b)
      case s: String if ByteVector.fromHex(s.toLowerCase).isDefined => ByteVector.fromValidHex(s.toLowerCase)
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = bytes
  }

  case object Boolean extends PrimitiveType {
    val stringRep = "boolean"

    /**
      * Converts [[String]] using [[String#toBoolean]], [[Number]] by converting to [[String]] and using
      * [[String#toBoolean]].  Accepts [[Boolean]].
      */
    val native: PartialFunction[Any, Any] = {
      case s: String => s.toBoolean
      case x: Number => x.toString.toBoolean
      case b: Boolean => b
    }

    // a custom codec is used in favor of bool(8) because we want true to be represented as 0x01 instead of 0xFF
    def baseCodec(implicit protocolVersion: ProtocolVersion) = new Codec[Boolean] {
      private val zero = BitVector.lowByte
      private val one = BitVector.fromByte(1)
      private val codec = bits(8).xmap[Boolean](bits => !(bits == zero), b => if (b) one else zero)
      def sizeBound = SizeBound.exact(8)
      def encode(b: Boolean) = codec.encode(b)
      def decode(b: BitVector) = codec.decode(b)
      override def toString = s"boolean"
    }
  }

  case object Counter extends PrimitiveType {
    val stringRep = "counter"

    /**
      * @see [[BigInt#native]]
      */
    val native = Bigint.native

    def baseCodec(implicit protocolVersion: ProtocolVersion) = Bigint.baseCodec
  }

  case object Decimal extends PrimitiveType {
    val stringRep = "decimal"

    /**
      * Converts [[String]] and [[Number]] to [[BigDecimal]].  Accepts [[BigDecimal]].
      */
    val native: PartialFunction[Any, Any] = {
      case s: String => BigDecimal(s)
      case b: BigDecimal => b
      case x: Number => BigDecimal(x.doubleValue())
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = cint.pairedWith(Varint.baseCodec).exmap(
      (t: (Int, BigInt)) => Successful(BigDecimal(t._2, t._1)),
      (b: BigDecimal)    => Successful((b.scale, BigInt(b.bigDecimal.unscaledValue())))
    )
  }

  case object Double extends PrimitiveType {
    val stringRep = "double"

    /**
      * Converts [[String]] using [[String#toDouble]].  [[Number]] using [[Number#doubleValue]].
      */
    val native: PartialFunction[Any, Any] = {
      case s: String => s.toDouble
      case x: Number => x.doubleValue()
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = double
  }

  case object Float extends PrimitiveType {
    val stringRep = "float"

    /**
      * Converts [[String]] using [[String#toFloat]].  [[Number]] using [[Number#floatValue]].
      */
    val native: PartialFunction[Any, Any] = {
      case s: String => s.toFloat
      case x: Number => x.floatValue()
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = float
  }

  case object Int extends PrimitiveType {
    val stringRep = "int"

    /**
      * Converts [[String]] using [[String#toInt]].  [[Number]] using [[Number#intValue]].
      */
    val native: PartialFunction[Any, Any] = {
      case s: String => s.toInt
      case x: Number => x.intValue()
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = int32
  }

  case object Timestamp extends PrimitiveType {
    val stringRep = "timestamp"

    /**
      * @see [[Bigint#native]]
      */
    val native = Bigint.native

    def baseCodec(implicit protocolVersion: ProtocolVersion) = Bigint.baseCodec
  }

  case object Uuid extends PrimitiveType {
    val stringRep = "uuid"

    /**
      * Converts [[String]] using [[UUID#fromString]].  Accepts [[UUID]].
      */
    val native: PartialFunction[Any, Any] = {
      case s: String => UUID.fromString(s)
      case u: UUID => u
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = uuid
  }

  case object Varchar extends PrimitiveType {
    val stringRep = "varchar"

    /**
      * Accepts any String as is, and converts anything that isn't a collection to its toString implementation.
      */
    val native: PartialFunction[Any, Any] = {
      case s: String => s
      case x: Any if !x.isInstanceOf[Iterable[_]] && !x.isInstanceOf[scala.Predef.Map[_,_]] => x.toString
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = utf8
  }

  case object Text extends PrimitiveType {
    val stringRep = "text"

    /**
      * @see [[Varchar#native]]
      */
    val native: PartialFunction[Any, Any] = Varchar.native

    def baseCodec(implicit protocolVersion: ProtocolVersion) = utf8
  }

  case object Varint extends PrimitiveType {
    val stringRep = "varint"

    /**
      * Converts [[String]] using [[BigInt]].  Converts [[Number]] using [[BigInt]] with [[Number#longValue]].
      * Accepts [[BigInt]].
      */
    val native: PartialFunction[Any, Any] = {
      case s: String => BigInt(s)
      case b: BigInt => b
      case x: Number => BigInt(x.longValue())
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = Codec(
      (b: BigInt) => Successful(BitVector(b.toByteArray)),
      (b: BitVector) => Successful(DecodeResult(BigInt(b.toByteArray), BitVector.empty))
    )
  }

  case object Timeuuid extends PrimitiveType {
    val stringRep = "timeuuid"

    /**
      * @see [[Uuid#native]]
      */
    val native = Uuid.native

    def baseCodec(implicit protocolVersion: ProtocolVersion) = Uuid.baseCodec
  }

  case object Inet extends PrimitiveType {
    val stringRep = "inet"

    /**
      * Converts [[String]] using [[InetAddresses#forString]] if its a valid ip address.  Accepts [[InetAddress]].
      */
    val native: PartialFunction[Any, Any] = {
      case s: String if InetAddresses.isInetAddress(s) => InetAddresses.forString(s)
      case a: InetAddress => a
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = Codec(
      (i: InetAddress) => Successful(BitVector(i.getAddress)),
      (b: BitVector) => try {
        Successful(DecodeResult(InetAddress.getByAddress(b.toByteArray), BitVector.empty))
      } catch {
        case e: UnknownHostException => Failure(Err(e.getMessage))
      }
    )
  }

  case object Date extends PrimitiveType {
    val stringRep = "date"

    /**
      * Converts [[String]] using [[String#toLong]], converts [[Number]] using [[Number#longValue]].
      */
    val native: PartialFunction[Any, Any] = {
      // TODO: perhaps parse literal dates, i.e. 1985-01-01
      case s: String => s.toLong
      case x: Number => x.longValue()
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = uint32
  }

  case object Time extends PrimitiveType {
    val stringRep = "time"

    /**
      * @see [[Bigint#native]]
      */
    val native: PartialFunction[Any, Any] = Bigint.native

    def baseCodec(implicit protocolVersion: ProtocolVersion) = Bigint.baseCodec.widen(
      (in: Long) => in,
      (in: Long) => in match {
        case x if x >= 0 && x <= 86399999999999L => Successful(x)
        case x => Failure(Err(s"$x is not between 0 and 86399999999999"))
      }
    )
  }

  case object Smallint extends PrimitiveType {
    val stringRep = "smallint"

    /**
      * Converts [[String]] using [[String#toShort]], converts [[Number]] using [[Number#shortValue]].
      */
    val native: PartialFunction[Any, Any] = {
      case s: String => s.toShort
      case x: Number => x.shortValue()
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = short16
  }

  case object Tinyint extends PrimitiveType {
    val stringRep = "tinyint"

    /**
      * Converts [[String]] using [[String#toByte]], converts [[Number]] using [[Number#byteValue]].
      */
    val native: PartialFunction[Any, Any] = {
      case s: String => s.toByte
      case x: Number => x.byteValue()
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = byte
  }

  case class List(element: DataType) extends DataType {
    val stringRep = s"list<${element.stringRep}>"

    /**
      * Accepts any [[TraversableOnce]].
      */
    val native: PartialFunction[Any, Any] = {
      case t: TraversableOnce[_] => t.toList
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = listOfN(protocolVersion.collectionLengthCodec,
      variableSizeBytes(protocolVersion.collectionLengthCodec, element.codec))
  }

  case class Map(key: DataType, value: DataType) extends DataType {
    val stringRep = s"map<${key.stringRep},${value.stringRep}>"

    /**
      * Accepts any [[scala.Predef.Map]].
      */
    val native: PartialFunction[Any, Any] = {
      case m: scala.Predef.Map[_, _] => m
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = map(protocolVersion.collectionLengthCodec,
      variableSizeBytes(protocolVersion.collectionLengthCodec, key.codec),
      variableSizeBytes(protocolVersion.collectionLengthCodec, value.codec))
  }

  case class Set(element: DataType) extends DataType {
    val stringRep = s"set<${element.stringRep}>"

    /**
      * Accepts any [[TraversableOnce]] as a [[scala.Predef.Set]].
      */
    val native: PartialFunction[Any, Any] = {
      case t: TraversableOnce[_] => t.toSet
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = listOfN(protocolVersion.collectionLengthCodec,
      variableSizeBytes(protocolVersion.collectionLengthCodec, element.codec)).xmap(
      (f: scala.List[Any]) => f.toSet,
      (f: scala.Predef.Set[Any]) => f.toList
    )
  }

  case class Tuple(elements: scala.List[DataType]) extends DataType {

    val stringRep = s"tuple<${elements.map(_.stringRep).mkString(",")}>"

    /**
      * Accepts any [[TraversableOnce]] as a [[scala.List]].  Accepts any [[Product]] by converting to list
      * using [[Product#productIterator]].  Note that this can be problematic for unterminating product iterators.
      */
    val native: PartialFunction[Any, Any] = {
      case a: TraversableOnce[_] => a.toList
      // Allows encoding of any TupleXX
      case p: Product => p.productIterator.toList
    }

    def baseCodec(implicit protocolVersion: ProtocolVersion) = TupleCodec(this)
  }

  object Tuple {
    /**
      * Convenience method to create a [[Tuple]] from [[DataType]] arguments.
      * @param elements [[DataType]] to create [[Tuple]] from.
      * @return The created [[Tuple]] type.
      */
    def apply(elements: DataType*): Tuple = Tuple(elements.toList)
  }
}
