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

import org.scassandra.codec.Notations.{value, int => cint}
import org.scassandra.codec.{Bytes, Null, ProtocolVersion, Unset, _}
import scodec.Attempt.{Failure, Successful}
import scodec.bits.BitVector
import scodec.codecs._
import scodec.{Attempt, Codec, DecodeResult, Err, SizeBound}

import scala.collection.immutable
import scala.util.control.Breaks._

/**
  * Codec for parsing a given [[DataType.Tuple]] definition.
  *
  * This works similarly to [[org.scassandra.codec.messages.RowCodec]] in that it takes the ordering of the tuple's
  * elements to decode and encode in order.
  *
  * @param tuple The tuple definition to provide [[Codec]] facilities for.
  * @param protocolVersion protocol version to use to encode/decode.
  */
case class TupleCodec(tuple: DataType.Tuple)(implicit protocolVersion: ProtocolVersion) extends Codec[List[_]] {

  lazy val codecsWithBytes = {
    tuple.elements.map(dataType => variableSizeBytes(cint, dataType.codec))
  }

  override def decode(bits: BitVector): Attempt[DecodeResult[List[_]]] = {
    var remaining = bits
    val data = immutable.List.newBuilder[Any]
    var failure: Option[Failure] = None

    breakable {
      // Decode each tuple element at a type.
      for (codec <- tuple.elements.map(_.codec)) {
        // first parse using value codec to detect any possible Null values.
        value.decode(remaining) match {
          case Successful(DecodeResult(v, r)) =>
            remaining = r
            v match {
              case Bytes(b) => codec.decode(b.bits) match {
                case Successful(DecodeResult(aV, rem)) =>
                  data += aV
                case f: Failure =>
                  failure = Some(f)
                  break
              }
              case Null => data += null
              case Unset => failure = Some(Failure(Err("Found UNSET [value] in data, this is unexpected")))
            }
          case f: Failure => failure = Some(f)
        }
      }
    }

    failure match {
      case Some(f) => f
      case None => Successful(DecodeResult(data.result, remaining))
    }
  }

  override def encode(data: List[_]): Attempt[BitVector] = {
    if(data.length != tuple.elements.length) {
      Failure(Err(s"List of size ${data.size} does not match number of expected codec elements for $tuple"))
    } else {
      // Encode each element with it's corresponding data type.
      val attempts: List[Attempt[BitVector]] = data.zip(codecsWithBytes).map {
        // Special case, encode nulls as -1
        case (null, _) => value.encode(Null)
        case (v: Any, codec: Codec[_]) => codec.encode(v)
      }

      foldAttempts(attempts)
    }
  }

  // since each tuple value is encoded as a [value] simply make the bound tupleElementCount * [value].sizeBound
  override val sizeBound: SizeBound = SizeBound(value.sizeBound.lowerBound * tuple.elements.size, value.sizeBound.upperBound.map(_ * tuple.elements.size))
}