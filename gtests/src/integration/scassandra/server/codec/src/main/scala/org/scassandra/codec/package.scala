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
package org.scassandra

import scodec.Attempt.{Failure, Successful}
import scodec.bits.{BitVector, ByteVector}
import scodec.{Attempt, Codec, DecodeResult}

import scala.util.{Try, Failure => TFailure}

package object codec {

  /**
    * Facilities the use of next in a manner where the desired type is provided.  i.e. next[ProtocolFlags](bytes).
    *
    * This will only work if a codec of the desired type is implicitly defined.
    *
    * @param c A lazy evaluated codec.
    */
  def next[T <: AnyRef](bytes: ByteVector)(implicit c: shapeless.Lazy[Codec[T]]): Try[(T, ByteVector)] = next(Codec[T], bytes)

  /**
    * For a given byte vector, attempt to decode bytes using the given codec and return the decoded value and the
    * remaining bytes.
    * @param codec Codec to use to decode byte.
    * @param bytes Bytes to decode.
    * @tparam T The desired type to decode.
    * @return If successful, the decoded value with the remaining bytes.  Otherwise a failure.
    */
  def next[T <: AnyRef](codec: Codec[T], bytes: ByteVector): Try[(T, ByteVector)] = {
    codec.decode(bytes.toBitVector) match {
      case Successful(DecodeResult(result, remainder)) => Try((result, remainder.bytes))
      case Failure(x) => TFailure(new Exception(x.toString))
    }
  }

  /**
    * Folds all attempts into one [[Attempt]], joining each [[BitVector]].  The first failure encountered will be the
    * one returned.
    *j
    * @param attempts The attempts to combined.
    * @return A combined [[Attempt]]
    */
  protected[codec] def foldAttempts(attempts: List[Attempt[BitVector]]): Attempt[BitVector] = {
    // Fold all attempts into one bit vector.  The first failure encountered
    // will be the one returned.
    attempts.fold[Attempt[BitVector]](Successful(BitVector.empty)) {
      (acc, e) => acc match {
        case Successful(accBits) => e match {
          case Successful(eBits) => Successful(accBits ++ eBits)
          case f: Failure => f
        }
        case f: Failure => f
      }
    }
  }
}
