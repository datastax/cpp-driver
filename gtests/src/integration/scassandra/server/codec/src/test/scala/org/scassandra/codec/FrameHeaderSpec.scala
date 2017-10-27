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

import org.scalacheck.Gen
import org.scalacheck.Prop.{BooleanOperators, forAll, all => allProps}
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}
import scodec.bits.{BitVector, ByteVector}
import scodec.codecs._
import scodec.{Attempt, DecodeResult}

class FrameHeaderSpec extends FlatSpec with Checkers with Matchers {

  "ProtocolVersion" must "properly decode for valid versions." in {
    val codec = ProtocolVersion.codec

    check {
      forAll(Gen.choose(1, 4)) {
        version => {
          codec.decode(BitVector(version).drop(1)) match {
            case Attempt.Successful(DecodeResult(decoded, remainder)) =>
              s"protocol = $decoded" |: allProps(
                ("identical version" |: decoded.version == version) &&
                (s"no remaining data(${remainder.size})" |: remainder.size == 0)
              )
            case Attempt.Failure(cause) =>
              s"$cause" |: false
          }
        }
      }
    }
  }

  it must "return UnsupportedProtocolVersion when given invalid version." in {
    val codec = ProtocolVersion.codec

    check {
      forAll(Gen.chooseNum(5, 127, 0)) {
        version => {
          codec.decode(BitVector(version).drop(1)) match {
            case Attempt.Successful(DecodeResult(decoded, remainder)) =>
              s"protocol = $decoded" |: allProps(
                ("invalid protocol version" |: decoded.isInstanceOf[UnsupportedProtocolVersion]) &&
                ("identical version" |: decoded.version == version) &&
                (s"no remaining data(${remainder.size})" |: remainder.size == 0)
              )
            case Attempt.Failure(cause) =>
              s"$cause" |: false
          }
        }
      }
    }
  }

  "ProtocolFlags" must "appropriately derive direction and version." in {
    val codec = ProtocolFlags.codec

    check {
      forAll(Gen.oneOf(false, true), Gen.choose(1,4)) { (direction, version) =>
        val msgDirection = direction match {
          case true => Response
          case false => Request
        }
        val byte = BitVector.bit(direction) ++ BitVector(version).drop(1)
        codec.decode(byte) match {
          case Attempt.Successful(DecodeResult(decoded, remainder)) =>
            s"$byte = $decoded" |: allProps(
              (s"$msgDirection = ${decoded.direction}"   |: msgDirection == decoded.direction) &&
              (s"$version == ${decoded.version.version}" |: version == decoded.version.version)
            )
          case Attempt.Failure(cause) =>
            s"$cause" |: false
        }
      }
    }
  }

  "HeaderFlags" must "properly decode from generated values." in {
    val codec = HeaderFlags.codec

    check {
      forAll { (warning: Boolean, customPayload: Boolean,
                tracing: Boolean, compression: Boolean) =>
        val byte = BitVector.bits(
          false :: false :: false :: false ::
          warning :: customPayload :: tracing :: compression :: Nil)

        val expected = HeaderFlags(warning, customPayload, tracing, compression)

        codec.decode(byte) match {
          case Attempt.Successful(DecodeResult(decoded, remainder)) =>
            s"$expected = $decoded" |: allProps(
              (s"$expected = $decoded"                 |: expected == decoded) &&
              (s"no remaining data(${remainder.size})" |: remainder.size == 0)
            )
          case Attempt.Failure(cause) =>
            s"$cause" |: false
        }
      }
    }
  }

  /*"Opcode" must "properly decode from documented values." in {
    // This test simply ensures that the enumeration index properly matches
    // the spec by testing with a known Opcode => Int mapping.
    val codec = Opcode.codec

    val event = 0xC.toByte

    codec.decode(ByteVector(event).toBitVector) match {
      case Attempt.Successful(DecodeResult(decoded, remainder)) =>
        decoded shouldEqual Opcode.Event
        remainder.size shouldEqual 0
      case Attempt.Failure(cause) =>
        fail(cause.messageWithContext)
    }
  }*/

  def validateData(data: ByteVector, version: Int, stream: Int) = {
    val codec = FrameHeader.codec
    codec.decode(data.toBitVector) match {
      case Attempt.Successful(DecodeResult(decoded, remainder)) =>
        s"protocol = $decoded" |: allProps(
          (s"version ${decoded.version} != $version" |: decoded.version.version.version == version) &&
          (s"stream ${decoded.stream} != $stream"    |: decoded.stream == stream) &&
          (s"no remaining data(${remainder.size})"   |: remainder.size == 0)
        )
      case Attempt.Failure(cause) =>
        s"$cause" |: false
    }
  }

  "FrameHeader" must "properly decode stream id for v1/v2." in {
    val headerV1or2Gen = for {
      version <- Gen.choose(1, 2)
      flags <- Gen.choose(0, 15)
      stream <- Gen.choose(-128, 127)
      opcode <- Gen.choose(0, 0x10)
      length <- Gen.choose(0, 256 * 0x100000) // 256 MB
    } yield (ByteVector(version, flags) ++
        int8.encode(stream).require.toByteVector ++
        ByteVector(opcode) ++
        uint32.encode(length).require.toByteVector, version, stream)

    check {
      forAll(headerV1or2Gen) {
        data => validateData(data._1, data._2, data._3)
      }
    }
  }

  it must "properly decode stream id for v3/v4." in {
    val headerV3PlusGen = for {
      version <- Gen.choose(3, 4)
      flags <- Gen.choose(0, 15)
      stream <- Gen.choose(-32768, 32767) // 4 byte stream id
      opcode <- Gen.choose(0, 0x10)
      length <- Gen.choose(0, 256 * 0x100000) // 256 MB
    } yield (ByteVector(version, flags) ++
        int16.encode(stream).require.toByteVector ++
        ByteVector(opcode) ++
        uint32.encode(length).require.toByteVector, version, stream)

    check {
      forAll(headerV3PlusGen) {
        data => validateData(data._1, data._2, data._3)
      }
    }
  }

  it must "properly choose the Message codec based on opcode." in {
    val codec = Frame.codec
    val frame = codec.decode((ByteVector(4, 0) ++
      int16.encode(1).require.toByteVector ++
      ByteVector(1) ++
      uint32.encode(12).require.toByteVector ++
      uint32.encode(15).require.toByteVector).toBitVector)
  }

  it must "behave someway when the message is not large enough." in {
    val codec = FrameHeader.codec
    val frame = codec.decode((ByteVector(4, 0) ++
      int16.encode(1).require.toByteVector ++
      ByteVector(1)).toBitVector)
  }
}
