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

import java.net.{InetAddress, InetSocketAddress}

import org.scalacheck.Prop.{BooleanOperators, forAll}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}
import scodec.Attempt.{Failure, Successful}
import scodec.DecodeResult
import scodec.bits.ByteVector
import scodec.codecs._

class NotationsSpec extends FlatSpec with Checkers with Matchers {

  implicit val arbitraryByteVector: Arbitrary[ByteVector] = Arbitrary {
    for {
      size <- Gen.choose[Int](0, 10000) // keep data set relatively small
      bytes <- Gen.containerOfN[Array, Byte](size, Arbitrary.arbitrary[Byte])
    } yield ByteVector(bytes)
  }

  "value" must "encode Unset into -2" in {
    Notations.value.encode(Unset) shouldEqual int32.encode(-2)
  }

  it must "decode -2 into Unset" in {
    Notations.value.decode(int32.encode(-2).require).require.value shouldEqual Unset
  }

  it must "encode Null into -1" in {
    Notations.value.encode(Null) shouldEqual int32.encode(-1)
  }

  it must "decode -1 into Null" in {
    Notations.value.decode(int32.encode(-1).require).require.value shouldEqual Null
  }

  it must "properly encode and decode Bytes" in {
    check {
      forAll { (bytes: ByteVector) =>
        val data = Bytes(bytes)
        Notations.value.encode(data) match {
          case Successful(bits) => Notations.value.decode(bits) match {
            case Successful(DecodeResult(result, remainder)) => {
              s"$result != $data" |: result == data
            }
            case f: Failure => s"$f" |: false
          }
          case f: Failure => s"$f" |: false
        }
      }
    }
  }

  it must "fail to decode for lengths < -2" in {
    check {
      forAll(Gen.choose(Int.MinValue, -3)) { (length: Int) =>
        val result = int32.encode(length).flatMap(l => Notations.value.decode(l))
        s"$result should be Failure" |: result.isFailure
      }
    }
  }

  implicit val arbitraryInet: Arbitrary[InetSocketAddress] = Arbitrary {
    for {
      size <- Gen.oneOf[Int](4, 16) // Ipv4 or Ipv6
      bytes <- Gen.containerOfN[Array, Byte](size, Arbitrary.arbitrary[Byte])
      port <- Gen.choose[Int](0, 65535)
    } yield new InetSocketAddress(InetAddress.getByAddress(bytes), port)
  }

  "inet" must "property encode and decode InetSocketAddress" in {
    check {
      forAll { (address: InetSocketAddress) =>
        val encoded = Notations.inet.encode(address).require
        val decoded = Notations.inet.decode(encoded).require.value
        s"$decoded != $address" |: decoded == address
      }
    }
  }
}
