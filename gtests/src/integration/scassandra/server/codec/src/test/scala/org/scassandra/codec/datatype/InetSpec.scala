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

import java.net.InetAddress

import scodec.Attempt.Failure
import scodec.bits.ByteVector

class InetSpec extends DataTypeSpec {

  val codec = DataType.Inet.codec

  val ipv4LocalBytes = Array[Byte](127, 0, 0, 1)
  val ipv4Local = InetAddress.getByAddress(ipv4LocalBytes)
  val ipv6LocalBytes = Array[Byte](0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1)
  val ipv6Local = InetAddress.getByAddress(ipv6LocalBytes)


  "codec" must "encode Inet4Address as inet format" in {
    codec.encode(ipv4Local).require.bytes shouldEqual ByteVector(ipv4LocalBytes)
  }

  it must "encode Inet6Address as inet format" in {
    codec.encode(ipv6Local).require.bytes shouldEqual ByteVector(ipv6LocalBytes)
  }

  it must "encode String than can be parsed as a Inet as inet format" in {
    codec.encode("127.0.0.1").require.bytes shouldEqual ByteVector(ipv4LocalBytes)
    codec.encode("::1").require.bytes shouldEqual ByteVector(ipv6LocalBytes)
  }

  it must "fail to encode value that doesn't map to a double" in {
    codec.encode("hello") should matchPattern { case Failure(_) => }
    codec.encode(true) should matchPattern { case Failure(_) => }
    codec.encode(false) should matchPattern { case Failure(_) => }
    codec.encode(List()) should matchPattern { case Failure(_) => }
    codec.encode(Map()) should matchPattern { case Failure(_) => }
  }

  it must "encode and decode back as InetAddress" in {
    encodeAndDecode(codec, ipv4Local)
    encodeAndDecode(codec, ipv6Local)
    encodeAndDecode(codec, "127.0.0.1", ipv4Local)
    encodeAndDecode(codec, "::1", ipv6Local)
  }

}
