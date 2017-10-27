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

import org.scalatest.{Matchers, WordSpec}
import scodec.Codec

trait CodecSpec extends WordSpec with Matchers with CodecTablePropertyChecks {

  /**
    * Convenience wrapper that runs enclosing tests with all protocol versions.
    *
    * @param f      enclosing function to wrap.  Provides protocol version under test.
    * @param filter filter to apply to specify explicit protocol versions to test.
    */
  def withProtocolVersions(f: ProtocolVersion => Unit, filter: ProtocolVersion => Boolean): Unit = {
    forAll(versions.filter(filter)) { (protocolVersion: ProtocolVersion) =>
      "using " + protocolVersion should {
        f(protocolVersion)
      }
    }
  }

  /**
    * Convenience wrapper that runs enclosing tests with all protocol versions.
    *
    * @param f      enclosing function to wrap.  Provides protocol version under test.
    */
  def withProtocolVersions(f: ProtocolVersion => Unit): Unit = withProtocolVersions(f, _ => true)

  def encodeAndDecode[T](data: T)(implicit codec: Codec[T]): Unit = {
    encodeAndDecode(codec, data)
  }

  def encodeAndDecode[T](data: T, expected: Any)(implicit codec: Codec[T]): Unit = {
    encodeAndDecode(codec, data, expected)
  }

  def encodeAndDecode[T](codec: Codec[T], data: T): Unit = {
    encodeAndDecode(codec, data, data)
  }

  def encodeAndDecode[T](codec: Codec[T], data: T, expected: Any): Unit = {
    val encoded = codec.encode(data).require
    val decoded = codec.decodeValue(encoded).require

    val expectedResult = expected
    decoded shouldEqual expectedResult
  }
}

