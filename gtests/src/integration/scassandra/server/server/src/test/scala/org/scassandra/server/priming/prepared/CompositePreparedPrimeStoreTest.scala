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
package org.scassandra.server.priming.prepared

import org.scalatest.{FunSuite, Matchers}
import org.scassandra.codec._
import org.scassandra.codec.messages.{PreparedMetadata, RowMetadata}
import org.scassandra.server.priming.query.{Prime, Reply}
import scodec.bits.ByteVector

class CompositePreparedPrimeStoreTest extends FunSuite with Matchers {
  implicit val protocolVersion = ProtocolVersion.latest

  val one: PreparedStoreLookup = new PreparedStoreLookup {
    def apply(prepare: Prepare, preparedFactory: (PreparedMetadata, RowMetadata) => Prepared) : Option[Prime] = None
    def apply(queryText: String, execute: Execute)(implicit protocolVersion: ProtocolVersion) : Option[Prime] = None
  }
  val two: PreparedStoreLookup = new PreparedStoreLookup {
    def apply(prepare: Prepare, preparedFactory: (PreparedMetadata, RowMetadata) => Prepared) : Option[Prime] = None
    def apply(queryText: String, execute: Execute)(implicit protocolVersion: ProtocolVersion) : Option[Prime] = None
  }

  val result = Some(Reply(NoRows))

  val three: PreparedStoreLookup = new PreparedStoreLookup {
    def apply(prepare: Prepare, preparedFactory: (PreparedMetadata, RowMetadata) => Prepared) : Option[Prime] = result
    def apply(queryText: String, execute: Execute)(implicit protocolVersion: ProtocolVersion) : Option[Prime] = result
  }

  test("Delegates to all stores - apply for prepared") {
    val underTest = new CompositePreparedPrimeStore(one, two, three)
    val factory = (preparedMetadata: PreparedMetadata, rowMetadata: RowMetadata) => Prepared(ByteVector(1))
    underTest.apply(Prepare("hello"), factory) shouldEqual result
  }

  test("Delegates to all stores - apply for execute") {
    val underTest = new CompositePreparedPrimeStore(one, two, three)
    underTest.apply("hello", Execute(ByteVector(1))) shouldEqual result
  }
}
