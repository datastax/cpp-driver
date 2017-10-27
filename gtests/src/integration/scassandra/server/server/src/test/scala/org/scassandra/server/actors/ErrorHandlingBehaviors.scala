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
package org.scassandra.server.actors

import org.scalatest.FunSuite
import org.scassandra.codec.Consistency.Consistency
import org.scassandra.codec._
import org.scassandra.server.priming._
import scodec.bits.ByteVector

trait ErrorHandlingBehaviors {
  this: FunSuite =>

  def executeWithError(result: ErrorResult, expectedError: (Consistency) => ErrorMessage): Unit

  test("Execute with read time out") {
    executeWithError(ReadRequestTimeoutResult(), (c) => ReadTimeout(consistency = c, received = 0, blockFor = 3, dataPresent = false))
  }

  test("Execute with write time out") {
    executeWithError(WriteRequestTimeoutResult(), (c) => WriteTimeout(consistency = c, received = 0, blockFor = 2, writeType = "SIMPLE"))
  }

  test("Execute with unavailable") {
    executeWithError(UnavailableResult(), (c) => Unavailable(consistency = c, required = 3, alive = 2))
  }

  test("Execute with server error") {
    val message = "Server Error"
    executeWithError(ServerErrorResult(message), (c) => ServerError(message))
  }

  test("Execute with protocol error") {
    val message = "Protocol Error"
    executeWithError(ProtocolErrorResult(message), (c) => ProtocolError(message))
  }

  test("Execute with bad credentials") {
    val message = "Bad Credentials"
    executeWithError(BadCredentialsResult(message), (c) => AuthenticationError(message))
  }

  test("Execute with overloaded") {
    val message = "Overloaded"
    executeWithError(OverloadedResult(message), (c) => Overloaded(message))
  }

  test("Execute with is bootstrapping") {
    val message = "I'm bootstrapping"
    executeWithError(IsBootstrappingResult(message), (c) => IsBootstrapping(message))
  }

  test("Execute with truncate error") {
    val message = "Truncate Error"
    executeWithError(TruncateErrorResult(message), (c) => TruncateError(message))
  }

  test("Execute with syntax error") {
    val message = "Syntax Error"
    executeWithError(SyntaxErrorResult(message), (c) => SyntaxError(message))
  }

  test("Execute with invalid") {
    val message = "Invalid"
    executeWithError(InvalidResult(message), (c) => Invalid(message))
  }

  test("Execute with config error") {
    val message = "Config Error"
    executeWithError(ConfigErrorResult(message), (c) => ConfigError(message))
  }

  test("Execute with already exists") {
    val message = "Already Exists"
    val keyspace = "keyspace"
    val table = "table"
    executeWithError(AlreadyExistsResult(message, keyspace, table),  (c) => AlreadyExists(message, keyspace, table))
  }

  test("Execute with unprepared") {
    val message = "Unprepared"
    val id: Array[Byte] = Array[Byte](0x00, 0x12, 0x13, 0x14)
    executeWithError(UnpreparedResult(message, id), (c) => Unprepared(message, ByteVector(id)))
  }
}

