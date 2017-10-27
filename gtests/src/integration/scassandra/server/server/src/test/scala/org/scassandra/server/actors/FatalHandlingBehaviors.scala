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

import akka.io.Tcp
import org.scalatest.FunSuite
import org.scassandra.server.priming.{ClosedConnectionResult, FatalResult}

trait FatalHandlingBehaviors {
  this: FunSuite =>

  def executeWithFatal(result: FatalResult, expectedCommand: Tcp.CloseCommand)

  test("Execute with ClosedCommand - close") {
    val result = ClosedConnectionResult("close")
    executeWithFatal(result, Tcp.Close)
  }

  test("Execute with ClosedCommand - reset") {
    val result = ClosedConnectionResult("reset")
    executeWithFatal(result, Tcp.Abort)
  }

  test("Execute with ClosedCommand - halfclose") {
    val result = ClosedConnectionResult("halfclose")
    executeWithFatal(result, Tcp.ConfirmedClose)
  }
}
