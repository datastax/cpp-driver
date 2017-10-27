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
package org.scassandra.server.priming

import akka.actor.ActorRef
import akka.io.Tcp
import org.scassandra.codec.Consistency.Consistency

object WriteType extends Enumeration {
  val SIMPLE, BATCH, UNLOGGED_BATCH, COUNTER, BATCH_LOG, CAS = Value
}

/**
 * Represents the result of the prime in the system. See ResultJsonRepresentation for
 * serialisation of prime JSON.
 */
sealed abstract class PrimeResult
sealed abstract class ErrorResult extends PrimeResult
sealed abstract class FatalResult extends PrimeResult {
  def produceFatalError(tcpConnection: ActorRef)
}

case object SuccessResult extends PrimeResult

case class ServerErrorResult(message: String) extends ErrorResult
case class ProtocolErrorResult(message: String) extends ErrorResult
case class BadCredentialsResult(message: String) extends ErrorResult
case class OverloadedResult(message: String) extends ErrorResult
case class IsBootstrappingResult(message: String) extends ErrorResult
case class TruncateErrorResult(message: String) extends ErrorResult
case class SyntaxErrorResult(message: String) extends ErrorResult
case class UnauthorizedResult(message: String) extends ErrorResult
case class InvalidResult(message: String) extends ErrorResult
case class ConfigErrorResult(message: String) extends ErrorResult
case class AlreadyExistsResult(message: String, keyspace: String, table: String) extends ErrorResult
case class UnpreparedResult(message: String, id: Array[Byte]) extends ErrorResult

case class ReadRequestTimeoutResult(receivedResponses: Int = 0, requiredResponses: Int = 1, dataPresent: Boolean = false, consistencyLevel: Option[Consistency] = None) extends ErrorResult
case class WriteRequestTimeoutResult(receivedResponses: Int = 0, requiredResponses: Int = 1, writeType: WriteType.Value = WriteType.SIMPLE, consistencyLevel: Option[Consistency] = None) extends ErrorResult
case class UnavailableResult(requiredResponses: Int = 1, alive: Int = 0, consistencyLevel: Option[Consistency] = None) extends ErrorResult

case class ClosedConnectionResult(command: String) extends FatalResult {

  private lazy val closeCommand: Tcp.CloseCommand = command match {
    case "reset"      => Tcp.Abort
    case "halfclose"  => Tcp.ConfirmedClose
    case "close" | _  => Tcp.Close
  }

  override def produceFatalError(tcpConnection: ActorRef) = tcpConnection ! closeCommand
}