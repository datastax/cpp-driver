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
package org.scassandra.server.priming.json

/**
 * Used to parse the JSON representing the result of the prime.
 */
abstract class ResultJsonRepresentation(val string: String)

case object Success extends ResultJsonRepresentation("success")

case object ReadTimeout extends ResultJsonRepresentation("read_request_timeout")

case object Unavailable extends ResultJsonRepresentation("unavailable")

case object WriteTimeout extends ResultJsonRepresentation("write_request_timeout")

case object ServerError extends ResultJsonRepresentation("server_error")

case object ProtocolError extends ResultJsonRepresentation("protocol_error")

case object BadCredentials extends ResultJsonRepresentation("bad_credentials")

case object Overloaded extends ResultJsonRepresentation("overloaded")

case object IsBootstrapping extends ResultJsonRepresentation("is_bootstrapping")

case object TruncateError extends ResultJsonRepresentation("truncate_error")

case object SyntaxError extends ResultJsonRepresentation("syntax_error")

case object Unauthorized extends ResultJsonRepresentation("unauthorized")

case object Invalid extends ResultJsonRepresentation("invalid")

case object ConfigError extends ResultJsonRepresentation("config_error")

case object AlreadyExists extends ResultJsonRepresentation("already_exists")

case object Unprepared extends ResultJsonRepresentation("unprepared")

case object ReadFailure extends ResultJsonRepresentation("read_failure")

case object FunctionFailure extends ResultJsonRepresentation("function_failure")

case object WriteFailure extends ResultJsonRepresentation("write_failure")

case object ClosedConnection extends ResultJsonRepresentation("closed_connection")

object ResultJsonRepresentation {
  def fromString(string: String): ResultJsonRepresentation = {
    string match {
      case ReadTimeout.string => ReadTimeout
      case Unavailable.string => Unavailable
      case WriteTimeout.string => WriteTimeout
      case ServerError.string => ServerError
      case ProtocolError.string => ProtocolError
      case BadCredentials.string => BadCredentials
      case Overloaded.string => Overloaded
      case IsBootstrapping.string => IsBootstrapping
      case TruncateError.string => TruncateError
      case SyntaxError.string => SyntaxError
      case Unauthorized.string => Unauthorized
      case Invalid.string => Invalid
      case ConfigError.string => ConfigError
      case AlreadyExists.string => AlreadyExists
      case Unprepared.string => Unprepared
      case ClosedConnection.string => ClosedConnection
      case ReadFailure.string => ReadFailure
      case WriteFailure.string => WriteFailure
      case FunctionFailure.string => FunctionFailure
      case Success.string => Success
      case _ => Success
    }
  }
}


