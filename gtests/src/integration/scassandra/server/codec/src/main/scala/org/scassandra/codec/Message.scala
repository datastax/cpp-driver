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

import org.scassandra.codec.Consistency.Consistency
import org.scassandra.codec.Notations.{int => cint, long => clong, short => cshort, string => cstring, _}
import org.scassandra.codec.messages.BatchType.BatchType
import org.scassandra.codec.messages._
import scodec.Attempt.{Failure, Successful}
import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs._

import scala.util.{Try, Failure => TFailure}

/**
  * Represents a protocol messages defined in section 4 of the native protocol spec.
  */
sealed trait Message {
  /**
    * The opcode used to identify the message defined in section 2.4 of the spec.
    */
  val opcode: Int

  /**
    * Convenience function for wrapping a [[Message]] into a frame and
    * @param stream stream id
    * @param direction direction of the message
    * @param protocolVersion protocol version to use to encode/decode.
    * @return encoded bytes of the message with the frame header.
    */
  def toBytes(stream: Int, direction: MessageDirection = Response)(implicit protocolVersion: ProtocolVersion): Try[ByteVector] = {
    Message.codec(opcode).encode(this).flatMap(bits => {
      val bytes = bits.bytes
      val header = FrameHeader(
        ProtocolFlags(direction, protocolVersion),
        EmptyHeaderFlags,
        stream,
        opcode,
        bits.bytes.size
      )
      Codec[FrameHeader].encode(header).map(_.bytes ++ bytes)
    }) match {
      case Successful(result) => Try(result)
      case Failure(x) => TFailure(new Exception(x.toString))
    }
  }
}

object Message {

  private[codec] def codecForVersion(implicit protocolVersion: ProtocolVersion, opcode: Int): Codec[Message] = opcode match {
    case ErrorMessage.opcode => Codec[ErrorMessage].upcast[Message]
    case Startup.opcode => Codec[Startup].upcast[Message]
    case Ready.opcode => Codec[Ready.type].upcast[Message]
    case Options.opcode => Codec[Options.type].upcast[Message]
    // TODO: Authenticate
    case Supported.opcode => Codec[Supported].upcast[Message]
    case Query.opcode => Codec[Query].upcast[Message]
    case Result.opcode => Codec[Result].upcast[Message]
    case Prepare.opcode => Codec[Prepare].upcast[Message]
    case Execute.opcode => Codec[Execute].upcast[Message]
    case Register.opcode => Codec[Register].upcast[Message]
    // TODO: Event
    case Batch.opcode => Codec[Batch].upcast[Message]
    // TODO: AuthChallenge
    // TODO: AuthResponse
    // TODO: AuthSuccess
    case _ => provide(ProtocolError(s"Unknown opcode: $opcode")).upcast[Message]
  }

  /**
    * Resolves the appropriate [[Message]] codec based on the opcode.
    * @param opcode Opcode of the message to resolve codec for.
    * @return The appropriate [[Codec]] based on the opcode.
    */
  def codec(opcode: Int)(implicit protocolVersion: ProtocolVersion) = protocolVersion.messageCodec(opcode)
}

/**
  * Indicates an error [[Message]] type as defined in section 4.2.1 of the spec.
  * @param message message describing the error, every error type has this.
  */
sealed abstract class ErrorMessage(message: String) extends Message {
  override val opcode = ErrorMessage.opcode
}

/**
  * Something unexpected happened.  This indicates a server-side bug.
  *
  * @param message message describing the error, every error type has this.
  */
case class ServerError(message: String) extends ErrorMessage(message)

/**
  * Some client message triggered a protocol violation.
  *
  * @param message message describing the error, every error type has this.
  */
case class ProtocolError(message: String) extends ErrorMessage(message)

/**
  * Authentication was required and failed.
  *
  * @param message message describing the error, every error type has this.
  */
case class AuthenticationError(message: String) extends ErrorMessage(message)

/**
  * Indicates that not enough replicas were available to complete the query.
  *
  * @param message message describing the error, every error type has this.
  * @param consistency the consistency level of the query that triggered the exception.
  * @param required the number of replicas that should be alive to respect the consistency level.
  * @param alive the number of replicas that were known to be alive when the request has been processed.
  */
case class Unavailable(message: String = "Unavailable Exception", consistency: Consistency, required: Int, alive: Int) extends ErrorMessage(message)

/**
  * The request cannot be processed because the coordinator node is overloaded.
  *
  * @param message message describing the error, every error type has this.
  */
case class Overloaded(message: String) extends ErrorMessage(message)

/**
  * The request was a read request but the coordinator node is bootstrapping.
  *
  * @param message message describing the error, every error type has this.
  */
case class IsBootstrapping(message: String) extends ErrorMessage(message)

/**
  * Error encountered during truncation.
  *
  * @param message message describing the error, every error type has this.
  */
case class TruncateError(message: String) extends ErrorMessage(message)

/**
  * Timeout exception during a write request.
  *
  * @param message message describing the error, every error type has this.
  * @param consistency the consistency level of the query that triggered the exception.
  * @param received represents the number of replicas having acknowledged the request.
  * @param blockFor the number of replicas whose acknowledgement is required to achieve the consistency level.
  * @param writeType describes the type of write that timed out.
  */
case class WriteTimeout(message: String = "Write Request Timeout", consistency: Consistency, received: Int, blockFor: Int, writeType: String) extends ErrorMessage(message)

/**
  * Timeout exception during a read request.
  * @param message message describing the error, every error type has this.
  * @param consistency the consistency level of the query that triggered the exception.
  * @param received represents the number of replicas having acknowledged the request.
  * @param blockFor the number of replicas whose acknowledgement is required to achieve the consistency level.
  * @param dataPresent whether or not the replica that was asked for data responded.
  */
case class ReadTimeout(message: String = "Read Request Timeout", consistency: Consistency, received: Int, blockFor: Int, dataPresent: Boolean) extends ErrorMessage(message)

/**
  * A non-timeout exception during a read request.
  *
  * @param message message describing the error, every error type has this.
  * @param consistency the consistency level of the query that triggered the exception.
  * @param received represents the number of replicas having acknowledged the request.
  * @param blockFor the number of replicas whose acknowledgement is required to achieve the consistency level.
  * @param numFailures The number of replicas that experienced a failure while executing the request.
  * @param dataPresent whether or not the replica that was asked for data responded.
  */
case class ReadFailure(message: String = "Read Failure", consistency: Consistency, received: Int, blockFor: Int, numFailures: Int, dataPresent: Boolean) extends ErrorMessage(message)

/**
  * A (user defined) function failed during execution.
  *
  * @param message message describing the error, every error type has this.
  * @param keyspace the keyspace of the failed function.
  * @param function the name of the failed function.
  * @param argTypes the argument types for the function.
  */
case class FunctionFailure(message: String = "Function Failure", keyspace: String, function: String, argTypes: List[String]) extends ErrorMessage(message)

/**
  * A non-timeout exception during a write request.
  * @param message message describing the error, every error type has this.
  * @param consistency the consistency level of the query that triggered the exception.
  * @param received represents the number of replicas having acknowledged the request.
  * @param blockFor the number of replicas whose acknowledgement is required to achieve the consistency level.
  * @param numFailures The number of replicas that experienced a failure while executing the request.
  * @param writeType describes the type of write that failed.
  */
case class WriteFailure(message: String = "Write Failure", consistency: Consistency, received: Int, blockFor: Int, numFailures: Int, writeType: String) extends ErrorMessage(message)

/**
  * The submitted query has a syntax error.
  *
  * @param message message describing the error, every error type has this.
  */
case class SyntaxError(message: String) extends ErrorMessage(message)

/**
  * The logged in user doesn't have the right to perform the query.
  *
  * @param message message describing the error, every error type has this.
  */
case class Unauthorized(message: String) extends ErrorMessage(message)

/**
  * The query is syntactically correct but invalid.
  *
  * @param message message describing the error, every error type has this.
  */
case class Invalid(message: String) extends ErrorMessage(message)

/**
  * The query is invalid because of some configuration issue.
  *
  * @param message message describing the error, every error type has this.
  */
case class ConfigError(message: String) extends ErrorMessage(message)

/**
  * The query attempted to create a keyspace or a table that was already existing.
  *
  * @param message message describing the error, every error type has this.
  * @param keyspace The keyspace that already exists, or the keyspace in which the table already exists is.
  * @param table The table that already exists, or empty string if the keyspace was what already existed.
  */
case class AlreadyExists(message: String, keyspace: String, table: String) extends ErrorMessage(message)

/**
  * Can be thrown while a prepared statement tries to be executed if the provided prepared statement ID is not known
  * by the host.
  *
  * @param message message describing the error, every error type has this.
  * @param id The id of the prepared statement.
  */
case class Unprepared(message: String, id: ByteVector) extends ErrorMessage(message)

object ErrorMessage {
  val opcode = 0x00

  /**
    * The [[Codec]] for describing the [[ErrorMessage]] opcode and the [[ErrorMessage]]
    * itself.
    */
  implicit val codec: Codec[ErrorMessage] = discriminated[ErrorMessage].by(cint)
    .typecase(0x0000, cstring.as[ServerError])
    .typecase(0x000A, cstring.as[ProtocolError])
    .typecase(0x0100, cstring.as[AuthenticationError])
    .typecase(0x1000, (cstring :: Consistency.codec :: cint :: cint).as[Unavailable])
    .typecase(0x1001, cstring.as[Overloaded])
    .typecase(0x1002, cstring.as[IsBootstrapping])
    .typecase(0x1003, cstring.as[TruncateError])
    .typecase(0x1100, (cstring :: Consistency.codec :: cint :: cint :: cstring).as[WriteTimeout])
    .typecase(0x1200, (cstring :: Consistency.codec :: cint :: cint :: bool(8)).as[ReadTimeout])
    .typecase(0x1300, (cstring :: Consistency.codec :: cint :: cint :: cint :: bool(8)).as[ReadFailure])
    .typecase(0x1400, (cstring :: cstring :: cstring :: stringList).as[FunctionFailure])
    .typecase(0x1500, (cstring :: Consistency.codec :: cint :: cint :: cint :: cstring).as[WriteFailure])
    .typecase(0x2000, cstring.as[SyntaxError])
    .typecase(0x2100, cstring.as[Unauthorized])
    .typecase(0x2200, cstring.as[Invalid])
    .typecase(0x2300, cstring.as[ConfigError])
    .typecase(0x2400, (cstring :: cstring :: cstring).as[AlreadyExists])
    .typecase(0x2500, (cstring :: shortBytes).as[Unprepared])
}

/**
  * Message sent to initialize the connection.  See section 4.1.1 of the spec.
  *
  * @param options indicates startup options (CQL_VERSION, COMPRESSION)
  */
case class Startup(options: Map[String, String] = Map()) extends Message {
  override val opcode = Startup.opcode
}

object Startup {
  val opcode = 0x01

  /**
    * Codec for [[Startup]], is simply a [string map].
    */
  implicit val codec: Codec[Startup] = stringMap.as[Startup]
}

/**
  * Message sent from server to indicate server is ready to process queries.  Sent
  * as a response to [[Startup]].  See section 4.2.2 of the spec.
  */
case object Ready extends Message {
  override val opcode = 0x2
  implicit val codec: Codec[Ready.type] = provide(Ready)
}

/**
  * Message sent from client which STARTUP options are supported.  See section 4.1.3 of
  * the spec.
  */
case object Options extends Message {
  override val opcode = 0x5
  implicit val codec: Codec[Options.type] = provide(Options)
}

/**
  * Message sent from server to indicate the startup options that are supported by the
  * server.  Sent as a repsonse to [[Options]].  See section 4.2.4 of the spec.
  *
  * @param options multimap of options supported by the server.
  */
case class Supported(options: Map[String, List[String]] = Map()) extends Message {
  override val opcode = Supported.opcode
}

object Supported {
  val opcode = 0x6

  /**
    * Codec for [[Supported]], is simply a [string multimap].
    */
  implicit val codec: Codec[Supported] = stringMultimap.as[Supported]
}

/**
  * Message sent from client to perform a CQL query.  See section 4.1.4 of the spec.
  * @param query the cql query.
  * @param parameters the query parameters.
  */
case class Query(query: String, parameters: QueryParameters = DefaultQueryParameters) extends Message {
  override val opcode = Query.opcode
}

object Query {
  val opcode = 0x7

  /**
    * @param protocolVersion protocol version to use to encode/decode.
    * @return Codec for [[Query]] that contains the query string and it's parameters.
    */
  implicit def codec(implicit protocolVersion: ProtocolVersion): Codec[Query] = protocolVersion.queryCodec

  private[codec] def codecForVersion(implicit protocolVersion: ProtocolVersion) = {
    ("query"      | longString) ::
    ("parameters" | Codec[QueryParameters])
  }.as[Query]
}

/**
  * The result to a query.  See section 4.2.5 of the spec.
  */
sealed trait Result extends Message {
  override val opcode: Int = Result.opcode
}

object Result {
  val opcode: Int = 0x8

  /**
    * @param protocolVersion protocol version to use to encode/decode.
    * @return Codec for [[Result]] which encompasses the result type opcode and the actual result.
    */
  implicit def codec(implicit protocolVersion: ProtocolVersion): Codec[Result] = protocolVersion.resultCodec

  private[codec] def codecForVersion(implicit protocolVersion: ProtocolVersion) = discriminated[Result].by(cint)
    .typecase(0x1, Codec[VoidResult.type])
    .typecase(0x2, Codec[Rows])
    .typecase(0x3, Codec[SetKeyspace])
    .typecase(0x4, Codec[Prepared])
}

/**
  * A result carrying no information.  See section 4.2.5.1 of the spec.
  */
case object VoidResult extends Result {
  implicit val codec: Codec[VoidResult.type] = provide(VoidResult)
}

/**
  * A result from a select query, returning a set of rows.  See section 4.2.5.2 of the spec.
  * @param metadata Metadata for the rows.
  * @param rows The row data.
  */
case class Rows(metadata: RowMetadata = NoRowMetadata, rows: List[Row] = Nil) extends Result
object NoRows extends Rows()

object Rows {

  /**
    * @param protocolVersion protocol version to use to encode/decode.
    * @return Codec for [[Row]] which parses the metadata and underlying rows.
    */
  implicit def codec(implicit protocolVersion: ProtocolVersion): Codec[Rows] =
    protocolVersion.rowsCodec

  private[codec] def codecForVersion(implicit protocolVersion: ProtocolVersion) = {
    // read RowMetadata, and use flat prepend to use that metadata to
    // pass the column specification into a Row codec parser.
    Codec[RowMetadata].flatPrepend{ metadata =>
      listOfN(cint, protocolVersion.rowCodec(metadata.columnSpec.getOrElse(Nil))).hlist
    }
  }.as[Rows]
}

/**
  * A result to a 'use' query.  See section 4.2.5.3 of the spec.
  * @param keyspace Keyspace that has been set on the connection.
  */
case class SetKeyspace(keyspace: String) extends Result

case object SetKeyspace {
  /**
    * Codec for [[SetKeyspace]].  Simply a [string] for the keyspace.
    */
  implicit val codec: Codec[SetKeyspace] = cstring.as[SetKeyspace]
}

/**
  * The result to a [[Prepare]] message.  See section 4.2.5.4 of the spec.
  * @param id The id of the prepared statement.
  * @param preparedMetadata The metadata for the prepared statement.
  * @param resultMetadata The metadata for a result of a prepared statement.
  */
case class Prepared(id: ByteVector, preparedMetadata: PreparedMetadata = NoPreparedMetadata,
                    resultMetadata: RowMetadata = NoRowMetadata) extends Result

case object Prepared {
  /**
    * @param protocolVersion protocol version to use to encode/decode.
    * @return Codec for [[Prepared]]
    */
  implicit def codec(implicit protocolVersion: ProtocolVersion): Codec[Prepared] =
    protocolVersion.preparedCodec

  private[codec] def codecForVersion(implicit protocolVersion: ProtocolVersion) = {
    ("id"               | shortBytes)               ::
    ("preparedMetadata" | Codec[PreparedMetadata])  ::
    ("resultMetadata"   | Codec[RowMetadata])
  }.as[Prepared]
}

//case class SchemaChange extends Result

/**
  * Sent from client to prepare a query for later execution.  See section 4.1.5 of the protocol spec.
  * @param query the CQL query
  */
case class Prepare(query: String) extends Message{
  override val opcode = Prepare.opcode
}

object Prepare {
  val opcode = 0x9
  /**
    * Codec for [[Prepare]] which is simply a [long string].
    */
  implicit val codec: Codec[Prepare] = longString.as[Prepare]
}

/**
  * Sent from client to execute a prepared query.  See section 4.1.6 of the spec.
  * @param id prepared statement id.
  * @param parameters the parameters to use for the query.
  */
case class Execute(id: ByteVector, parameters: QueryParameters = DefaultQueryParameters) extends Message {
  val opcode = Execute.opcode
}

object Execute {
  val opcode = 0xA

  /**
    * @param protocolVersion protocol version to use to encode/decode.
    * @return Codec for [[Execute]].
    */
  implicit def codec(implicit protocolVersion: ProtocolVersion): Codec[Execute] =
    protocolVersion.executeCodec

  private[codec] def codecForVersion(implicit protocolVersion: ProtocolVersion) = {
    ("id"              | shortBytes) ::
    ("queryParameters" | Codec[QueryParameters])
  }.as[Execute]
}

/**
  * Sent by client to register this connection to receive some types of events.  See section 4.1.8 of the spec.
  * @param events events to subscribe on.
  */
case class Register(events: List[String] = Nil) extends Message {
  override val opcode: Int = Register.opcode
}

object Register {
  val opcode = 0xB

  /**
    * Codec for [[Register]] simply a [string list] of events.
    */
  implicit val codec: Codec[Register] = stringList.as[Register]
}

/**
  * Sent by client to execute a list of queries.  See 4.1.7 of the spec.
  * @param batchType The type of batch (LOGGED, UNLOGGED, COUNTER).
  * @param queries The queries to make.
  * @param consistency The consistency to use for the queries.
  * @param serialConsistency The serial consistency to use for the queries.
  * @param timestamp The timestamp to be used for mutations.
  */
case class Batch(
  batchType: BatchType,
  queries: List[BatchQuery],
  consistency: Consistency = Consistency.ONE,
  serialConsistency: Option[Consistency] = None,
  timestamp: Option[Long] = None
) extends Message {
  override val opcode = Batch.opcode
}

object Batch {
  val opcode = 0xD

  /**
    * @param protocolVersion protocol version to use to encode/decode.
    * @return Codec for [[Batch]].
    */
  implicit def codec(implicit protocolVersion: ProtocolVersion): Codec[Batch] = protocolVersion.batchCodec

  private[codec] def codecForVersion(implicit protocolVersion: ProtocolVersion) = {
    ("batchType"         | Codec[BatchType]) ::
    ("queries"           | listOfN(cshort, Codec[BatchQuery])) ::
    ("consistency"       | Consistency.codec) ::
    // only parse flags for protocol version 2+ since that's when they were added.
    ("flags"             | withDefaultValue(conditional(protocolVersion.version > 2, Codec[BatchFlags]), DefaultBatchFlags)).consume { flags =>
    ("serialConsistency" | conditional(flags.withSerialConsistency, Consistency.codec)) ::
    ("timestamp"         | conditional(flags.withDefaultTimestamp, clong))
    } { data => // derive flags from presence of serialConsistency and timestamp.
      val serialConsistency = data.head
      val timestamp = data.tail.head
      BatchFlags(
        withDefaultTimestamp = timestamp.isDefined,
        withSerialConsistency = serialConsistency.isDefined
      )
    }
  }.as[Batch]
}
