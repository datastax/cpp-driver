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
package org.scassandra.server.priming.routes

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.LazyLogging
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.messages.{ColumnSpecWithoutTable, Row, RowMetadata}
import org.scassandra.codec.{Consistency, Rows}
import org.scassandra.server.priming._
import org.scassandra.server.priming.json._
import org.scassandra.server.priming.query._
import org.scassandra.{codec => c}
import scodec.bits.ByteVector

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Try, Success => TSuccess}

object PrimingJsonHelper extends LazyLogging {

  def extractPrimeCriteria(primeQueryRequest: PrimeQuerySingle): Try[PrimeCriteria] = {
    val primeConsistencies = primeQueryRequest.when.consistency.getOrElse(Consistency.all)

    primeQueryRequest.when match {
      // Prime for a specific query
      case When(Some(query), None, _, _, _) =>
        TSuccess(PrimeCriteria(query, primeConsistencies, patternMatch = false))
      // Prime for a query pattern
      case When(None, Some(queryPattern), _, _, _) => TSuccess(PrimeCriteria(queryPattern, primeConsistencies, patternMatch = true))
      case _ => Failure(new IllegalArgumentException("Can't specify query and queryPattern"))
    }
  }

  def extractPrime(thenDo: ThenProvider, keyspace: Option[String] = Some(""), table: Option[String] = Some("")) = {
    val fixedDelay = thenDo.fixedDelay.map(FiniteDuration(_, TimeUnit.MILLISECONDS))
    val config = thenDo.config.getOrElse(Map.empty)

    // For errors expecting a consistency level, only provide one if one is present in the prime, otherwise
    // set it explicitly to null.  This is a bit fragile, but it allows us to detect whether or not we are allowed
    // to override the error message with the statement used in the query.  Could have used [Option] but in this
    // case we want to make it required on the Error messages.
    thenDo.result.getOrElse(Success) match {
      // Special case ClosedConnectionReport as it produces a fatal error.
      case ClosedConnection => ClosedConnectionReport(config.getOrElse(ErrorConstants.CloseType, "close"), fixedDelay)
      case report =>
        val message = report match {
          case Success => extractRows(thenDo, keyspace, table)
          case ServerError => c.ServerError(config.getOrElse(ErrorConstants.Message, "Server Error"))
          case ProtocolError => c.ProtocolError(config.getOrElse(ErrorConstants.Message, "Protocol Error"))
          case BadCredentials => c.AuthenticationError(config.getOrElse(ErrorConstants.Message, "Bad Credentials"))
          case Unavailable => c.Unavailable(
            config.getOrElse(ErrorConstants.Message, "Unavailable Exception"),
            config.get(ErrorConstants.ConsistencyLevel).map(Consistency.withName).orNull,
            config.getOrElse(ErrorConstants.RequiredResponse, "1").toInt,
            config.getOrElse(ErrorConstants.Alive, "0").toInt
          )
          case Overloaded => c.Overloaded(config.getOrElse(ErrorConstants.Message, "Overloaded"))
          case IsBootstrapping => c.IsBootstrapping(config.getOrElse(ErrorConstants.Message, "Bootstrapping"))
          case TruncateError => c.TruncateError(config.getOrElse(ErrorConstants.Message, "Truncate Error"))
          case WriteTimeout => c.WriteTimeout(
            config.getOrElse(ErrorConstants.Message, "Write Request Timeout"),
            config.get(ErrorConstants.ConsistencyLevel).map(Consistency.withName).orNull,
            config.getOrElse(ErrorConstants.ReceivedResponse, "0").toInt,
            config.getOrElse(ErrorConstants.RequiredResponse, "1").toInt,
            config.getOrElse(ErrorConstants.WriteType, "SIMPLE")
          )
          case ReadTimeout => c.ReadTimeout(
            config.getOrElse(ErrorConstants.Message, "Read Request Timeout"),
            config.get(ErrorConstants.ConsistencyLevel).map(Consistency.withName).orNull,
            config.getOrElse(ErrorConstants.ReceivedResponse, "0").toInt,
            config.getOrElse(ErrorConstants.RequiredResponse, "1").toInt,
            config.getOrElse(ErrorConstants.DataPresent, "false").toBoolean
          )
          case ReadFailure => c.ReadFailure(
            config.getOrElse(ErrorConstants.Message, "Read Failure"),
            config.get(ErrorConstants.ConsistencyLevel).map(Consistency.withName).orNull,
            config.getOrElse(ErrorConstants.ReceivedResponse, "0").toInt,
            config.getOrElse(ErrorConstants.RequiredResponse, "1").toInt,
            config.getOrElse(ErrorConstants.NumberOfFailures, "1").toInt,
            config.getOrElse(ErrorConstants.DataPresent, "false").toBoolean
          )
          case WriteFailure => c.WriteFailure(
            config.getOrElse(ErrorConstants.Message, "Write Failure"),
            config.get(ErrorConstants.ConsistencyLevel).map(Consistency.withName).orNull,
            config.getOrElse(ErrorConstants.ReceivedResponse, "0").toInt,
            config.getOrElse(ErrorConstants.RequiredResponse, "1").toInt,
            config.getOrElse(ErrorConstants.NumberOfFailures, "1").toInt,
            config.getOrElse(ErrorConstants.WriteType, "SIMPLE")
          )
          case FunctionFailure => c.FunctionFailure(
            config.getOrElse(ErrorConstants.Message, "Function Failure"),
            config.getOrElse(ErrorConstants.Keyspace, "keyspace"),
            config.getOrElse(ErrorConstants.Function, "function"),
            config.getOrElse(ErrorConstants.Argtypes, "argtypes").split(",").toList
          )
          case SyntaxError => c.SyntaxError(config.getOrElse(ErrorConstants.Message, "Syntax Error"))
          case Unauthorized => c.Unauthorized(config.getOrElse(ErrorConstants.Message, "Unauthorized"))
          case Invalid => c.Invalid(config.getOrElse(ErrorConstants.Message, "Invalid"))
          case ConfigError => c.ConfigError(config.getOrElse(ErrorConstants.Message, "Config Error"))
          case AlreadyExists => c.AlreadyExists(
            config.getOrElse(ErrorConstants.Message, "Already Exists"),
            config.getOrElse(ErrorConstants.Keyspace, "keyspace"),
            config.getOrElse(ErrorConstants.Table, "")
          )
          case Unprepared => c.Unprepared(
            config.getOrElse(ErrorConstants.Message, "Unprepared"),
            ByteVector.fromValidHex(config.getOrElse(ErrorConstants.PrepareId, "0x").toLowerCase)
          )
        }

        // In the case of a Then implementation, provide the variable types.
        thenDo match {
          case t: Then =>
            Reply(message, fixedDelay, variableTypes = t.variable_types)
          case _ =>
            Reply(message, fixedDelay)
        }
    }
  }

  private def extractRows(thenDo: ThenProvider, keyspace: Option[String] = Some(""), table: Option[String] = Some("")): Rows = {
    // Convert Map[String,Any] -> Row
    val cRows: List[Row] = thenDo.rows.getOrElse(Nil).map(columns => Row(columns))

    // column types from prime.
    val colTypes = thenDo.column_types.getOrElse(Map())
    // extract all column names not in column types
    val nonPresentColumns: Map[String, DataType] = cRows
      .flatMap(row => row.columns)
      .map(_._1) // extract column names from columnName/dataType pairing.
      .filter(name => !colTypes.contains(name)) // remove all elements not in columnTypes.
      .toSet // remove duplicates.
      .map((name: String) => (name, DataType.Varchar)) // create the default Varchar mapping.
      .toMap

    // combine the column types from the prime and the default column types and map to a ColumnSpec.
    val columnSpec = (colTypes ++ nonPresentColumns).map {
      case ((name, dataType)) => ColumnSpecWithoutTable(name, dataType)
    }.toList

    val metadata = RowMetadata(
      keyspace = keyspace.orElse(Some("")),
      table = table.orElse(Some("")),
      columnSpec = Some(columnSpec)
    )

    Rows(metadata, cRows)
  }
}
