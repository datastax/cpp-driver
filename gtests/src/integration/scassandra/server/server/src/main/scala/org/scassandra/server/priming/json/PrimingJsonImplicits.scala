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

import java.math.BigInteger
import java.net.InetAddress
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.scassandra.codec.Consistency
import org.scassandra.codec.Consistency.Consistency
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.messages.BatchQueryKind.BatchQueryKind
import org.scassandra.codec.messages.BatchType.BatchType
import org.scassandra.codec.messages.{BatchQueryKind, BatchType}
import org.scassandra.cql._
import org.scassandra.server.actors._
import org.scassandra.server.priming._
import org.scassandra.server.priming.batch.{BatchPrimeSingle, BatchQueryPrime, BatchWhen}
import org.scassandra.server.priming.prepared._
import org.scassandra.server.priming.query.{PrimeCriteria, PrimeQuerySingle, Then, When}
import org.scassandra.server.priming.routes.Version
import scodec.bits.ByteVector
import spray.httpx.SprayJsonSupport
import spray.json._

import scala.collection.Set
import scala.util.{Failure, Try, Success => TSuccess}

object PrimingJsonImplicits extends DefaultJsonProtocol with SprayJsonSupport with LazyLogging {

  implicit object VariableMatchFormat extends JsonFormat[VariableMatch] {
    override def write(obj: VariableMatch): JsValue = {
      obj match {
        case ExactMatch(value) => {
          JsObject(Map(
            "type" -> JsString("exact"),
            "matcher" -> AnyJsonFormat.write(value)
          ))
        }
        case AnyMatch => JsObject(Map("type" -> JsString("any")))
      }
    }

    override def read(json: JsValue): VariableMatch = {
      json match {
        case obj: JsObject => {
          obj.fields("type") match {
            case JsString("exact") => ExactMatch(Some(AnyJsonFormat.read(obj.fields("matcher"))))
            case JsString("any") => AnyMatch
            case _ => AnyMatch
          }
        }
        case _ => AnyMatch
      }
    }
  }

  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any) = x match {
      case n: Int => JsNumber(n)
      case n: Long => JsNumber(n)
      case short: Short => JsNumber(short)
      case byte: Byte => JsNumber(byte)
      case bd: BigDecimal => JsString(bd.bigDecimal.toPlainString)
      case s: String => JsString(s)
      case seq: Seq[_] => seqFormat[Any].write(seq)
      case m: Map[_, _] =>
        val keysAsString: Map[String, Any] = m.map({ case (k, v) => (k.toString, v)})
        mapFormat[String, Any].write(keysAsString)
      case set: Set[_] => setFormat[Any].write(set.map(s => s))
      case b: Boolean if b => JsTrue
      case b: Boolean if !b => JsFalse

      // sending as strings to not lose precision
      case double: Double => JsString(double.toString)
      case float: Float => JsString(float.toString)
      case uuid: UUID => JsString(uuid.toString)
      case bigInt: BigInt => JsNumber(bigInt)
      case bigInt: BigInteger => JsNumber(bigInt)
      case bigD: java.math.BigDecimal => JsString(bigD.toPlainString)
      case inet: InetAddress => JsString(inet.getHostAddress)
      case bytes: Array[Byte] => JsString("0x" + bytes2hex(bytes))
      case bytes: ByteVector => JsString("0x" + bytes2hex(bytes.toArray))
      case null => JsNull
      case None => JsNull
      case Some(s) => this.write(s)
      case p: Product => seqFormat[Any].write(p.productIterator.toList) // To support tuples
      case other => serializationError("Do not understand object of type " + other.getClass.getName)
    }

    def read(value: JsValue) = value match {
      case jsNumber : JsNumber => jsNumber.value
      case JsString(s) => s
      case a: JsArray => listFormat[Any].read(value)
      case o: JsObject => mapFormat[String, Any].read(value)
      case JsTrue => true
      case JsFalse => false
      case x => deserializationError("Do not understand how to deserialize " + x)
    }

    def bytes2hex(bytes: Array[Byte]): String = {
      bytes.map("%02x".format(_)).mkString
    }
  }

  implicit object ConsistencyJsonFormat extends RootJsonFormat[Consistency] {
    def write(c: Consistency) = JsString(c.toString)

    def read(value: JsValue) = value match {
      case JsString(consistency) => Consistency.withName(consistency)
      case _ => throw new IllegalArgumentException("Expected Consistency as JsString")
    }
  }

  implicit object BatchQueryKindJsonFormat extends RootJsonFormat[BatchQueryKind] {
    def write(c: BatchQueryKind) = JsString(c match {
      case BatchQueryKind.Simple => "query"
      case BatchQueryKind.Prepared => "prepared_statement"
    })

    def read(value: JsValue) = value match {
      case JsString(v) => v match {
        case "query" => BatchQueryKind.Simple
        case "prepared_statement" => BatchQueryKind.Prepared
      }
      case _ => throw new IllegalArgumentException("Expected BatchQueryKind as JsString")
    }
  }

  implicit object BatchTypeJsonFormat extends RootJsonFormat[BatchType] {
    def write(c: BatchType) = JsString(c.toString)

    def read(value: JsValue) = value match {
      case JsString(batchType) => BatchType.withName(batchType)
      case _ => throw new IllegalArgumentException("Expected BatchType as JsString")
    }
  }


  implicit object DataTypeJsonFormat extends RootJsonFormat[DataType] {

    lazy val cqlTypeFactory = new CqlTypeFactory

    def convertJavaToScalaType(javaType: CqlType): DataType = javaType match {
        // TODO: Update for UDTs when supported.
        case primitive: PrimitiveType => DataType.primitiveTypeMap(primitive.serialise())
        case map: MapType => DataType.Map(convertJavaToScalaType(map.getKeyType), convertJavaToScalaType(map.getValueType))
        case set: SetType => DataType.Set(convertJavaToScalaType(set.getType))
        case list: ListType => DataType.List(convertJavaToScalaType(list.getType))
        case tuple: TupleType => DataType.Tuple(tuple.getTypes.map(convertJavaToScalaType):_*)
    }

    def fromString(typeString: String): Try[DataType] = {
      try {
        val cqlType = cqlTypeFactory.buildType(typeString)
        TSuccess(convertJavaToScalaType(cqlType))
      } catch {
        case e: Exception => Failure(e)
      }
    }

    def write(d: DataType) = JsString(d.stringRep)

    def read(value: JsValue) = value match {
      case JsString(string) => fromString(string) match {
        case TSuccess(columnType) => columnType
        case Failure(e) =>
          logger.warn(s"Received invalid column type '$string'", e)
          throw new IllegalArgumentException(s"Not a valid column type '$string'")
      }
      case _ => throw new IllegalArgumentException("Expected ColumnType as JsString")
    }
  }

  implicit object ResultJsonFormat extends RootJsonFormat[ResultJsonRepresentation] {
    def write(result: ResultJsonRepresentation) = JsString(result.string)

    def read(value: JsValue) = value match {
      case JsString(string) => ResultJsonRepresentation.fromString(string)
      case _ => throw new IllegalArgumentException("Expected Result as JsString")
    }
  }

  implicit val impThen = jsonFormat6(Then)
  implicit val impWhen = jsonFormat5(When)
  implicit val impPrimeQueryResult = jsonFormat(PrimeQuerySingle, "when", "then")
  implicit val impConnection = jsonFormat1(Connection)
  implicit val impQuery = jsonFormat6(Query)
  implicit val impPrimeCriteria = jsonFormat3(PrimeCriteria)
  implicit val impConflictingPrimes = jsonFormat1(ConflictingPrimes)
  implicit val impTypeMismatch = jsonFormat3(TypeMismatch)
  implicit val impTypeMismatches = jsonFormat1(TypeMismatches)
  implicit val impWhenPreparedSingle = jsonFormat3(WhenPrepared)
  implicit val impThenPreparedSingle = jsonFormat6(ThenPreparedSingle)
  implicit val impPrimePreparedSingle = jsonFormat(PrimePreparedSingle, "when", "then")
  implicit val impPreparedStatementExecution = jsonFormat6(PreparedStatementExecution)
  implicit val impPreparedStatementPreparation = jsonFormat1(PreparedStatementPreparation)
  implicit val impVersion = jsonFormat1(Version)
  implicit val impBatchQuery = jsonFormat4(BatchQuery)
  implicit val impBatchExecution = jsonFormat5(BatchExecution)
  implicit val impBatchQueryPrime = jsonFormat2(BatchQueryPrime)
  implicit val impBatchWhen = jsonFormat3(BatchWhen)
  implicit val impBatchPrimeSingle = jsonFormat(BatchPrimeSingle, "when", "then")
  implicit val impClientConnection = jsonFormat2(ClientConnection)
  implicit val impClientConnections = jsonFormat1(ClientConnections)
  implicit val impClosedConnections = jsonFormat(ClosedConnections, "closed_connections", "operation")
  implicit val impAcceptNewConnectionsEnabled = jsonFormat1(AcceptNewConnectionsEnabled)
  implicit val impRejectNewConnectionsEnabled = jsonFormat1(RejectNewConnectionsEnabled)

  implicit val impCriterna = jsonFormat1(Criteria)
  implicit val impAction = jsonFormat5(Action)
  implicit val impOutcoe = jsonFormat2(Outcome)
  implicit val impPrimePreparedMultiThen = jsonFormat2(ThenPreparedMulti)
  implicit val impPrimePreparedMulti = jsonFormat(PrimePreparedMulti, "when", "then")

}
