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
package org.scassandra.server.priming.query

import java.util.regex.Pattern

import akka.actor.ActorRef
import akka.io.Tcp
import com.typesafe.scalalogging.LazyLogging
import org.scassandra.codec.Consistency._
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.{Message, Query, SetKeyspace}
import org.scassandra.server.priming.query.PrimeQueryStore.useKeyspace
import org.scassandra.server.priming.routes.PrimingJsonHelper
import org.scassandra.server.priming.{BadCriteria, PrimeAddResult, PrimeAddSuccess, PrimeValidator}

import scala.collection.immutable.Map
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

class PrimeQueryStore() extends LazyLogging {

  val validator = new PrimeValidator

  var queryPrimes = Map[PrimeCriteria, PrimeQuerySingle]()

  var queryPatternPrimes: Map[PrimeCriteria, PrimeQuerySingle] = Map()

  def getAllPrimes: List[PrimeQuerySingle] = queryPrimes.values.toList ++ queryPatternPrimes.values.toList

  def add(primeQuerySingle: PrimeQuerySingle): PrimeAddResult = {
    val p = primeQuerySingle.withDefaults
    PrimingJsonHelper.extractPrimeCriteria(p) match {
      case Success(criteria) =>
        validator.validate(criteria, p.prime, queryPrimes.keys.toList) match {
          case PrimeAddSuccess =>
            if (criteria.patternMatch) {
              queryPatternPrimes += (criteria -> p)
              PrimeAddSuccess
            } else {
              queryPrimes += (criteria -> p)
              PrimeAddSuccess
            }
          case notSuccess: PrimeAddResult =>
            notSuccess
        }
      case failure @ Failure(x) =>
        logger.warn(s"Received invalid prime $failure")
        BadCriteria(x.getMessage)
    }
  }

  def clear() = {
    queryPrimes = Map()
    queryPatternPrimes = Map()
  }

  def apply(query: Query): Option[Prime] = {
    logger.debug("Current primes: " + queryPrimes)
    logger.debug(s"Query for |$query|")

    def findPrime: ((PrimeCriteria, PrimeQuerySingle)) => Boolean = {
      entry => entry._1.query == query.query &&
        entry._1.consistency.contains(query.parameters.consistency)
    }

    def findPrimePattern: ((PrimeCriteria, PrimeQuerySingle)) => Boolean = {
      entry => {
        entry._1.query.r.findFirstIn(query.query) match {
          case Some(_) => entry._1.consistency.contains(query.parameters.consistency)
          case None => false
        }
      }
    }

    val matcher = useKeyspace.matcher(query.query)
    if (matcher.matches()) {
      val keyspaceName = matcher.group(1)
      logger.info(s"Use keyspace $keyspaceName")
      Some(Reply(SetKeyspace(keyspaceName)))
    } else {
      queryPrimes.find(findPrime).orElse(queryPatternPrimes.find(findPrimePattern)).map(_._2.prime)
    }
  }
}

object PrimeQueryStore {
   private [PrimeQueryStore] val useKeyspace = Pattern.compile("\\s*use\\s+(.*)$", Pattern.CASE_INSENSITIVE)
}

case class PrimeCriteria(query: String, consistency: List[Consistency], patternMatch : Boolean = false)

sealed trait Prime {
  val fixedDelay: Option[FiniteDuration]
  val variableTypes: Option[List[DataType]]
}


case class Reply(message: Message, fixedDelay: Option[FiniteDuration] = None, variableTypes: Option[List[DataType]] = None) extends Prime

sealed trait Fatal extends Prime {
  def produceFatalError(tcpConnection: ActorRef)
}

case class ClosedConnectionReport(command: String, fixedDelay: Option[FiniteDuration] = None, variableTypes: Option[List[DataType]] = None) extends Fatal {

  private lazy val closeCommand: Tcp.CloseCommand = command match {
    case "reset"      => Tcp.Abort
    case "halfclose"  => Tcp.ConfirmedClose
    case "close" | _  => Tcp.Close
  }

  override def produceFatalError(tcpConnection: ActorRef) = tcpConnection ! closeCommand
}
