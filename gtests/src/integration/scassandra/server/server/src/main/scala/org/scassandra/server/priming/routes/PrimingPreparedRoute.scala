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

import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.priming._
import org.scassandra.server.priming.cors.CorsSupport
import org.scassandra.server.priming.json._
import org.scassandra.server.priming.prepared._
import spray.http.StatusCodes
import spray.routing.HttpService

trait PrimingPreparedRoute extends HttpService with LazyLogging with CorsSupport {

  import PrimingJsonImplicits._

  implicit val primePreparedStore: PrimePreparedStore
  implicit val primePreparedPatternStore: PrimePreparedPatternStore
  implicit val primePreparedMultiStore: PrimePreparedMultiStore

  val routeForPreparedPriming =
    cors {
      path("prime-prepared-multi") {
        post {
          entity(as[PrimePreparedMulti]) { prime: PrimePreparedMulti =>
            complete {
              logger.info(s"Received a prepared multi prime $prime")
              primePreparedMultiStore.record(prime)
              StatusCodes.OK
            }
          }
        } ~
          delete {
            complete {
              primePreparedMultiStore.clear()
              StatusCodes.OK
            }
          } ~
          get {
            complete {
              primePreparedMultiStore.retrievePrimes()
            }
          }
      } ~
        path("prime-prepared-single") {
          post {
            entity(as[PrimePreparedSingle]) { prime =>
              complete {
                val storeToUse = if (prime.when.query.isDefined && prime.when.queryPattern.isEmpty) {
                  Some(primePreparedStore)
                } else if (prime.when.queryPattern.isDefined && prime.when.query.isEmpty) {
                  Some(primePreparedPatternStore)
                } else {
                  None
                }

                storeToUse match {
                  case Some(store) => store.record(prime) match {
                    case cp: ConflictingPrimes => StatusCodes.BadRequest -> cp
                    case tm: TypeMismatches => StatusCodes.BadRequest -> tm
                    case _ => StatusCodes.OK
                  }
                  case None =>
                    StatusCodes.BadRequest -> "Must specify either query or queryPattern, not both"
                }
              }
            }
          } ~
            delete {
              complete {
                primePreparedStore.clear()
                primePreparedPatternStore.clear()
                StatusCodes.OK
              }
            } ~
            get {
              complete {
                primePreparedStore.retrievePrimes() // TODO:  Shouldn't this also hit the pattern store?  It didn't before.
              }
            }
        }
    }
}
