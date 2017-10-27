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
package org.scassandra.server.priming.cors

import com.typesafe.scalalogging.LazyLogging
import org.scassandra.server.ScassandraConfig
import spray.http.HttpHeaders._
import spray.http.HttpMethods._
import spray.http._
import spray.routing._

/**
  * A mixin to provide support for CORS headers as appropriate
  */
trait CorsSupport {
  this: HttpService with LazyLogging =>

  def cors[T]: Directive0 = mapRequestContext {
    ctx => ctx.withRouteResponseHandling {
      // If an OPTIONS request was rejected as 405, complete the request by responding with the
      // defined CORS details and the allowed options grabbed from the rejection
      case Rejected(reasons) if
      ctx.request.method == HttpMethods.OPTIONS &&
        reasons.exists(_.isInstanceOf[MethodRejection]) => {

        if (!ScassandraConfig.corsEnabled) {
          logger.warn(s"OPTIONS request made to ${ctx.request.uri} has been refused, because CORS is disabled. ${
            "Enable it by setting 'scassandra.cors.enabled = true'"
          }")
          ctx.complete(HttpResponse(StatusCodes.MethodNotAllowed))
        } else {
          logger.debug(s"OPTIONS request made to ${ctx.request.uri}")

          // supported methods for the requested endpoint
          val allowedMethods = reasons.collect { case r: MethodRejection => r.supported }

          ctx.complete(HttpResponse().withHeaders(List(
            `Access-Control-Allow-Origin`(AllOrigins),
            `Access-Control-Max-Age`(ScassandraConfig.corsMaxAge),
            `Access-Control-Allow-Credentials`(ScassandraConfig.corsAllowCredentials),
            `Access-Control-Allow-Methods`(OPTIONS, allowedMethods :_*),
            `Access-Control-Allow-Headers`(ScassandraConfig.corsAllowHeaders)
          )))
        }
      }
    } withHttpResponseHeadersMapped (headers => `Access-Control-Allow-Origin`(AllOrigins) :: headers)
  }
}
