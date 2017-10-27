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
import org.scassandra.server.priming.cors.CorsSupport
import org.scassandra.server.priming.json.PrimingJsonImplicits
import spray.routing.HttpService

trait VersionRoute extends HttpService with LazyLogging with CorsSupport {

  import PrimingJsonImplicits._

  val versionRoute =
    cors {
      path("version") {
        get {
          complete {
            val version = if (getClass.getPackage.getImplementationVersion == null) {
              "unknown"
            } else {
              getClass.getPackage.getImplementationVersion
            }
            Version(version)
          }
        }
      }
    }
}

case class Version(version: String)
