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

import spray.testkit.ScalatestRouteTest
import org.scalatest.{Matchers, FunSpec}
import spray.http.StatusCodes._

class VersionRouteTest extends FunSpec with ScalatestRouteTest with VersionRoute with Matchers {
  implicit def actorRefFactory = system

  describe("Version info") {
    it("should get it from the implementation version") {
      Get("/version") ~> versionRoute ~> check {
        status should equal(OK)
      }
    }
  }
}
