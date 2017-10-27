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
package org.scassandra.server.cors

import com.typesafe.scalalogging.LazyLogging
import org.scalatest._
import org.scassandra.server.ScassandraConfig
import org.scassandra.server.priming.cors.CorsSupport
import spray.routing.HttpService
import spray.testkit.ScalatestRouteTest


class CorsSupportTest extends FlatSpec
  with ScalatestRouteTest
  with HttpService
  with LazyLogging
  with CorsSupport
  with Matchers {
  def actorRefFactory = system

  val testRoute = path("test") {
    cors {
      get {
        complete((200, "'CORS it works!"))
      } ~
      post {
        complete((200, "'CORS I'll update that!"))
      }
    }
  }

  "A CORS route" should "work" in {
    Get("/test") ~> testRoute ~> check {
      status.intValue should be(200)
      responseAs[String] should be("'CORS it works!")
    }
    Post("/test") ~> testRoute ~> check {
      status.intValue should be(200)
      responseAs[String] should be("'CORS I'll update that!")
    }
  }

  it should "respond to OPTIONS requests properly" in {
    Options("/test") ~> testRoute ~> check {
      status.intValue should be(200)
      header("Access-Control-Allow-Headers").isDefined should be(true)

      val allowMethods = header("Access-Control-Allow-Methods").get.value.split(",").map(m => m.trim)
      Array("OPTIONS", "POST", "GET") foreach {
        allowMethods should contain(_)
      }
    }
  }

  it should "respond to all requests with the Access-Control-Allow-Origin header" in {
    Get("/test") ~> testRoute ~> check {
      header("Access-Control-Allow-Origin").isDefined should be(true)
    }
    Post("/test") ~> testRoute ~> check {
      header("Access-Control-Allow-Origin").isDefined should be(true)
    }
  }
}
