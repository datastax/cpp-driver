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
package org.scassandra.server

import com.datastax.driver.core.exceptions.{ProtocolError => ProtocolErrorDriver, ServerError => ServerErrorDriver, SyntaxError => SyntaxErrorDriver, _}
import com.datastax.driver.core.policies.FallthroughRetryPolicy
import org.scalatest.concurrent.ScalaFutures
import org.scassandra.server.priming.ErrorConstants
import org.scassandra.server.priming.json._
import org.scassandra.server.priming.query.{Then, When}

import scala.reflect.Manifest

class JavaDriverIntegrationTest extends AbstractIntegrationTest with ScalaFutures {

  test("Should by by default return empty result set for any query") {
    val result = session.execute("select * from people")

    result.all().size() should equal(0)
  }

  test("Test prime and query with many rows") {
    // priming
    val whenQuery = "Test prime and query with many rows"
    val rows: List[Map[String, String]] = List(Map("name" -> s"Chris"), Map("name"->"Alexandra"))
    prime(When(query = Some(whenQuery)), rows)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(2)
    results.get(0).getString("name") should equal("Chris")
    results.get(1).getString("name") should equal("Alexandra")
  }

  test("Test prime and query with many columns") {
    // priming
    val whenQuery = "Test prime and query with many columns"
    val rows: List[Map[String, String]] = List(Map("name" -> s"Chris", "age"->"28"), Map("name"->"Alexandra", "age"->"24"))
    prime(When(query = Some(whenQuery)), rows)

    val result = session.execute(whenQuery)

    val results = result.all()
    results.size() should equal(2)
    results.get(0).getString("name") should equal("Chris")
    results.get(0).getString("age") should equal("28")
    results.get(1).getString("name") should equal("Alexandra")
    results.get(1).getString("age") should equal("24")
  }

  def expectException[T <: AnyRef](result: ResultJsonRepresentation, config: Option[Map[String, String]] = None)(implicit manifest: Manifest[T]): Unit = {
    // create a temporary cluster, since these queries may change the behavior of how the driver
    // sees a host.
    val cluster = builder().withRetryPolicy(FallthroughRetryPolicy.INSTANCE).build()
    try {
      val session = cluster.connect()
      val whenQuery = "read timeout query"
      prime(When(query = Some(whenQuery)), Then(None, result = Some(result), config = config))

      intercept[T] {
        session.execute(whenQuery)
      }
    } finally {
      cluster.close()
    }
  }

  test("Test read timeout on query") {
    expectException[ReadTimeoutException](ReadTimeout)
  }

  test("Test unavailable exception on query") {
    expectException[UnavailableException](Unavailable)
  }

  test("Test write timeout on query") {
    expectException[WriteTimeoutException](WriteTimeout)
  }

  test("Test server error on query") {
    expectException[ServerErrorDriver](ServerError)
  }

  test("Test protocol error on query") {
    expectException[DriverInternalError](ProtocolError)
  }

  test("Test bad credentials on query") {
    expectException[AuthenticationException](BadCredentials)
  }

  test("Test overloaded on query") {
    expectException[OverloadedException](Overloaded)
  }

  test("Test is bootstrapping on query") {
    // Expect a NHAE since on bootstrapping driver will try next host.
    expectException[NoHostAvailableException](IsBootstrapping)
  }

  test("Test truncate error on query") {
    expectException[TruncateException](TruncateError)
  }

  test("Test syntax error on query") {
    expectException[SyntaxErrorDriver](SyntaxError)
  }

  test("Test unauthorized error on query") {
    expectException[UnauthorizedException](Unauthorized)
  }

  test("Test invalid error on query") {
    expectException[InvalidQueryException](Invalid)
  }

  test("Test config error on query") {
    expectException[InvalidConfigurationInQueryException](ConfigError)
  }

  test("Test already exists error on query") {
    expectException[AlreadyExistsException](AlreadyExists, Some(Map(ErrorConstants.Keyspace -> "Keyspace")))
  }

  test("Test unprepared on query") {
    expectException[DriverInternalError](Unprepared, Some(Map(ErrorConstants.PrepareId -> "0x8675")))
  }

  test("Test closed connection on query") {
    expectException[TransportException](ClosedConnection, Some(Map(ErrorConstants.CloseType -> "close")))
  }
}
