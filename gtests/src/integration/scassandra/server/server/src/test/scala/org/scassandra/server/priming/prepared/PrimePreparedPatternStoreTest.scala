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
package org.scassandra.server.priming.prepared


import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scassandra.codec.Consistency._
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.messages.ColumnSpec._
import org.scassandra.codec.messages.{PreparedMetadata, QueryParameters, RowMetadata}
import org.scassandra.codec.{Execute, Prepare, Prepared, ProtocolVersion}
import org.scassandra.server.priming.query.Reply
import scodec.bits.ByteVector

class PrimePreparedPatternStoreTest extends FunSuite with Matchers with BeforeAndAfter {
  implicit val protocolVersion = ProtocolVersion.latest

  val id = ByteVector(1)

  var underTest : PrimePreparedPatternStore= _

  before {
    underTest = new PrimePreparedPatternStore
  }

  test("Should return None if pattern does not match") {
    //given
    val pattern = "select .* from people.*"
    val when = WhenPrepared(None, Some(pattern), Some(List(ONE)))
    val thenDo = ThenPreparedSingle(Some(List()))
    val preparedPrime = PrimePreparedSingle(when, thenDo)

    //when
    underTest.record(preparedPrime)

    //then
    val result = underTest("select name from users where age = '6'", Execute(id))
    result.isDefined should equal(false)
  }

  test("Should find prime if pattern and consistency match") {
    //given
    val pattern = "select .* from people.*"
    val when = WhenPrepared(None, Some(pattern), consistency = Some(List(ONE)))
    val thenDo = ThenPreparedSingle(Some(List()))

    val preparedPrime = PrimePreparedSingle(when, thenDo)

    //when
    underTest.record(preparedPrime)

    //then
    val result = underTest("select name from people where age = '6'", Execute(id, QueryParameters(consistency=ONE)))
    result.isDefined should equal(true)
    result.get should equal(thenDo.prime)
  }

  test("Should not find prime if pattern matches but consistency does not") {
    //given
    val pattern = "select .* from people.*"
    val when = WhenPrepared(None, Some(pattern), consistency = Some(List(ONE, TWO)))
    val thenDo = ThenPreparedSingle(Some(List()))

    val preparedPrime = PrimePreparedSingle(when, thenDo)

    //when
    underTest.record(preparedPrime)

    //then
    val result = underTest("select name from people where age = '6'", Execute(id, QueryParameters(consistency=THREE)))
    result.isDefined should equal(false)
  }

  test("Defaults consistencies to all") {
    val pattern = "select .* from people.*"
    val when = WhenPrepared(None, Some(pattern))
    val thenDo = ThenPreparedSingle(Some(List()))

    val preparedPrime = PrimePreparedSingle(when, thenDo)

    //when
    underTest.record(preparedPrime)

    //then
    underTest("select name from people where age = '6'", Execute(id, QueryParameters(consistency=ONE))).isDefined should equal(true)
    underTest("select name from people where age = '6'", Execute(id, QueryParameters(consistency=TWO))).isDefined should equal(true)
    underTest("select name from people where age = '6'", Execute(id, QueryParameters(consistency=THREE))).isDefined should equal(true)
    underTest("select name from people where age = '6'", Execute(id, QueryParameters(consistency=QUORUM))).isDefined should equal(true)
  }

  test("Clearing all the primes") {
    //given
    val when = WhenPrepared(None, Some("select .* from people.*"))
    val thenDo = ThenPreparedSingle(None)
    val prime = PrimePreparedSingle(when, thenDo)
    underTest.record(prime)
    //when
    underTest.clear()
    //then
    underTest.retrievePrimes().size should equal(0)
  }

  val factory = (p: PreparedMetadata, r: RowMetadata) => Prepared(id, p, r)

  test("Prepared prime - None when no match") {
    // when
    val prepared = underTest(Prepare("select * from people where a = ? and b = ? and c = ?"), factory)

    // then
    prepared.isDefined should equal(false)
  }

  test("Prepared prime - no parameters") {
    val pattern = "select .* from people.*"
    val query: String = "select * from people"
    val when = WhenPrepared(None, Some(pattern))
    val thenDo = ThenPreparedSingle(Some(List()))
    val prime = PrimePreparedSingle(when, thenDo)
    underTest.record(prime)

    // when
    val prepared = underTest(Prepare(query), factory)

    // then - should be a prepared with no column spec
    prepared should matchPattern { case Some(Reply(Prepared(`id`, PreparedMetadata(_, _, _, `Nil`), _), _, _)) => }
  }

  test("Prepared prime - with parameters") {
    val pattern = "select .* from people.*"
    val columnSpec = List(column("0", DataType.Varchar), column("1", DataType.Bigint))
    val query: String = "select * from people where first=? and last=?"
    val when = WhenPrepared(None, Some(pattern))
    val thenDo = ThenPreparedSingle(Some(List()), Some(columnSpec.map(_.dataType)))
    val prime = PrimePreparedSingle(when, thenDo)
    underTest.record(prime)

    // when
    val prepared = underTest(Prepare(query), factory)

    // then - should be a prepared with a column spec containing parameters.
    prepared should matchPattern { case Some(Reply(Prepared(`id`, PreparedMetadata(_, _, _, `columnSpec`), _), _, _)) => }
  }
}
