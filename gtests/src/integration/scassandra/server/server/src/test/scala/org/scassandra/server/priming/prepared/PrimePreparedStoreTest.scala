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
/*
* Copyright (C) 2014 Christopher Batey and Dogan Narinc
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

import org.scalatest.{FunSuite, Matchers}
import org.scassandra.codec.Consistency._
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.messages.ColumnSpec.column
import org.scassandra.codec.messages._
import org.scassandra.codec.{Execute, Prepare, Prepared, ProtocolVersion}
import org.scassandra.server.priming.ConflictingPrimes
import org.scassandra.server.priming.query.Reply
import scodec.bits.ByteVector

class PrimePreparedStoreTest extends FunSuite with Matchers {
  implicit val protocolVersion = ProtocolVersion.latest

  val id = ByteVector(1)

  test("Finds prime - no variables") {
    //given
    val underTest = new PrimePreparedStore
    val query: String = "select * from people where name = ?"
    val when = WhenPrepared(Some(query))
    val thenDo = ThenPreparedSingle(Some(List()))
    val prime = PrimePreparedSingle(when, thenDo)
    //when
    underTest.record(prime)
    val actualPrime = underTest(query, Execute(id))
    //then
    actualPrime.get should equal(thenDo.prime)
  }

  test("Clearing all the primes") {
    //given
    val underTest = new PrimePreparedStore
    val when = WhenPrepared(Some(""))
    val thenDo = ThenPreparedSingle(None)
    val prime = PrimePreparedSingle(when, thenDo)
    underTest.record(prime)
    //when
    underTest.clear()
    //then
    underTest.retrievePrimes().size should equal(0)
  }

  test("Priming consistency. Should match on consistency") {
    val underTest = new PrimePreparedStore
    val query: String = "select * from people where name = ?"
    val consistencies = List(ONE, TWO)
    val when = WhenPrepared(Some(query), None, Some(consistencies))
    val thenDo = ThenPreparedSingle(Some(List()))
    val prime = PrimePreparedSingle(when, thenDo)
    underTest.record(prime)
    //when
    val primeForOne = underTest(query, Execute(id, QueryParameters(consistency=ONE)))
    val primeForTwo = underTest(query, Execute(id, QueryParameters(consistency=TWO)))
    val primeForAll = underTest(query, Execute(id, QueryParameters(consistency=ALL)))
    //then
    primeForOne.isDefined should equal(true)
    primeForTwo.isDefined should equal(true)
    primeForAll.isDefined should equal(false)
  }

  test("Priming consistency. Should default to all consistencies") {
    val underTest = new PrimePreparedStore
    val query: String = "select * from people where name = ?"
    val when = WhenPrepared(Some(query), None)
    val thenDo = ThenPreparedSingle(Some(List()))
    val prime = PrimePreparedSingle(when, thenDo)
    underTest.record(prime)
    //when
    val primeForOne = underTest(query, Execute(id, QueryParameters(consistency=ONE)))
    val primeForTwo = underTest(query, Execute(id, QueryParameters(consistency=TWO)))
    val primeForAll = underTest(query, Execute(id, QueryParameters(consistency=ALL)))
    val primeForLocalOne = underTest(query, Execute(id, QueryParameters(consistency=LOCAL_ONE)))
    //then
    primeForOne.isDefined should equal(true)
    primeForTwo.isDefined should equal(true)
    primeForAll.isDefined should equal(true)
    primeForLocalOne.isDefined should equal(true)
  }

  test("Conflicting primes") {
    val underTest = new PrimePreparedStore
    val query: String = "select * from people where name = ?"
    val thenDo = ThenPreparedSingle(Some(List()))
    val primeForOneAndTwo = PrimePreparedSingle(WhenPrepared(Some(query), None, Some(List(ONE, TWO))), thenDo)
    val primeForTwoAndThree = PrimePreparedSingle(WhenPrepared(Some(query), None, Some(List(TWO, THREE))), thenDo)
    //when
    underTest.record(primeForOneAndTwo)
    val result = underTest.record(primeForTwoAndThree)
    //then
    result.isInstanceOf[ConflictingPrimes] should equal(true)
  }

  val factory = (p: PreparedMetadata, r: RowMetadata) => Prepared(id, p, r)

  test("Prepared prime - None when no match") {
    val underTest = new PrimePreparedStore

    // when
    val prepared = underTest(Prepare("select * from people where a = ? and b = ? and c = ?"), factory)

    // then
    prepared.isDefined should equal(false)
  }

  test("Prepared prime - no parameters") {
    val underTest = new PrimePreparedStore
    val query: String = "select * from people"
    val when = WhenPrepared(Some(query), None)
    val thenDo = ThenPreparedSingle(Some(List()))
    val prime = PrimePreparedSingle(when, thenDo)
    underTest.record(prime)

    // when
    val prepared = underTest(Prepare(query), factory)

    // then - should be a prepared with no column spec
    prepared should matchPattern { case Some(Reply(Prepared(`id`, PreparedMetadata(_, _, _, `Nil`), _), _, _)) => }
  }

  test("Prepared prime - with parameters") {
    val underTest = new PrimePreparedStore
    val query: String = "select * from people where first=? and last=?"
    val columnSpec = List(column("0", DataType.Varchar), column("1", DataType.Bigint))
    val when = WhenPrepared(Some(query), None)
    val thenDo = ThenPreparedSingle(Some(List()), Some(columnSpec.map(_.dataType)))
    val prime = PrimePreparedSingle(when, thenDo)
    underTest.record(prime)

    // when
    val prepared = underTest(Prepare(query), factory)

    // then - should be a prepared with a column spec containing parameters.
    prepared should matchPattern { case Some(Reply(Prepared(`id`, PreparedMetadata(_, _, _, `columnSpec`), _), _, _)) => }
  }
}
