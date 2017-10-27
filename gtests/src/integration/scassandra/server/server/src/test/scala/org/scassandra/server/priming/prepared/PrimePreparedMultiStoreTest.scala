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
import org.scassandra.codec.Consistency.{all => ALL, _}
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.messages.ColumnSpec._
import org.scassandra.codec.messages.{PreparedMetadata, QueryParameters, RowMetadata}
import org.scassandra.codec.{QueryValue => v, Consistency => _, _}
import org.scassandra.server.priming.json.{ReadTimeout, Success, WriteTimeout}
import org.scassandra.server.priming.query.Reply
import scodec.bits.ByteVector

// todo generalise all the prepared stores, very little difference
class PrimePreparedMultiStoreTest extends FunSuite with Matchers with BeforeAndAfter {
  implicit val protocolVersion = ProtocolVersion.latest

  val id = ByteVector(1)

  var underTest: PrimePreparedMultiStore = _

  before {
    underTest = new PrimePreparedMultiStore
  }

  test("Match on variable type - success") {
    val variableTypes = List(DataType.Text)
    val action = Action(Some(List()))
    val thenDo: ThenPreparedMulti = ThenPreparedMulti(Some(variableTypes), List(
      Outcome(Criteria(List(ExactMatch(Some("Chris")))), action)))
    val queryText = "Some query"
    underTest.record(PrimePreparedMulti(WhenPrepared(Some(queryText)), thenDo))

    val preparedPrime = underTest(queryText, Execute(id, QueryParameters(consistency=ONE, values=Some(List(v("Chris", DataType.Text))))))

    preparedPrime.get should equal (action.prime)
  }

  test("Match on variable type - multiple options") {
    val variableTypes = List(DataType.Text)
    val danielAction = Action(Some(List()), result = Some(WriteTimeout))
    val chrisAction = Action(Some(List()), result = Some(ReadTimeout))
    val thenDo: ThenPreparedMulti = ThenPreparedMulti(Some(variableTypes), List(
      Outcome(Criteria(List(ExactMatch(Some("Chris")))), chrisAction),
      Outcome(Criteria(List(ExactMatch(Some("Daniel")))), danielAction)
    ))
    val queryText = "Some query"
    underTest.record(PrimePreparedMulti(WhenPrepared(Some(queryText)), thenDo))

    val chrisPrime = underTest(queryText, Execute(id, QueryParameters(consistency=ONE, values=Some(List(v("Chris", DataType.Text))))))
    chrisPrime.get should equal (chrisAction.prime)

    val danielPrime = underTest(queryText, Execute(id, QueryParameters(consistency=ONE, values=Some(List(v("Daniel", DataType.Text))))))
    danielPrime.get should equal (danielAction.prime)
  }

  test("Match on consistency") {
    val variableTypes = List(DataType.Text)
    val thenDo: ThenPreparedMulti = ThenPreparedMulti(Some(variableTypes), List(
      Outcome(Criteria(List(ExactMatch(Some("Daniel")))), Action(Some(List()), result = Some(WriteTimeout)))
    ))
    val queryText = "Some query"
    val when: WhenPrepared = WhenPrepared(Some(queryText), consistency = Some(List(TWO)))
    underTest.record(PrimePreparedMulti(when, thenDo))

    val preparedPrime = underTest(queryText, Execute(id, QueryParameters(consistency=ONE, values=Some(List(v("Daniel", DataType.Text))))))

    preparedPrime should equal(None)
  }

  test("Returns all the primes") {
    val thenDo: ThenPreparedMulti = ThenPreparedMulti(Some(List(DataType.Text)), List())
    val when: WhenPrepared = WhenPrepared(Some("Some query"), None, Some(ALL))
    val prime: PrimePreparedMulti = PrimePreparedMulti(when, thenDo)
    underTest.record(prime)

    val primes = underTest.retrievePrimes()

    primes.size should equal(1)
    primes.head should equal(prime)
  }

  test("Clearing all the primes") {
    //given
    val variableTypes = List(DataType.Text)
    val thenDo: ThenPreparedMulti = ThenPreparedMulti(Some(variableTypes), List(
      Outcome(Criteria(List(ExactMatch(Some("Chris")))), Action(Some(List()), result = Some(Success)))))
    val queryText = "Some query"
    underTest.record(PrimePreparedMulti(WhenPrepared(Some(queryText)), thenDo))
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
    val thenDo: ThenPreparedMulti = ThenPreparedMulti(None, Nil)
    val queryText = "Some query"
    underTest.record(PrimePreparedMulti(WhenPrepared(Some(queryText)), thenDo))

    // when
    val prepared = underTest(Prepare(queryText), factory)

    // then - should be a prepared with no column spec
    prepared should matchPattern { case Some(Reply(Prepared(`id`, PreparedMetadata(_, _, _, `Nil`), _), _, _)) => }
  }

  test("Prepared prime - with parameters") {
    val columnSpec = List(column("0", DataType.Text))
    val danielAction = Action(Some(List()), result = Some(WriteTimeout))
    val chrisAction = Action(Some(List()), result = Some(ReadTimeout))
    val thenDo: ThenPreparedMulti = ThenPreparedMulti(Some(columnSpec.map(_.dataType)), List(
      Outcome(Criteria(List(ExactMatch(Some("Chris")))), chrisAction),
      Outcome(Criteria(List(ExactMatch(Some("Daniel")))), danielAction)
    ))
    val queryText = "Some query where a = ?"
    underTest.record(PrimePreparedMulti(WhenPrepared(Some(queryText)), thenDo))

    // when
    val prepared = underTest(Prepare(queryText), factory)

    // then - should be a prepared with a column spec containing parameters.
    prepared should matchPattern { case Some(Reply(Prepared(`id`, PreparedMetadata(_, _, _, `columnSpec`), _), _, _)) => }
  }
}
