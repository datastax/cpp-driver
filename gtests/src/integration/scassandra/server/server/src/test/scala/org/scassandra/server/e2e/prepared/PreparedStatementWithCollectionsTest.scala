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
package org.scassandra.server.e2e.prepared

import java.util

import com.datastax.driver.core.Row
import dispatch.Defaults._
import dispatch._
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.ScalaFutures
import org.scassandra.codec.datatype.DataType
import org.scassandra.server.priming.prepared.{ThenPreparedSingle, WhenPrepared}
import org.scassandra.server.{AbstractIntegrationTest, PrimingHelper}

class PreparedStatementWithCollectionsTest  extends AbstractIntegrationTest with BeforeAndAfter with ScalaFutures {

  before {
    val svc = url("http://localhost:8043/prime-prepared-single").DELETE
    val response = Http(svc OK as.String)
    response()
  }

  test("Text map as a variable and column type") {
    // given
    val preparedStatementText = "Select * from people where id = ? and map = ?"
    val primedRow = Map[String, Any]("map_column" -> Map("the" -> "result"))
    val mapVariale = new util.HashMap[String, String]()
    mapVariale.put("one", "ONE")
    mapVariale.put("two", "TWO")
    val variableTypes: List[DataType] = List(DataType.Int, DataType.Map(DataType.Text, DataType.Text))

    PrimingHelper.primePreparedStatement(
      WhenPrepared(Some(preparedStatementText)),
      ThenPreparedSingle(Some(List(primedRow)),
        variable_types = Some(variableTypes),
        column_types = Some(Map[String, DataType]("map_column" -> DataType.Map(DataType.Text, DataType.Text)))
      )
    )

    // when
    val preparedStatement = session.prepare(preparedStatementText)
    val boundStatement = preparedStatement.bind(Int.box(1), mapVariale)
    val result = session.execute(boundStatement)

    // then
    val all: util.List[Row] = result.all()
    all.size() should equal(1)
    val resultRow = all.get(0)

    val resultMap = resultRow.getMap("map_column", classOf[String], classOf[String])
    resultMap.get("the") should equal("result")
  }
}
