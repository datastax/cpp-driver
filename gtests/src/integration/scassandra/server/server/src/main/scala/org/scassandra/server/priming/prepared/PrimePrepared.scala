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

import com.typesafe.scalalogging.LazyLogging
import org.scassandra.codec.Consistency.Consistency
import org.scassandra.codec.datatype.DataType
import org.scassandra.codec.{Execute, Prepare}
import org.scassandra.server.priming.Defaulter._
import org.scassandra.server.priming.json._
import org.scassandra.server.priming.query.ThenProvider
import org.scassandra.server.priming.routes.PrimingJsonHelper.extractPrime

trait PreparedPrimeCriteria {
  def matches(prepare: Prepare): Boolean
  def matches(queryText: String, execute: Execute): Boolean
}

class DefaultPreparedPrimeCriteria(query: String, consistency: List[Consistency]) extends PreparedPrimeCriteria {
  override def matches(prepare: Prepare): Boolean = prepare.query.equals(query)

  override def matches(queryText: String, execute: Execute): Boolean =
    queryText.equals(query) && consistency.contains(execute.parameters.consistency)
}

sealed trait PreparedPrimeIncoming {
  val when: WhenPrepared
  val thenDo: ThenPrepared

  def withDefaults: PreparedPrimeIncoming
}

sealed trait ThenPrepared {
  val variable_types: Option[List[DataType]]
}

case class PrimePreparedSingle(when: WhenPrepared, thenDo: ThenPreparedSingle) extends PreparedPrimeIncoming {
  override def withDefaults: PrimePreparedSingle = copy(when.withDefaults, thenDo.withDefaults(when))
}

case class PrimePreparedMulti(when: WhenPrepared, thenDo: ThenPreparedMulti) extends PreparedPrimeIncoming {
  override def withDefaults: PrimePreparedMulti = copy(when.withDefaults, thenDo.withDefaults(when))
}

case class WhenPrepared(query: Option[String] = None,
                        queryPattern: Option[String] = None,
                        consistency: Option[List[Consistency]] = None) {
  def withDefaults: WhenPrepared = copy(consistency = defaultConsistency(consistency))
}

case class ThenPreparedSingle(rows: Option[List[Map[String, Any]]],
                              variable_types: Option[List[DataType]] = None,
                              column_types: Option[Map[String, DataType]] = None,
                              result : Option[ResultJsonRepresentation] = Some(Success),
                              fixedDelay : Option[Long] = None,
                              config: Option[Map[String, String]] = None) extends ThenProvider with ThenPrepared {
  @transient lazy val prime = {
    extractPrime(this)
  }

  def withDefaults(when: WhenPrepared): ThenPreparedSingle = copy(variable_types = defaultVariableTypesToVarChar(when.query, variable_types), column_types = defaultColumnTypesToVarChar(column_types, rows))
}

case class ThenPreparedMulti(variable_types: Option[List[DataType]] = None,
                            outcomes: List[Outcome]) extends ThenPrepared {
  def withDefaults(when: WhenPrepared): ThenPreparedMulti =  {
    this.copy(variable_types = defaultVariableTypesToVarChar(when.query, variable_types), outcomes = outcomes.map(_.withDefaults))
  }
}

case class Outcome(criteria: Criteria, action: Action) {
  def withDefaults: Outcome = copy(action = action.withDefaults)
}

case class Criteria(variable_matcher: List[VariableMatch])

case class Action(rows: Option[List[Map[String, Any]]],
                  column_types: Option[Map[String, DataType]] = None,
                  result : Option[ResultJsonRepresentation] = Some(Success),
                  fixedDelay : Option[Long] = None,
                  config: Option[Map[String, String]] = None) extends ThenProvider {
  @transient lazy val prime = {
    extractPrime(this)
  }

  def withDefaults: Action = this.copy(column_types = defaultColumnTypesToVarChar(column_types, rows))
}

sealed trait VariableMatch {
  def test(variable: Option[Any], dataType: DataType): Boolean
}

case class ExactMatch(matcher: Option[Any]) extends VariableMatch with LazyLogging {

  def test(variable: Option[Any], dataType: DataType): Boolean = {
    (variable, matcher) match {
      case (Some(v), Some(x)) if dataType.native.isDefinedAt(x) =>
        dataType.native(x).equals(v) // convert into data type's native value and then compare.
      case (Some(v), Some(x)) =>
        logger.warn("Unsure how to convert matcher value of $x to data type $dataType, forgoing comparison")
        false
      case (None, None) => true
      case _ => false // one is none and another is not.
    }
  }
}

case object AnyMatch extends VariableMatch {
  def test(variable: Option[Any], dataType: DataType): Boolean = {
    true
  }
}
