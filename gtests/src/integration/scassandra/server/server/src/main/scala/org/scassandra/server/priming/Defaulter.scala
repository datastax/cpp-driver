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
package org.scassandra.server.priming

import org.scassandra.codec.Consistency
import org.scassandra.codec.Consistency.Consistency
import org.scassandra.codec.datatype.DataType

object Defaulter {

  def defaultVariableTypesToVarChar(query : Option[String], dataTypes : Option[List[DataType]]) : Option[List[DataType]] = query match {
    case Some(queryText) =>
      val dTypes = dataTypes.getOrElse(Nil)
      val numberOfVariables = queryText.toCharArray.count(_ == '?')
      val deficit = numberOfVariables - dTypes.size
      if (deficit <= 0) {
        dataTypes
      } else {
        val defaults = (0 until deficit).map(_ => DataType.Varchar).toList
        Some(dTypes ++ defaults)
      }
    case None => dataTypes
  }

  def defaultColumnTypesToVarChar(columnTypes: Option[Map[String, DataType]], rows: Option[List[Map[String, Any]]]) = columnTypes match {
    case Some(_) => columnTypes
    case None =>
      val names = rows.getOrElse(Nil).flatMap(row => row.keys).distinct
      Some(names.map(n => (n, DataType.Varchar)).toMap)
  }

  def defaultConsistency(consistency: Option[List[Consistency]]) = consistency match {
    case Some(_) => consistency
    case None => Some(Consistency.all)
  }
}
