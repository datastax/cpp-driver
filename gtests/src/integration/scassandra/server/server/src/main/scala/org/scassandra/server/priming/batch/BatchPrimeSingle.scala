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
package org.scassandra.server.priming.batch

import org.scassandra.codec.Consistency.Consistency
import org.scassandra.codec.messages.BatchQueryKind.BatchQueryKind
import org.scassandra.codec.messages.BatchType
import org.scassandra.codec.messages.BatchType.BatchType
import org.scassandra.server.priming.Defaulter
import org.scassandra.server.priming.query.Then
import org.scassandra.server.priming.routes.PrimingJsonHelper.extractPrime

case class BatchPrimeSingle(when: BatchWhen, thenDo: Then) {
  @transient lazy val prime = {
    extractPrime(thenDo)
  }

  def withDefaults: BatchPrimeSingle =
    // join queries into 1 string so '?' can be counted.
    copy(when.withDefaults, thenDo.withDefaults(Some(when.queries.map(_.text).mkString("\n"))))
}

case class BatchWhen(queries: List[BatchQueryPrime], consistency: Option[List[Consistency]] = None, batchType: Option[BatchType] = None) {
  def withDefaults: BatchWhen = copy(batchType = batchType.orElse(Some(BatchType.LOGGED)), consistency = Defaulter.defaultConsistency(consistency))
}

case class BatchQueryPrime(text: String, kind: BatchQueryKind)
