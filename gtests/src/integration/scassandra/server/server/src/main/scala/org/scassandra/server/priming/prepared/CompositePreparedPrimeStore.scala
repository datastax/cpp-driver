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

import org.scassandra.codec.messages.{PreparedMetadata, RowMetadata}
import org.scassandra.codec.{Execute, Prepare, Prepared, ProtocolVersion}
import org.scassandra.server.priming.query.Prime


class CompositePreparedPrimeStore(primeStores: PreparedStoreLookup*) extends PreparedStoreLookup {

  def apply(prepare: Prepare, preparedFactory: (PreparedMetadata, RowMetadata) => Prepared) : Option[Prime] = {
    // TODO: Fix duplicate lookup
    primeStores.collectFirst {
      case psb if psb(prepare, preparedFactory).isDefined => psb(prepare, preparedFactory).get
    }
  }

  def apply(queryText: String, execute: Execute)(implicit protocolVersion: ProtocolVersion) : Option[Prime] = {
    primeStores.collectFirst {
      case psb if psb(queryText, execute).isDefined => psb(queryText, execute).get
    }
  }
}
