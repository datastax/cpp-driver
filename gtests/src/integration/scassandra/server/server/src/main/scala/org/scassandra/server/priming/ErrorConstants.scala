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

object ErrorConstants {
  val ReceivedResponse = "error.received_responses"
  val RequiredResponse = "error.required_responses"
  val NumberOfFailures = "error.num_failures"
  val DataPresent = "error.data_present"
  val WriteType = "error.write_type"
  val Alive = "error.alive"
  var Keyspace = "error.keyspace"
  var Table = "error.table"
  var Function = "error.function"
  var Argtypes = "error.arg_types"
  val Message = "error.message"
  var PrepareId = "error.prepare_id"
  var CloseType = "error.close_type"
  var ConsistencyLevel = "error.consistency_level"
}
