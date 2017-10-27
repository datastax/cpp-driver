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

import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory

object ScassandraConfig {
  private val config =  ConfigFactory.load()

  val binaryPort           = config.getInt("scassandra.binary.port")
  val binaryListenAddress  = config.getString("scassandra.binary.listen-address")
  val adminPort            = config.getInt("scassandra.admin.port")
  val adminListenAddress   = config.getString("scassandra.admin.listen-address")

  val corsEnabled          = config.getBoolean("scassandra.cors.enabled")
  val corsMaxAge           = config.getLong("scassandra.cors.max-age")
  val corsAllowCredentials = config.getBoolean("scassandra.cors.allow-credentials")
  val corsAllowHeaders     = config.getString("scassandra.cors.allow-headers")

  val startupTimeout       = config.getDuration("scassandra.startup-timeout-ms", TimeUnit.SECONDS)
}
