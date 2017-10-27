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
package org.scassandra;

import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.CurrentClient;
import org.scassandra.http.client.PrimingClient;

/**
 * Interface with Scassandra.
 */
public interface Scassandra {
    /**
     * Retrieves a Priming client that is configured with the same admin port
     * as Scassandra.
     * @return PrimingClient
     */
    PrimingClient primingClient();

    /**
     * Retrieves an Activity client that is configured with the same admin port
     * as Scassandra.
     * @return ActivityClient
     */
    ActivityClient activityClient();

    /**
     * Retrieves a Current client that is configured with the same admin port
     * as Scassandra.
     * @return CurrentClient
     */
    CurrentClient currentClient();

    /**
     * Start Scassandra. This will result in both the binary port for Cassandra to be opened
     * and the admin port for priming and verifying recorded activity.
     */
    void start();

    /**
     * Stops Scassandra.
     */
    void stop();

    /**
     * The port that Scassandars REST API is listening on.
     * This is the port that Priming / Activity verification happens on.
     * @return Scassandra admin port
     */
    int getAdminPort();

    /**
     * The port Scassandra is listening on for connections from Cassandra.
     * @return Scassandra binary port
     */
    int getBinaryPort();

    /**
     * @return Scassandra server version.
     */
    String serverVersion();
}
