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

/**
 * Factory for creating Scassandra instances.
 */
public class ScassandraFactory {
    /**
     * @param binaryPort Cassandra binary port
     * @param adminPort Scassandra admin port
     * @return An instance of Scassandra configured with the given ports.
     */
    public static Scassandra createServer(int binaryPort, int adminPort) {
        return createServer("localhost", binaryPort, "localhost", adminPort);
    }

    /**
     * @param binaryListenAddress Defaults to localhost, override for example to 0.0.0.0
     * @param binaryPort Cassandra binary port
     * @param adminListenAddress Defaults to localhost, override for example to 0.0.0.0
     * @param adminPort Scassandra admin port
     * @return An instance of Scassandra configured with the given ports.
     */
    public static Scassandra createServer(String binaryListenAddress, int binaryPort, String adminListenAddress, int adminPort) {
        return createServer(binaryListenAddress, binaryPort, adminListenAddress, adminPort, 10);
    }

    /**
     * Creates a Scassandra instance with 8042 as the binary port and 8043 as the admin port.
     * @return Scassandra
     */
    public static Scassandra createServer() {
        return createServer("localhost", 8042, "localhost", 8043);
    }

    /**
     * @param binaryListenAddress Defaults to localhost, override for example to 0.0.0.0
     * @param binaryPort Cassandra binary port
     * @param adminListenAddress Defaults to localhost, override for example to 0.0.0.0
     * @param adminPort Scassandra admin port
     * @param startupTimeout Number of seconds to wait for SCassandra to start up
     * @return An instance of Scassandra configured with the given ports.
     */
    public static Scassandra createServer(String binaryListenAddress, int binaryPort, String adminListenAddress, int adminPort, long startupTimeout) {
        return new ScassandraRunner(binaryListenAddress, binaryPort, adminListenAddress, adminPort, startupTimeout);
    }
}
