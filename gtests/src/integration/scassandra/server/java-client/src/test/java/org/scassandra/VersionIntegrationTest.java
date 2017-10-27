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

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VersionIntegrationTest {
    private static Scassandra scassandra = ScassandraFactory.createServer();

    // this worked when we depended on the sever as a jar that had the version in the manifest file
    @Ignore
    @Test
    public void getVersion() {
        scassandra.start();
        String version = scassandra.serverVersion();
        assertEquals("0.8.0-SNAPSHOT", version);
    }

}
