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
package common;

import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.cql.CqlType;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.ColumnTypes;
import org.scassandra.http.client.PrimingClient;

import java.nio.ByteBuffer;
import java.util.Map;

import static common.PortLocator.findFreePort;

abstract public class AbstractScassandraTest<C extends CassandraExecutor> {
    protected static Scassandra scassandra;
    protected static PrimingClient primingClient;
    protected static ActivityClient activityClient;

    private C cassandraExecutor;

    public AbstractScassandraTest(C cassandraExecutor) {
        this.cassandraExecutor = cassandraExecutor;
    }


    @BeforeClass
    public static void startScassandra() {
        scassandra = ScassandraFactory.createServer(findFreePort(), findFreePort());
        primingClient = scassandra.primingClient();
        activityClient = scassandra.activityClient();
        scassandra.start();
    }

    @AfterClass
    public static void stopScassandra() {
        scassandra.stop();
    }

    @Before
    public void setup() {
        activityClient.clearAllRecordedActivity();
        primingClient.clearAllPrimes();
    }

    @After
    public void shutdownCluster() {
        cassandraExecutor.close();
    }

    public C cassandra() {
        return cassandraExecutor;
    }

    public static Map<String, CqlType> cqlTypes(Map<String, ColumnTypes> columnTypes) {
        return Maps.transformValues(columnTypes, input -> input.getType());
    }

    public static byte[] getArray(ByteBuffer buffer) {
        int length = buffer.remaining();
        byte[] data = new byte[length];
        buffer.get(data);
        return data;
    }

}
