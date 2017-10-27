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
package just30;/*
 * Copyright (C) 2014 Christopher Batey
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

import cassandra.CassandraExecutor30;
import com.datastax.driver.core.Cluster;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import common.AbstractScassandraTest;
import org.junit.Test;
import org.scassandra.http.client.ColumnTypes;
import org.scassandra.http.client.PrimingRequest;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MetaDataPriming30 extends AbstractScassandraTest {
    public static final String CUSTOM_CLUSTER_NAME = "custom cluster name";

    public MetaDataPriming30() {
        super(new CassandraExecutor30(scassandra.getBinaryPort()));
    }

    @Test
    public void testPrimingOfClusterName() {
        //then
        Map<String, ColumnTypes> columnTypes = ImmutableMap.of("tokens",ColumnTypes.VarcharSet);
        String query = "SELECT * FROM system.local WHERE key='local'";
        Map<String, Object> row = new HashMap<>();
        row.put("cluster_name", CUSTOM_CLUSTER_NAME);
        row.put("partitioner","org.apache.cassandra.dht.Murmur3Partitioner");
        row.put("data_center","dc1");
        row.put("tokens", Sets.newHashSet("1743244960790844724"));
        row.put("rack","rc1");
        row.put("release_version","3.0");
        PrimingRequest prime = PrimingRequest.queryBuilder()
                .withQuery(query)
                .withColumnTypes(columnTypes)
                .withRows(row)
                .build();
        primingClient.primeQuery(prime);

        //when
        Cluster cluster = Cluster.builder().addContactPoint("localhost")
                .withPort(scassandra.getBinaryPort()).build();
        cluster.connect();

        //then
        assertEquals(CUSTOM_CLUSTER_NAME, cluster.getMetadata().getClusterName());
    }
}
