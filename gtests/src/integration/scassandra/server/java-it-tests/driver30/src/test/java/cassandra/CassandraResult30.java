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
package cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import common.CassandraResultV3;
import common.CassandraRowV3;

import java.util.List;
import java.util.stream.Collectors;

public class CassandraResult30 implements CassandraResultV3 {

    private ResultSet resultSet;
    private ResponseStatus result;

    public CassandraResult30(ResultSet resultSet) {
        this.resultSet = resultSet;
        this.result = new SuccessStatus();
    }

    public CassandraResult30(ResponseStatus result) {
        this.result = result;
    }

    @Override
    public List<CassandraRowV3> rows() {
        List<Row> all = resultSet.all();
        return all.stream().map(CassandraRow30::new).collect(Collectors.toList());
    }

    @Override
    public ResponseStatus status() {
        return result;
    }
}
