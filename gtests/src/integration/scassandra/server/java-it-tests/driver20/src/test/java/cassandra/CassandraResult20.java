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
import common.CassandraResult;
import common.CassandraRow;

import java.util.List;
import java.util.stream.Collectors;

public class CassandraResult20 implements CassandraResult<CassandraRow> {

    private ResultSet resultSet;
    private ResponseStatus result;

    public CassandraResult20(ResultSet resultSet) {
        this.resultSet = resultSet;
        this.result = new SuccessStatus();
    }

    public CassandraResult20(ResponseStatus unavailable) {
        result = unavailable;
    }

    @Override
    public List<CassandraRow> rows() {
        List<Row> all = resultSet.all();
        return all.stream().map(CassandraRow20::new).collect(Collectors.toList());
    }

    @Override
    public ResponseStatus status() {
        return result;
    }
}
