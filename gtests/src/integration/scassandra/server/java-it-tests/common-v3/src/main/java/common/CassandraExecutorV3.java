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

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;

public interface CassandraExecutorV3 extends CassandraExecutor<CassandraResultV3> {

    /**
     * Create a {@link TupleType} from the given {@link DataType}s.
     *
     * This is abstract as the means for creating a {@link TupleType} varies between versions of the driver.
     * @param dataTypes Types, in order, to be part of the tuple.
     * @return generated tuple type.
     */
    TupleType tupleType(DataType... dataTypes);
}
