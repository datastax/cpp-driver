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

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Optional;
import common.*;
import io.netty.channel.EventLoopGroup;
import org.scassandra.http.client.BatchType;
import org.scassandra.http.client.Result;
import org.scassandra.http.client.WriteTypePrime;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static common.Config.KEYSPACE;
import static org.scassandra.http.client.Result.*;

public class CassandraExecutor21 implements CassandraExecutorV3 {
    private Cluster cluster;
    private Session session;

    public CassandraExecutor21(int binaryPort) {
        NettyOptions closeQuickly = new NettyOptions() {
            public void onClusterClose(EventLoopGroup eventLoopGroup) {
                //Shutdown immediately, since we close cluster when finished, we know nothing new coming through.
                eventLoopGroup.shutdownGracefully(0, 1000, TimeUnit.MILLISECONDS).syncUninterruptibly();
            }
        };
        cluster = Cluster.builder().addContactPoint(Config.NATIVE_HOST)
                .withPort(binaryPort)
                .withNettyOptions(closeQuickly)
                .withTimestampGenerator(ServerSideTimestampGenerator.INSTANCE)
                .build();
        session = cluster.connect(KEYSPACE);
    }

    @Override
    public CassandraResult21 executeQuery(String query) {
        return execute(session::execute, query);
    }

    @Override
    public CassandraResult21 executeSimpleStatement(String query, String consistency) {
        SimpleStatement simpleStatement = new SimpleStatement(query);
        Statement statement = simpleStatement.setConsistencyLevel(ConsistencyLevel.valueOf(consistency));
        return this.execute(session::execute, statement);
    }

    @Override
    public void prepare(String query) {
        session.prepare(query);
    }

    @Override
    public CassandraResult21 prepareAndExecute(String query, Object... variable) {
        PreparedStatement prepare = session.prepare(query);
        BoundStatement bind = prepare.bind(variable);
        return this.execute(session::execute, bind);
    }

    @Override
    public CassandraResult21 prepareAndExecuteWithConsistency(String query, String consistency, Object... vars) {
        PreparedStatement prepare = session.prepare(query);
        prepare.setConsistencyLevel(ConsistencyLevel.valueOf(consistency));
        BoundStatement bind = prepare.bind(vars);
        return this.execute(session::execute, bind);
    }

    @Override
    public CassandraResult21 executeSelectWithBuilder(String table, Optional<WhereEquals> clause) {
        Select query = QueryBuilder.select().all().from(table);
        if (clause.isPresent()) {
            query.where(eq(clause.get().getField(), clause.get().getValue()));
        }
        return new CassandraResult21(session.execute(query));
    }

    @Override
    public CassandraResult21 executeSelectWithBuilder(String table) {
        return executeSelectWithBuilder(table, Optional.<WhereEquals>absent());
    }

    @Override
    public CassandraResult21 executeBatch(List<CassandraQuery> queries, BatchType batchType) {
        BatchStatement batch = new BatchStatement(BatchStatement.Type.valueOf(batchType.name()));
        queries.forEach(query -> {
            switch (query.getQueryType()) {
                case QUERY:
                    batch.add(new SimpleStatement(query.getQuery(), query.getVariables()));
                    break;
                case PREPARED_STATEMENT:
                    batch.add(session.prepare(query.getQuery()).bind(query.getVariables()));
                    break;
            }
        });
        return execute(session::execute, batch);
    }

    @Override
    public UUID currentTimeUUID() {
        return UUIDs.timeBased();
    }

    @Override
    public void close() {
        cluster.close();
    }


    private <A> CassandraResult21 execute(Function<A, ResultSet> function, A input) {
        ResultSet resultSet;
        try {
            resultSet = function.apply(input);
        } catch (ReadTimeoutException e) {
            return new CassandraResult21(new CassandraResult.ReadTimeoutStatus(
                    e.getConsistencyLevel().toString(),
                    e.getReceivedAcknowledgements(),
                    e.getRequiredAcknowledgements(),
                    e.wasDataRetrieved()));
        } catch (WriteTimeoutException e) {
            return new CassandraResult21(
                    new CassandraResult.WriteTimeoutStatus(e.getConsistencyLevel().toString(),
                            e.getReceivedAcknowledgements(),
                            e.getRequiredAcknowledgements(),
                            WriteTypePrime.valueOf(e.getWriteType().toString())));
        } catch (UnavailableException e) {
            return new CassandraResult21(
                    new CassandraResult.UnavailableStatus(e.getConsistencyLevel().toString(),
                            e.getRequiredReplicas(),
                            e.getAliveReplicas()));
        } catch (NoHostAvailableException e) {
            Result error = server_error;
            String message = e.getMessage();
            InetSocketAddress addr = e.getErrors().keySet().iterator().next();
            Throwable e1 = e.getErrors().get(addr);
            try {
                throw e1;
            } catch (UnavailableException ue) {
                // NHAE is raised when first node is unavailable and there are no remaining hosts.
                return new CassandraResult21(
                    new CassandraResult.UnavailableStatus(
                        ue.getConsistencyLevel().toString(),
                        ue.getRequiredReplicas(),
                        ue.getAliveReplicas()));
            } catch(DriverException de) {
                message = de.getMessage();
                // These errors are thrown in NHAE as the driver considers them host-specific errors and
                // tries another host.
                if(message.contains("protocol error")) {
                    error = protocol_error;
                } else if(message.contains("overloaded")) {
                    error = overloaded;
                } else if(message.contains("bootstrapping")) {
                    error = is_bootstrapping;
                }
            } catch(Throwable t) {} // unknown error we can handle later.
            return new CassandraResult21(new CassandraResult.ErrorMessageStatus(error, message));
        } catch(DriverInternalError e) {
            Result error = protocol_error;
            String message = e.getMessage();
            // Unprepared is thrown as a DriverInternalError if the Driver doesn't know about the query either
            // as this is unexpected behavior.
            if(message.startsWith("Tried to execute unknown prepared query")) {
                error = unprepared;
            }
            return new CassandraResult21(new CassandraResult.ErrorMessageStatus(error, message));
        } catch(AuthenticationException e) {
            return new CassandraResult21(new CassandraResult.ErrorMessageStatus(bad_credentials, e.getMessage()));
        } catch(TruncateException e) {
            return new CassandraResult21(new CassandraResult.ErrorMessageStatus(truncate_error, e.getMessage()));
        } catch(SyntaxError e) {
            return new CassandraResult21(new CassandraResult.ErrorMessageStatus(syntax_error, e.getMessage()));
        } catch(UnauthorizedException e) {
            return new CassandraResult21(new CassandraResult.ErrorMessageStatus(unauthorized, e.getMessage()));
        } catch(InvalidConfigurationInQueryException e) {
            return new CassandraResult21(new CassandraResult.ErrorMessageStatus(config_error, e.getMessage()));
        } catch(InvalidQueryException e) {
            return new CassandraResult21(new CassandraResult.ErrorMessageStatus(invalid, e.getMessage()));
        } catch(AlreadyExistsException e) {
            return new CassandraResult21(new CassandraResult.ErrorMessageStatus(already_exists, e.getMessage()));
        }
        return new CassandraResult21(resultSet);
    }

    @Override
    public TupleType tupleType(DataType... dataTypes) {
        return TupleType.of(dataTypes);
    }
}
