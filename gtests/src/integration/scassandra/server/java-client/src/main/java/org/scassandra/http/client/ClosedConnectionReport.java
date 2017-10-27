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
package org.scassandra.http.client;

import java.net.InetSocketAddress;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

public class ClosedConnectionReport implements ConnectionReport {

    public enum CloseType {
        @SerializedName("close")
        CLOSE,
        @SerializedName("reset")
        RESET,
        @SerializedName("halfclose")
        HALFCLOSE;
    }

    @SerializedName("operation")
    private final CloseType closeType;

    @SerializedName("closed_connections")
    private final List<ClientConnection> connections;

    public ClosedConnectionReport(CloseType closeType, List<ClientConnection> connections) {
        this.closeType = closeType;
        this.connections = connections;
    }

    public CloseType getCloseType() {
        return closeType;
    }

    public List<ClientConnection> getConnections() {
        return connections;
    }

    @Override
    public List<InetSocketAddress> getAddresses() {
        return Lists.transform(this.connections, new Function<ClientConnection, InetSocketAddress>() {
            @Override
            public InetSocketAddress apply(ClientConnection input) {
                return input.getAddress();
            }
        });
    }
}
