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

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.scassandra.http.client.ClosedConnectionReport.CloseType;

public class CurrentClient {

    private static final String REQUEST_FOR_CONNECTIONS_FAILED = "Request for connections failed";

    private static final String REQUEST_FAILED = "Request failed";

    private final Type map = new TypeToken<Map<String, Boolean>>() { }.getType();

    public static class CurrentClientBuilder {

        private String host = "localhost";

        private int port = 8043;
        private int socketTimeout = 5000;

        public CurrentClientBuilder withHost(String host) {
            this.host = host;
            return this;
        }

        public CurrentClientBuilder withPort(int port) {
            this.port = port;
            return this;
        }

        public CurrentClientBuilder withSocketTimeout(int timeout) {
            this.socketTimeout = timeout;
            return this;
        }

        public CurrentClient build() {
            return new CurrentClient(this.host, this.port, this.socketTimeout);
        }
    }

    public static CurrentClientBuilder builder() {
        return new CurrentClientBuilder();
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(CurrentClient.class);

    private final CloseableHttpClient httpClient;

    private final String currentUrl;

    private final String connectionsUrl;

    private final String listenerUrl;

    private final Gson gson = new GsonBuilder().create();

    private CurrentClient(String host, int port, int socketTimeout) {
        RequestConfig.Builder requestBuilder = RequestConfig.custom();
        requestBuilder = requestBuilder.setConnectTimeout(socketTimeout);
        requestBuilder = requestBuilder.setConnectionRequestTimeout(socketTimeout);
        requestBuilder = requestBuilder.setSocketTimeout(socketTimeout);
        HttpClientBuilder builder = HttpClientBuilder.create();
        builder.setDefaultRequestConfig(requestBuilder.build());
        httpClient = builder.build();
        this.currentUrl = "http://" + host + ":" + port + "/current";
        this.connectionsUrl = this.currentUrl + "/connections";
        this.listenerUrl = this.currentUrl + "/listener";
    }

    public ConnectionReport getConnections() {
        return getConnectionsByUrl("");
    }

    public ConnectionReport getConnections(String ip) {
        return getConnectionsByUrl("/" + ip);
    }

    public ConnectionReport getConnections(InetAddress address) {
        return getConnectionsByUrl("/" + address.getHostAddress());
    }

    public ConnectionReport getConnection(String host, int port) {
        return getConnectionsByUrl("/" + host + "/" + port);
    }

    public ConnectionReport getConnection(InetSocketAddress address) {
        return getConnection(address.getAddress().getHostAddress(), address.getPort());
    }

    private ConnectionReport getConnectionsByUrl(String url) {
        HttpGet get = new HttpGet(connectionsUrl + url);
        try {
            CloseableHttpResponse response = httpClient.execute(get);
            String body = EntityUtils.toString(response.getEntity());
            LOGGER.debug("Received response {}", body);
            return gson.fromJson(body, CurrentConnectionReport.class);
        } catch(IOException e) {
            LOGGER.info(REQUEST_FOR_CONNECTIONS_FAILED, e);
            throw new ConnectionsRequestFailed(REQUEST_FOR_CONNECTIONS_FAILED, e);
        }
    }

    public ClosedConnectionReport closeConnections(CloseType closeType) {
        return closeConnectionsByUrl(closeType, "");
    }

    public ClosedConnectionReport closeConnections(CloseType closeType, String ip) {
        return closeConnectionsByUrl(closeType, "/" + ip);
    }

    public ClosedConnectionReport closeConnections(CloseType closeType, InetAddress address) {
        return closeConnectionsByUrl(closeType, "/" + address.getHostAddress());
    }

    public ClosedConnectionReport closeConnection(CloseType closeType, String host, int port) {
        return closeConnectionsByUrl(closeType, "/" + host + "/" + port);
    }

    public ClosedConnectionReport closeConnection(CloseType closeType, InetSocketAddress address) {
        return closeConnectionsByUrl(closeType, "/" + address.getAddress().getHostAddress() + "/" + address.getPort());
    }

    private ClosedConnectionReport closeConnectionsByUrl(CloseType closeType, String url) {
        URI httpUrl;
        try {
            httpUrl = new URIBuilder(connectionsUrl + url).setParameter("type", closeType.name().toLowerCase()).build();
        } catch(URISyntaxException e) {
            // Shouldn't happen but we handle anyways.
            throw new ConnectionsRequestFailed(REQUEST_FAILED, e);
        }
        HttpDelete delete = new HttpDelete(httpUrl);
        try {
            CloseableHttpResponse response = httpClient.execute(delete);
            String body = EntityUtils.toString(response.getEntity());
            LOGGER.debug("Received response {}", body);
            return gson.fromJson(body, ClosedConnectionReport.class);
        } catch (IOException e) {
            LOGGER.warn(REQUEST_FAILED, e);
            throw new ConnectionsRequestFailed(REQUEST_FAILED, e);
        }
    }

    public boolean enableListener() {
        HttpPut put = new HttpPut(this.listenerUrl);
        return invokeListener(put);
    }

    public boolean disableListener() {
        return disableListener(0);
    }

    public boolean disableListener(int after) {
        URI httpUrl;
        try {
            httpUrl = new URIBuilder(this.listenerUrl).setParameter("after", Integer.toString(after)).build();
        } catch(URISyntaxException e) {
            // Shouldn't happen but we handle anyways.
            throw new ListenerRequestFailed(REQUEST_FAILED, e);
        }
        HttpDelete delete = new HttpDelete(httpUrl);
        return invokeListener(delete);
    }

    private boolean invokeListener(HttpUriRequest request) {
        try {
            CloseableHttpResponse response = httpClient.execute(request);
            String body = EntityUtils.toString(response.getEntity());
            LOGGER.debug("Received response {}", body);
            Map<String, Boolean> data = gson.fromJson(body, map);
            Boolean changed = data.get("changed");
            return changed == null ? false : changed;
        } catch (IOException e) {
            LOGGER.info(REQUEST_FAILED, e);
            throw new ListenerRequestFailed(REQUEST_FAILED, e);
        }
    }
}
