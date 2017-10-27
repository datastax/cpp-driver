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

import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.scassandra.http.client.CurrentClient.CurrentClientBuilder;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.scassandra.http.client.ClosedConnectionReport.CloseType.*;

public class CurrentClientTest {

    private static final int PORT = 1236;
    private static final String connectionsUrl = "/current/connections";
    private static final String listenerUrl = "/current/listener";

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(PORT);

    private CurrentClient underTest;

    private CurrentClient lowTimeoutClient;

    @Before
    public void setup() {
        underTest = CurrentClient.builder().withHost("localhost").withPort(PORT).build();
        lowTimeoutClient = CurrentClient.builder().withHost("localhost").withPort(PORT).withSocketTimeout(1000).build();
    }

    @Test
    public void testRetrievalOfZeroConnections() {
        //given
        stubFor(get(urlEqualTo(connectionsUrl)).willReturn(aResponse().withBody("{\n"
            + "  \"connections\": []\n"
            + "}")));
        //when
        ConnectionReport report = underTest.getConnections();
        //then
        assertEquals(0, report.getConnections().size());
    }

    @Test
    public void testRetrievalOfAllConnections() {
        //given
        stubFor(get(urlEqualTo(connectionsUrl)).willReturn(aResponse().withBody("{\n"
            + "  \"connections\": [{\n"
            + "    \"host\": \"127.0.0.1\",\n"
            + "    \"port\": 53806\n"
            + "  }, {\n"
            + "    \"host\": \"127.0.0.1\",\n"
            + "    \"port\": 53807\n"
            + "  }, {\n"
            + "    \"host\": \"127.0.0.3\",\n"
            + "    \"port\": 53808\n"
            + "  }]\n"
            + "}")));
        //when
        ConnectionReport report = underTest.getConnections();
        List<InetSocketAddress> connections = report.getAddresses();
        //then
        assertEquals(3, connections.size());
        InetSocketAddress connection0 = connections.get(0);
        assertEquals("127.0.0.1", connection0.getAddress().getHostAddress());
        assertEquals(53806, connection0.getPort());
        InetSocketAddress connection1 = connections.get(1);
        assertEquals("127.0.0.1", connection1.getAddress().getHostAddress());
        assertEquals(53807, connection1.getPort());
        InetSocketAddress connection2 = connections.get(2);
        assertEquals("127.0.0.3", connection2.getAddress().getHostAddress());
        assertEquals(53808, connection2.getPort());
    }

    @Test
    public void testRetrievalOfConnectionsByHost() {
        //given
        stubFor(get(urlEqualTo(connectionsUrl + "/127.0.0.1")).willReturn(aResponse().withBody("{\n"
            + "  \"connections\": [{\n"
            + "    \"host\": \"127.0.0.1\",\n"
            + "    \"port\": 53806\n"
            + "  }, {\n"
            + "    \"host\": \"127.0.0.1\",\n"
            + "    \"port\": 53807\n"
            + "  }]\n"
            + "}")));
        //when
        ConnectionReport report = underTest.getConnections("127.0.0.1");
        List<InetSocketAddress> connections = report.getAddresses();
        //then
        assertEquals(2, connections.size());
        InetSocketAddress connection0 = connections.get(0);
        assertEquals("127.0.0.1", connection0.getAddress().getHostAddress());
        assertEquals(53806, connection0.getPort());
        InetSocketAddress connection1 = connections.get(1);
        assertEquals("127.0.0.1", connection1.getAddress().getHostAddress());
        assertEquals(53807, connection1.getPort());
    }

    @Test
    public void testRetrievalOfConnectionsByAddress() {
        //given
        stubFor(get(urlEqualTo(connectionsUrl + "/127.0.0.1")).willReturn(aResponse().withBody("{\n"
            + "  \"connections\": [{\n"
            + "    \"host\": \"127.0.0.1\",\n"
            + "    \"port\": 53806\n"
            + "  }, {\n"
            + "    \"host\": \"127.0.0.1\",\n"
            + "    \"port\": 53807\n"
            + "  }]\n"
            + "}")));
        //when
        ConnectionReport report = underTest.getConnections(new InetSocketAddress("127.0.0.1", 1000).getAddress());
        List<InetSocketAddress> connections = report.getAddresses();
        //then
        assertEquals(2, connections.size());
        InetSocketAddress connection0 = connections.get(0);
        assertEquals("127.0.0.1", connection0.getAddress().getHostAddress());
        assertEquals(53806, connection0.getPort());
        InetSocketAddress connection1 = connections.get(1);
        assertEquals("127.0.0.1", connection1.getAddress().getHostAddress());
        assertEquals(53807, connection1.getPort());
    }

    @Test
    public void testRetrievalOfConnectionByHostPort() {
        //given
        stubFor(get(urlEqualTo(connectionsUrl + "/127.0.0.1/53806")).willReturn(aResponse().withBody("{\n"
            + "  \"connections\": [{\n"
            + "    \"host\": \"127.0.0.1\",\n"
            + "    \"port\": 53806\n"
            + "  }]\n"
            + "}")));
        ConnectionReport report = underTest.getConnection("127.0.0.1", 53806);
        List<ClientConnection> connections = report.getConnections();
        //then
        assertEquals(1, connections.size());
        ClientConnection connection = connections.get(0);
        assertEquals("127.0.0.1", connection.getHost());
        assertEquals(53806, connection.getPort());
    }

    @Test
    public void testRetrievalOfConnectionByHostPortEmptyResult() {
        //given
        stubFor(get(urlEqualTo(connectionsUrl + "/127.0.0.1/53806")).willReturn(aResponse().withBody("{connections: []}")));
        //when
        ConnectionReport report = underTest.getConnection("127.0.0.1", 53806);
        //then
        assertEquals(0, report.getConnections().size());
    }

    @Test
    public void testRetrievalOfConnectionBySocketAddress() {
        //given
        stubFor(get(urlEqualTo(connectionsUrl + "/127.0.0.1/53807")).willReturn(aResponse().withBody("{\n"
            + "  \"connections\": [{\n"
            + "    \"host\": \"127.0.0.1\",\n"
            + "    \"port\": 53807\n"
            + "  }]\n"
            + "}")));
        //when
        ConnectionReport report = underTest.getConnection(new InetSocketAddress("127.0.0.1", 53807));
        List<InetSocketAddress> connections = report.getAddresses();
        //then
        assertEquals(1, connections.size());
        InetSocketAddress address = connections.get(0);
        assertEquals("127.0.0.1", address.getAddress().getHostAddress());
        assertEquals(53807, address.getPort());
    }

    @Test(expected = ConnectionsRequestFailed.class)
    public void testErrorDuringConnectionRetrieval() {
        //given
        stubFor(get(urlEqualTo(connectionsUrl)).willReturn(aResponse().withFault(Fault.RANDOM_DATA_THEN_CLOSE)));
        //when
        underTest.getConnections();
    }

    @Test(expected = ConnectionsRequestFailed.class, timeout = 2500)
    public void testServerHanging() {
        //given
        stubFor(get(urlEqualTo(connectionsUrl)).willReturn(aResponse().withFixedDelay(5000)));
        //when
        lowTimeoutClient.getConnections();
    }

    @Test
    public void testCloseOfZeroConnections() {
        //given
        stubFor(delete(urlEqualTo(connectionsUrl + "?type=close")).willReturn(aResponse().withBody("{\n"
            + "  \"closed_connections\": [],\n"
            + "  \"operation\": \"close\"\n"
            + "}")));
        //when
        ClosedConnectionReport report = underTest.closeConnections(CLOSE);
        //then
        assertEquals(CLOSE, report.getCloseType());
        assertEquals(newArrayList(), report.getConnections());
    }

    @Test
    public void testCloseOfAllConnections() {
        //given
        stubFor(delete(urlEqualTo(connectionsUrl + "?type=close")).willReturn(aResponse().withBody("{\n"
                + "  \"closed_connections\": [{\n"
                + "    \"host\": \"127.0.0.1\",\n"
                + "    \"port\": 57518\n"
                + "  }, {\n"
                + "    \"host\": \"127.0.0.1\",\n"
                + "    \"port\": 57519\n"
                + "  }, {\n"
                + "    \"host\": \"127.0.0.3\",\n"
                + "    \"port\": 57520\n"
                + "  }],\n"
                + "  \"operation\": \"close\"\n"
                + "}")));
        //when
        ClosedConnectionReport report = underTest.closeConnections(CLOSE);
        //then
        assertEquals(CLOSE, report.getCloseType());
        assertEquals(newArrayList(new InetSocketAddress("127.0.0.1", 57518), new InetSocketAddress("127.0.0.1", 57519),
                new InetSocketAddress("127.0.0.3", 57520)), report.getAddresses());
    }

    @Test
    public void testResetOfAllConnections() {
        //given
        stubFor(delete(urlEqualTo(connectionsUrl + "?type=reset")).willReturn(aResponse().withBody("{\n"
            + "  \"closed_connections\": [{\n"
            + "    \"host\": \"127.0.0.1\",\n"
            + "    \"port\": 57518\n"
            + "  }, {\n"
            + "    \"host\": \"127.0.0.1\",\n"
            + "    \"port\": 57519\n"
            + "  }, {\n"
            + "    \"host\": \"127.0.0.3\",\n"
            + "    \"port\": 57520\n"
            + "  }],\n"
            + "  \"operation\": \"reset\"\n"
            + "}")));
        //when
        ClosedConnectionReport report = underTest.closeConnections(RESET);
        //then
        assertEquals(RESET, report.getCloseType());
        assertEquals(newArrayList(new InetSocketAddress("127.0.0.1", 57518), new InetSocketAddress("127.0.0.1", 57519),
            new InetSocketAddress("127.0.0.3", 57520)), report.getAddresses());
    }

    @Test
    public void testHalfCloseOfAllConnections() {
        //given
        stubFor(delete(urlEqualTo(connectionsUrl + "?type=halfclose")).willReturn(aResponse().withBody("{\n"
            + "  \"closed_connections\": [{\n"
            + "    \"host\": \"127.0.0.1\",\n"
            + "    \"port\": 57518\n"
            + "  }, {\n"
            + "    \"host\": \"127.0.0.1\",\n"
            + "    \"port\": 57519\n"
            + "  }, {\n"
            + "    \"host\": \"127.0.0.3\",\n"
            + "    \"port\": 57520\n"
            + "  }],\n"
            + "  \"operation\": \"halfclose\"\n"
            + "}")));
        //when
        ClosedConnectionReport report = underTest.closeConnections(HALFCLOSE);
        //then
        assertEquals(HALFCLOSE, report.getCloseType());
        assertEquals(newArrayList(new InetSocketAddress("127.0.0.1", 57518), new InetSocketAddress("127.0.0.1", 57519),
                new InetSocketAddress("127.0.0.3", 57520)), report.getAddresses());
    }
    @Test
    public void testCloseOfConnectionsByHost() {
        //given
        stubFor(delete(urlEqualTo(connectionsUrl + "/127.0.0.1?type=close")).willReturn(aResponse().withBody("{\n"
            + "  \"closed_connections\": [{\n"
            + "    \"host\": \"127.0.0.1\",\n"
            + "    \"port\": 57518\n"
            + "  }, {\n"
            + "    \"host\": \"127.0.0.1\",\n"
            + "    \"port\": 57519\n"
            + "  }],\n"
            + "  \"operation\": \"close\"\n"
            + "}")));
        //when
        ClosedConnectionReport report = underTest.closeConnections(CLOSE, "127.0.0.1");
        //then
        assertEquals(CLOSE, report.getCloseType());
        assertEquals(newArrayList(new InetSocketAddress("127.0.0.1", 57518), new InetSocketAddress("127.0.0.1", 57519)),
            report.getAddresses());
    }

    @Test
    public void testCloseOfConnectionsByAddress() {
        //given
        stubFor(delete(urlEqualTo(connectionsUrl + "/127.0.0.1?type=close")).willReturn(aResponse().withBody("{\n"
            + "  \"closed_connections\": [{\n"
            + "    \"host\": \"127.0.0.1\",\n"
            + "    \"port\": 57518\n"
            + "  }, {\n"
            + "    \"host\": \"127.0.0.1\",\n"
            + "    \"port\": 57519\n"
            + "  }],\n"
            + "  \"operation\": \"close\"\n"
            + "}")));
        //when
        ClosedConnectionReport report = underTest.closeConnections(CLOSE,
                new InetSocketAddress("127.0.0.1", 1000).getAddress());
        //then
        assertEquals(CLOSE, report.getCloseType());
        assertEquals(newArrayList(new InetSocketAddress("127.0.0.1", 57518), new InetSocketAddress("127.0.0.1", 57519)),
            report.getAddresses());
    }

    @Test
    public void testCloseOfConnectionsByHostPort() {
        //given
        stubFor(delete(urlEqualTo(connectionsUrl + "/127.0.0.1/57518?type=close")).willReturn(aResponse().withBody("{\n"
            + "  \"closed_connections\": [{\n"
            + "    \"host\": \"127.0.0.1\",\n"
            + "    \"port\": 57518\n"
            + "  }],\n"
            + "  \"operation\": \"close\"\n"
            + "}")));
        //when
        ClosedConnectionReport report = underTest.closeConnection(CLOSE, "127.0.0.1", 57518);
        //then
        assertEquals(CLOSE, report.getCloseType());
        assertEquals(newArrayList(new InetSocketAddress("127.0.0.1", 57518)), report.getAddresses());
    }

    @Test
    public void testCloseOfConnectionsBySocketAddress() {
        //given
        stubFor(delete(urlEqualTo(connectionsUrl + "/127.0.0.1/57518?type=close")).willReturn(aResponse().withBody("{\n"
            + "  \"closed_connections\": [{\n"
            + "    \"host\": \"127.0.0.1\",\n"
            + "    \"port\": 57518\n"
            + "  }],\n"
            + "  \"operation\": \"close\"\n"
            + "}")));
        //when
        ClosedConnectionReport report = underTest.closeConnection(CLOSE, new InetSocketAddress("127.0.0.1", 57518));
        //then
        assertEquals(CLOSE, report.getCloseType());
        assertEquals(newArrayList(new InetSocketAddress("127.0.0.1", 57518)), report.getAddresses());
    }

    @Test(expected = ConnectionsRequestFailed.class, timeout = 2500)
    public void testServerHangingWhileCloseOfConnections() {
        stubFor(delete(urlEqualTo(connectionsUrl + "?type=close")).willReturn(aResponse().withFixedDelay(5000)));
        //when
        lowTimeoutClient.closeConnections(CLOSE);
        //then
    }

    @Test(expected = ConnectionsRequestFailed.class)
    public void testErrorDuringCloseOfConnections() {
        stubFor(delete(urlEqualTo(connectionsUrl + "?type=close")).willReturn(aResponse().withFault(Fault.RANDOM_DATA_THEN_CLOSE)));
        //when
        underTest.closeConnections(CLOSE);
        //then
    }

    @Test
    public void testEnableListener() {
        //given
        stubFor(put(urlEqualTo(listenerUrl)).willReturn(aResponse().withBody("{\n"
            + "  \"changed\": true\n"
            + "}")));
        //when
        boolean enabled = underTest.enableListener();
        //then
        assertEquals(true, enabled);
    }

    @Test
    public void testEnableListenerAlreadyEnabled() {
        //given
        stubFor(put(urlEqualTo(listenerUrl)).willReturn(aResponse().withBody("{\n"
            + "  \"changed\": false\n"
            + "}")));
        //when
        boolean enabled = underTest.enableListener();
        //then
        assertEquals(false, enabled);
    }

    @Test
    public void testDisableListener() {
        //given
        stubFor(delete(urlEqualTo(listenerUrl + "?after=0")).willReturn(aResponse().withBody("{\n"
            + "  \"changed\": true\n"
            + "}")));
        //when
        boolean disabled = underTest.disableListener();
        //then
        assertEquals(true, disabled);
    }

    @Test
    public void testDisableListenerAlreadyDisabled() {
        //given
        stubFor(delete(urlEqualTo(listenerUrl + "?after=0")).willReturn(aResponse().withBody("{\n"
            + "  \"changed\": false\n"
            + "}")));
        //when
        boolean enabled = underTest.disableListener();
        //then
        assertEquals(false, enabled);
    }

    @Test(expected = ListenerRequestFailed.class, timeout = 2500)
    public void testListenerServerHanging() {
        //given
        stubFor(put(urlEqualTo(listenerUrl)).willReturn(aResponse().withFixedDelay(5000)));
        //when
        lowTimeoutClient.enableListener();
    }

    @Test(expected = ListenerRequestFailed.class)
    public void testErrorDuringEnable() {
        //given
        stubFor(put(urlEqualTo(listenerUrl)).willReturn(aResponse().withFault(Fault.RANDOM_DATA_THEN_CLOSE)));
        //when
        underTest.enableListener();
    }
}
