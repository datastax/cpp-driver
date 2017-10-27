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

import java.io.IOException;
import java.net.ServerSocket;

public class PortLocator {

    public static int findFreePort() {
        ServerSocket socket = null;
        try {
            // let the system pick an ephemeral port.
            socket = new ServerSocket(0);
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            // throw as RuntimeException, this would be completely
            // unexpected and we don't expect callers to have a
            // strategy to handle this.
            throw new RuntimeException(e);
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
