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

public enum Result {
    success,
    read_request_timeout,
    unavailable,
    write_request_timeout,
    server_error,
    protocol_error,
    bad_credentials,
    overloaded,
    is_bootstrapping,
    truncate_error,
    syntax_error,
    unauthorized,
    invalid,
    config_error,
    already_exists,
    unprepared,
    closed_connection,
    read_failure,
    write_failure,
    function_failure
}
