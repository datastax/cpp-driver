/*
  Copyright (c) DataStax, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#ifndef __RESULT_CLOSE_CONNECTION_HPP__
#define __RESULT_CLOSE_CONNECTION_HPP__

#include "result.hpp"

#include "exception.hpp"

#include <sstream>

/**
 * Enumeration for 'close_type' property
 */
enum CloseType { CLOSE_TYPE_DISCONNECT, CLOSE_TYPE_SHUTDOWN_READ, CLOSE_TYPE_SHUTDOWN_WRITE };

/**
 * Enumeration for disconnect 'scope' property
 *
 * //TODO(fero): Should move into its own definition when more results are added
 *               that will require this enumeration type.
 */
enum DisconnectScope {
  DISCONNECT_SCOPE_CONNECTION,
  DISCONNECT_SCOPE_NODE,
  DISCONNECT_SCOPE_DATA_CENTER,
  DISCONNECT_SCOPE_CLUSTER
};

namespace prime {

/**
 * Priming result 'close_connection'
 */
class CloseConnection : public Result {
public:
  class Exception : public test::Exception {
  public:
    Exception(const std::string& message)
        : test::Exception(message) {}
  };

  CloseConnection()
      : Result("close_connection")
      , close_type_(CLOSE_TYPE_DISCONNECT)
      , scope_(DISCONNECT_SCOPE_CONNECTION) {}

  /**
   * Fully construct the 'close_connection' result
   *
   * @param delay_in_ms Delay in milliseconds before forwarding result
   * @param close_type The way to close the connection(s)
   * @param scope The scope (connection, node, data center, cluster) at which to
   *              close the associated connection(s)
   */
  CloseConnection(unsigned long delay_in_ms, CloseType close_type, DisconnectScope scope)
      : Result("close_connection", delay_in_ms)
      , close_type_(close_type)
      , scope_(scope) {}

  /**
   * Set a fixed delay to the response time of a result
   *
   * @param delay_in_ms Delay in milliseconds before responding with the result
   * @return CloseConnection instance
   */
  CloseConnection& with_delay_in_ms(unsigned long delay_in_ms) {
    delay_in_ms_ = delay_in_ms;
    return *this;
  }

  /**
   * Set the way to close the connection(s) during the request
   *
   * @param close_type The way to close the connection(s)
   * @return CloseConnection instance
   */
  CloseConnection& with_close_type(CloseType close_type) {
    close_type_ = close_type;
    return *this;
  }

  /**
   * Set the scope (connection, node, data center, cluster) at which to close
   * the associated connection(s).
   *
   * @param scope The scope (connection, node, data center, cluster) at which to
   *              close the associated connection(s)
   * @return CloseConnection instance
   */
  CloseConnection& with_scope(DisconnectScope scope) {
    scope_ = scope;
    return *this;
  }

  /**
   * Generate the JSON for the 'close_connection` result
   *
   * @return  writer JSON writer to add the 'close_connection' result properties to
   */
  virtual void build(rapidjson::PrettyWriter<rapidjson::StringBuffer>& writer) const {
    Result::build(writer);

    writer.Key("scope");
    writer.String(get_disconnect_scope(scope_).c_str());
    writer.Key("close_type");
    writer.String(get_close_type(close_type_).c_str());
  }

private:
  /**
   * Get the JSON property value for the close type
   *
   * @param type Close type
   * @return String representation of the close type property
   */
  std::string get_close_type(CloseType type) const {
    switch (type) {
      case CLOSE_TYPE_DISCONNECT:
        return "disconnect";
      case CLOSE_TYPE_SHUTDOWN_READ:
        return "shutdown_read";
      case CLOSE_TYPE_SHUTDOWN_WRITE:
        return "shutdown_write";
      default:
        std::stringstream message;
        message << "Unsupported Close Type: " << type << " will need to be added";
        throw Exception(message.str());
        break;
    }
  }

  /**
   * Get the JSON property value for the disconnect scope
   *
   * @param disconnect_scope Disconnect scope
   * @return String representation of the disconnect scope property
   */
  std::string get_disconnect_scope(DisconnectScope disconnect_scope) const {
    switch (disconnect_scope) {
      case DISCONNECT_SCOPE_CONNECTION:
        return "connection";
      case DISCONNECT_SCOPE_NODE:
        return "node";
      case DISCONNECT_SCOPE_DATA_CENTER:
        return "data_center";
      case DISCONNECT_SCOPE_CLUSTER:
        return "cluster";
      default:
        std::stringstream message;
        message << "Unsupported Disconnect Scope: " << disconnect_scope << " will need to be added";
        throw Exception(message.str());
        break;
    }
  }

private:
  /**
   * The way to close the connection(s)
   */
  CloseType close_type_;
  /**
   * The scope (connection, node, data center, cluster) at which to close the
   * associated connection(s)
   */
  DisconnectScope scope_;
};

} // namespace prime

#endif //__RESULT_CLOSE_CONNECTION_HPP__
