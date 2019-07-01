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

#ifndef __TEST_CUSTOM_PAYLOAD_HPP__
#define __TEST_CUSTOM_PAYLOAD_HPP__
#include "cassandra.h"

#include "objects/future.hpp"
#include "objects/object_base.hpp"
#include "values.hpp"

#include <gtest/gtest.h>

#include <iterator>
#include <map>
#include <string>
#include <utility>

namespace test { namespace driver {

/**
 * Custom payload object
 */
class CustomPayload : public Object<CassCustomPayload, cass_custom_payload_free> {
public:
  class Exception : public test::Exception {
  public:
    Exception(const std::string& message)
        : test::Exception(message) {}
  };

  /**
   * Create an empty custom payload object
   */
  CustomPayload()
      : Object<CassCustomPayload, cass_custom_payload_free>(cass_custom_payload_new()) {}

  /**
   * Create the custom payload from a response future
   *
   * @param column Column to retrieve custom payload from
   */
  CustomPayload(Future future)
      : Object<CassCustomPayload, cass_custom_payload_free>(cass_custom_payload_new()) {
    initialize(future);
  }

  /**
   * Set the value in the custom payload
   *
   * @param name Name for the custom payload
   * @param value Blob/Bytes to set to the custom payload
   */
  void set(const std::string& name, Blob value) {
    cass_custom_payload_set(get(), name.c_str(), value.wrapped_value().data(),
                            value.wrapped_value().size());

    // Update the local items map and the count
    items_[name] = value;
  }

  /**
   * Get the item count of the custom payload
   *
   * @return The number of items in the custom payload
   */
  size_t item_count() { return items_.size(); }

  /**
   * Get the item from the custom payload at the specified index
   *
   * @param index Index to retrieve value from in the custom payload
   * @return Pair; key name and Blob/Byte value at the specified index
   * @throws Exception If index is out of bounds
   */
  std::pair<const std::string, Blob> item(size_t index) {
    if (index < items_.size()) {
      std::map<std::string, Blob>::iterator it = items_.begin();
      std::advance(it, index);
      return *it;
    }
    throw Exception("Invalid Custom Payload: Empty");
  }

  /**
   * Gets all the items as a map
   *
   * @return The custom payload as a map of key/value pairs
   */
  std::map<std::string, Blob> items() { return items_; }

protected:
  /**
   * Custom payload items
   */
  std::map<std::string, Blob> items_;

  /**
   * Initialize the iterator from the CassValue
   *
   * @param future Future to initialize the custom payload from
   */
  void initialize(Future future) {
    // Determine if the future has a custom payload
    size_t item_count = cass_future_custom_payload_item_count(future.get());

    // Retrieve the items from the custom payload (may be none)
    for (size_t i = 0; i < item_count; ++i) {
      // Get the item from the custom payload
      const char* name;
      size_t name_length;
      const cass_byte_t* value;
      size_t value_size;
      ASSERT_EQ(CASS_OK, cass_future_custom_payload_item(future.get(), i, &name, &name_length,
                                                         &value, &value_size));

      // Add the item to the map of items for the custom payload
      items_[std::string(name, name_length)] =
          Blob(std::string(reinterpret_cast<const char*>(value), value_size));
    }
  }
};

}} // namespace test::driver

#endif // __TEST_CUSTOM_PAYLOAD_HPP__
