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

#ifndef LOOP_TEST_HPP
#define LOOP_TEST_HPP

#include "unit.hpp"
#include <uv.h>

class LoopTest : public Unit {
public:
  uv_loop_t* loop() { return &loop_; }

  virtual void SetUp() {
    uv_loop_init(loop());
  }

  virtual void TearDown() {
    int rc = uv_loop_close(loop());
    if (rc != 0) {
      uv_run(loop(), UV_RUN_DEFAULT);
      rc = uv_loop_close(loop());
      if (rc != 0) {
        uv_print_all_handles(loop(), stderr);
        assert(false && "Test event loop still has pending handles");
      }
    }
  }

private:
  uv_loop_t loop_;
};

#endif // LOOP_TEST_HPP
