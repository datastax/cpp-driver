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

#include "test_utils.hpp"
#include "unit.hpp"

#include <uv.h>

// Default number of workers is 4 for libuv:
// http://docs.libuv.org/en/v1.x/threadpool.html
#define NUM_WORKERS 4

class LoopTest : public Unit {
public:
  uv_loop_t* loop() { return &loop_; }

  virtual void SetUp() {
    Unit::SetUp();
    uv_loop_init(loop());
  }

  virtual void TearDown() {
    Unit::TearDown();
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

  int run_loop(uv_run_mode mode = UV_RUN_DEFAULT) { return uv_run(loop(), mode); }

  /**
   * Prevent the uv_work thread pool from completing any useful work for the
   * provided sleep time.
   *
   * @param sleep_ms The duration in milliseconds to starve the thread pool.
   */
  void starve_thread_pool(unsigned int sleep_ms) {
    for (int i = 0; i < NUM_WORKERS; ++i) {
      workers[i].data = new unsigned int(sleep_ms);
      uv_queue_work(loop(), &workers[i], on_work, NULL);
    }
  }

private:
  static void on_work(uv_work_t* req) {
    unsigned int* sleep_ms = static_cast<unsigned int*>(req->data);
    test::Utils::msleep(*sleep_ms);
    delete sleep_ms;
  }

private:
  uv_loop_t loop_;
  uv_work_t workers[NUM_WORKERS];
};

#endif // LOOP_TEST_HPP
