/*
  Copyright (c) 2014-2016 DataStax

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

#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "async_queue.hpp"
#include "loop_thread.hpp"
#include "mpmc_queue.hpp"
#include "spsc_queue.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/atomic.hpp>

#include <stdio.h>

const int NUM_ITERATIONS = 1000000;
const int NUM_ENQUEUE_THREADS = 2;

template <class Queue>
struct TestAsyncQueue : public cass::LoopThread {
  TestAsyncQueue(size_t queue_size)
    : value_(0)
    , async_queue_(queue_size) {
    BOOST_REQUIRE(init() == 0);
    BOOST_REQUIRE(async_queue_.init(loop(), this, TestAsyncQueue::async_func) == 0);
  }

  void close_and_join() {
    while(!async_queue_.enqueue(-1)) {
      // Keep trying
    }
    join();
  }

#if UV_VERSION_MAJOR == 0
  static void async_func(uv_async_t *handle, int status) {
#else
  static void async_func(uv_async_t *handle) {
#endif
    TestAsyncQueue* test_queue = static_cast<TestAsyncQueue*>(handle->data);
    int n;
    while (test_queue->async_queue_.dequeue(n)) {
      if (n < 0) {
        test_queue->close_handles();
        test_queue->async_queue_.close_handles();
        break;
      } else {
        test_queue->value_++;
      }
    }
  }

  boost::atomic<int> value_;
  cass::AsyncQueue<Queue> async_queue_;
};

void enqueue_thread(void* data) {
  cass::AsyncQueue<cass::MPMCQueue<int> >* queue
      = static_cast<cass::AsyncQueue<cass::MPMCQueue<int> >*>(data);
  for (int i = 0; i < NUM_ITERATIONS / NUM_ENQUEUE_THREADS; ++i) {
    if (!queue->enqueue(i)) {
      fprintf(stderr, "Failed to enqueue entry\n");
    }
  }
}

template <class Queue>
void queue_simple() {
  Queue queue(17);

  for (int i = 0; i < 16; ++i) {
    BOOST_CHECK(queue.enqueue(i));
  }

  for (int i = 0; i < 16; ++i) {
    int r;
    BOOST_CHECK(queue.dequeue(r) && r == i);
  }
}

BOOST_AUTO_TEST_SUITE(async_queue)

BOOST_AUTO_TEST_CASE(simple)
{
  queue_simple<cass::SPSCQueue<int> >();
  queue_simple<cass::MPMCQueue<int> >();
}

BOOST_AUTO_TEST_CASE(bounds)
{
  {
    cass::SPSCQueue<int> queue(1);

    BOOST_CHECK(queue.enqueue(0));
    BOOST_CHECK(queue.enqueue(1) == false);

    int r;
    BOOST_CHECK(queue.dequeue(r) && r == 0);
    BOOST_CHECK(queue.dequeue(r) == false);
  }

  {
    cass::MPMCQueue<int> queue(2);

    BOOST_CHECK(queue.enqueue(0));
    BOOST_CHECK(queue.enqueue(1));
    BOOST_CHECK(queue.enqueue(2) == false);

    int r;
    BOOST_CHECK(queue.dequeue(r) && r == 0);
    BOOST_CHECK(queue.dequeue(r) && r == 1);
    BOOST_CHECK(queue.dequeue(r) == false);
  }
}

BOOST_AUTO_TEST_CASE(spsc_async)
{
  TestAsyncQueue<cass::SPSCQueue<int> > test_queue(NUM_ITERATIONS);

  test_queue.run();

  for (int i = 0; i < NUM_ITERATIONS; ++i) {
    BOOST_CHECK(test_queue.async_queue_.enqueue(i));
  }

  test_queue.close_and_join();

  BOOST_CHECK_EQUAL(test_queue.value_.load(), NUM_ITERATIONS);
}

BOOST_AUTO_TEST_CASE(mpmc_async)
{
  uv_thread_t threads[NUM_ENQUEUE_THREADS];
  TestAsyncQueue<cass::MPMCQueue<int> > test_queue(NUM_ITERATIONS);

  test_queue.run();

  for (int i = 0; i < NUM_ENQUEUE_THREADS; ++i) {
    uv_thread_create(&threads[i], enqueue_thread, &(test_queue.async_queue_));
  }

  for (int i = 0; i < NUM_ENQUEUE_THREADS; ++i) {
    uv_thread_join(&threads[i]);
  }

  test_queue.close_and_join();

  BOOST_CHECK_EQUAL(test_queue.value_.load(), NUM_ITERATIONS);
}

BOOST_AUTO_TEST_SUITE_END()
