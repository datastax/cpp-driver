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

#include <gtest/gtest.h>

#include "async_queue.hpp"
#include "loop_thread.hpp"
#include "mpmc_queue.hpp"
#include "spsc_queue.hpp"
#include "atomic.hpp"

#include <stdio.h>

const int NUM_ITERATIONS = 1000000;
const int NUM_ENQUEUE_THREADS = 2;

template <class Queue>
struct TestAsyncQueue : public cass::LoopThread {
  TestAsyncQueue(size_t queue_size)
    : value_(0)
    , async_queue_(queue_size) {
  }

  void init() {
    ASSERT_EQ(0, cass::LoopThread::init());
    ASSERT_EQ(0, async_queue_.init(loop(), this, TestAsyncQueue::async_func));
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
        test_queue->value_.fetch_add(1);
      }
    }
  }

  cass::Atomic<int> value_;
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
    EXPECT_TRUE(queue.enqueue(i));
  }

  for (int i = 0; i < 16; ++i) {
    int r;
    EXPECT_TRUE(queue.dequeue(r));
    EXPECT_EQ(r, i);
  }
}

TEST(AsyncQueueUnitTest, Simple) {
  queue_simple<cass::SPSCQueue<int> >();
  queue_simple<cass::MPMCQueue<int> >();
}

TEST(AsyncQueueUnitTest, CheckBoundsSingleProducerQueue) {
  cass::SPSCQueue<int> queue(1);

  EXPECT_TRUE(queue.enqueue(0));
  EXPECT_FALSE(queue.enqueue(1));

  int r;
  EXPECT_TRUE(queue.dequeue(r));
  EXPECT_EQ(r, 0);
  EXPECT_FALSE(queue.dequeue(r));
}

TEST(AsyncQueueUnitTest, CheckBoundsMulipleProducerQueue) {
  cass::MPMCQueue<int> queue(2);

  EXPECT_TRUE(queue.enqueue(0));
  EXPECT_TRUE(queue.enqueue(1));
  EXPECT_FALSE(queue.enqueue(2));

  int r;
  EXPECT_TRUE(queue.dequeue(r));
  EXPECT_EQ(r, 0);
  EXPECT_TRUE(queue.dequeue(r));
  EXPECT_EQ(r, 1);
  EXPECT_FALSE(queue.dequeue(r));
}

TEST(AsyncQueueUnitTest, VerifySingleProducerQueue) {
  TestAsyncQueue<cass::SPSCQueue<int> > test_queue(NUM_ITERATIONS);

  test_queue.init();
  test_queue.run();

  for (int i = 0; i < NUM_ITERATIONS; ++i) {
    EXPECT_TRUE(test_queue.async_queue_.enqueue(i));
  }

  test_queue.close_and_join();

  EXPECT_EQ(test_queue.value_.load(), NUM_ITERATIONS);
}

TEST(AsyncQueueUnitTest, VerifyMultipleProducerQueue) {
  uv_thread_t threads[NUM_ENQUEUE_THREADS];
  TestAsyncQueue<cass::MPMCQueue<int> > test_queue(NUM_ITERATIONS);

  test_queue.init();
  test_queue.run();

  for (int i = 0; i < NUM_ENQUEUE_THREADS; ++i) {
    uv_thread_create(&threads[i], enqueue_thread, &(test_queue.async_queue_));
  }

  for (int i = 0; i < NUM_ENQUEUE_THREADS; ++i) {
    uv_thread_join(&threads[i]);
  }

  test_queue.close_and_join();

  EXPECT_EQ(test_queue.value_.load(), NUM_ITERATIONS);
}
