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

#include "metrics.hpp"

#include "test_utils.hpp"

#include <uv.h>

#define NUM_THREADS 2
#define NUM_ITERATIONS 100

struct CounterThreadArgs {
  uv_thread_t thread;
  cass::Metrics::Counter* counter;
};

void counter_thread(void* data) {
  CounterThreadArgs* args = static_cast<CounterThreadArgs*>(data);
  for (int i = 0; i < NUM_ITERATIONS; ++i) {
    args->counter->inc();
  }
}

struct HistogramThreadArgs {
  uv_thread_t thread;
  uint64_t id;
  cass::Metrics::Histogram* histogram;
};

void histogram_thread(void* data) {
  HistogramThreadArgs* args = static_cast<HistogramThreadArgs*>(data);
  for (uint64_t i = 0; i < 100; ++i) {
    args->histogram->record_value(args->id + i * NUM_THREADS);
  }
}

struct MeterThreadArgs {
  uv_thread_t thread;
  cass::Metrics::Meter* meter;
};

void meter_thread(void* data) {
  MeterThreadArgs* args = static_cast<MeterThreadArgs*>(data);

  // ~10 requests a second (Needs to run for at least 5 seconds)
  for (int i = 0; i < 51; ++i) {
    test::Utils::msleep(100);
    args->meter->mark();
  }
}

TEST(MetricsUnitTest, Counter) {
  cass::Metrics::ThreadState thread_state(1);
  cass::Metrics::Counter counter(&thread_state);

  EXPECT_EQ(counter.sum(), 0);

  counter.inc();
  EXPECT_EQ(counter.sum(), 1);

  counter.dec();
  EXPECT_EQ(counter.sum(), 0);

  counter.inc();
  EXPECT_EQ(counter.sum_and_reset(), 1);
  EXPECT_EQ(counter.sum(), 0);
}

TEST(MetricsUnitTest, CounterWithThreads) {
  CounterThreadArgs args[NUM_THREADS];

  cass::Metrics::ThreadState thread_state(NUM_THREADS);
  cass::Metrics::Counter counter(&thread_state);

  for (int i = 0; i < NUM_THREADS; ++i) {
    args[i].counter = &counter;
    uv_thread_create(&args[i].thread, counter_thread, &args[i]);
  }

  for (int i = 0; i < NUM_THREADS; ++i) {
    uv_thread_join(&args[i].thread);
  }

  EXPECT_EQ(counter.sum(), static_cast<int64_t>(NUM_THREADS * NUM_ITERATIONS));
}

TEST(MetricsUnitTest, Histogram) {
  cass::Metrics::ThreadState thread_state(1);
  cass::Metrics::Histogram histogram(&thread_state);

  for (uint64_t i = 1; i <= 100; ++i) {
    histogram.record_value(i);
  }

  cass::Metrics::Histogram::Snapshot snapshot;
  histogram.get_snapshot(&snapshot);

  EXPECT_EQ(snapshot.min, 1);
  EXPECT_EQ(snapshot.max, 100);
  EXPECT_EQ(snapshot.median, 50);
  EXPECT_EQ(snapshot.percentile_75th, 75);
  EXPECT_EQ(snapshot.percentile_95th, 95);
  EXPECT_EQ(snapshot.percentile_98th, 98);
  EXPECT_EQ(snapshot.percentile_99th, 99);
  EXPECT_EQ(snapshot.percentile_999th, 100);
  EXPECT_EQ(snapshot.mean, 50);
  EXPECT_EQ(snapshot.stddev, 28);
}

TEST(MetricsUnitTest, HistogramWithThreads) {
  HistogramThreadArgs args[NUM_THREADS];

  cass::Metrics::ThreadState thread_state(NUM_THREADS);
  cass::Metrics::Histogram histogram(&thread_state);

  for (int i = 0; i < NUM_THREADS; ++i) {
    args[i].histogram = &histogram;
    args[i].id = i + 1;
    uv_thread_create(&args[i].thread, histogram_thread, &args[i]);
  }

  for (int i = 0; i < NUM_THREADS; ++i) {
    uv_thread_join(&args[i].thread);
  }

  cass::Metrics::Histogram::Snapshot snapshot;
  histogram.get_snapshot(&snapshot);

  EXPECT_EQ(snapshot.min, 1);
  EXPECT_EQ(snapshot.max, 100 * NUM_THREADS);
  EXPECT_EQ(snapshot.median, (50 * NUM_THREADS * 100) / 100);
  EXPECT_EQ(snapshot.percentile_75th, (75 * NUM_THREADS * 100) / 100);
  EXPECT_EQ(snapshot.percentile_95th, (95 * NUM_THREADS * 100) / 100);
  EXPECT_EQ(snapshot.percentile_98th, (98 * NUM_THREADS * 100) / 100);
  EXPECT_EQ(snapshot.percentile_99th, (99 * NUM_THREADS * 100) / 100);
  EXPECT_EQ(snapshot.percentile_999th, 100 * NUM_THREADS);
  EXPECT_EQ(snapshot.mean, snapshot.median);
}

TEST(MetricsUnitTest, Meter) {
  cass::Metrics::ThreadState thread_state(1);
  cass::Metrics::Meter meter(&thread_state);

  // ~10 requests a second (Needs to run for at least 5 seconds)
  for (int i = 0; i < 51; ++i) {
    test::Utils::msleep(100);
    meter.mark();
  }

  // Sleep can be off by as much as 10+ ms on most systems (or >10% for 100ms)
  double tolerance = 15.0;
#ifdef _MSC_VER
  // Sleep can be off more on Windows; increasing tolerance
  // https://msdn.microsoft.com/en-us/library/windows/desktop/ms686298(v=vs.85).aspx
  tolerance = 25.0;
# ifndef _M_X64
  // 32-bit metrics are slower on Windows (split operations)
  tolerance *= 1.5;
# endif
#endif
  double expected = 10;
  double abs_error = tolerance * expected;
  EXPECT_NEAR(meter.mean_rate(), expected, abs_error);
  EXPECT_NEAR(meter.one_minute_rate(), expected, abs_error);
  EXPECT_NEAR(meter.five_minute_rate(), expected, abs_error);
  EXPECT_NEAR(meter.fifteen_minute_rate(), expected, abs_error);
}

TEST(MetricsUnitTest, MeterWithThreads) {
  MeterThreadArgs args[NUM_THREADS];

  cass::Metrics::ThreadState thread_state(NUM_THREADS);
  cass::Metrics::Meter meter(&thread_state);

  for (int i = 0; i < NUM_THREADS; ++i) {
    args[i].meter = &meter;
    uv_thread_create(&args[i].thread, meter_thread, &args[i]);
  }

  for (int i = 0; i < NUM_THREADS; ++i) {
    uv_thread_join(&args[i].thread);
  }

  // Sleep can be off by as much as 10+ ms on most systems (or 10% for 100ms)
    double tolerance = 15.0;
#ifdef _MSC_VER
  // Sleep can be off more on Windows; increasing tolerance
  // https://msdn.microsoft.com/en-us/library/windows/desktop/ms686298(v=vs.85).aspx
  tolerance = 25.0;
# ifndef _M_X64
  // 32-bit metrics are slower on Windows (split operations)
  tolerance *= 1.5;
# endif
#endif
  double expected = 10 * NUM_THREADS;
  double abs_error = tolerance * expected;
  EXPECT_NEAR(meter.mean_rate(), expected, abs_error);
  EXPECT_NEAR(meter.one_minute_rate(), expected, abs_error);
  EXPECT_NEAR(meter.five_minute_rate(), expected, abs_error);
  EXPECT_NEAR(meter.fifteen_minute_rate(), expected, abs_error);
}
