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

using datastax::internal::core::Metrics;

struct CounterThreadArgs {
  uv_thread_t thread;
  Metrics::Counter* counter;
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
  Metrics::Histogram* histogram;
};

void histogram_thread(void* data) {
  HistogramThreadArgs* args = static_cast<HistogramThreadArgs*>(data);
  for (uint64_t i = 0; i < 100; ++i) {
    args->histogram->record_value(args->id + i * NUM_THREADS);
  }
}

struct MeterThreadArgs {
  uv_thread_t thread;
  Metrics::Meter* meter;
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
  Metrics::ThreadState thread_state(1);
  Metrics::Counter counter(&thread_state);

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

  Metrics::ThreadState thread_state(NUM_THREADS);
  Metrics::Counter counter(&thread_state);

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
  Metrics::ThreadState thread_state(1);
  Metrics::Histogram histogram(&thread_state);

  for (uint64_t i = 1; i <= 100; ++i) {
    histogram.record_value(i);
  }

  Metrics::Histogram::Snapshot snapshot;
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

TEST(MetricsUnitTest, HistogramEmpty) {
  Metrics::ThreadState thread_state(1);
  Metrics::Histogram histogram(&thread_state);

  Metrics::Histogram::Snapshot snapshot;
  histogram.get_snapshot(&snapshot);

  EXPECT_EQ(snapshot.min, 0);
  EXPECT_EQ(snapshot.max, 0);
  EXPECT_EQ(snapshot.mean, 0);
  EXPECT_EQ(snapshot.stddev, 0);
  EXPECT_EQ(snapshot.median, 0);
  EXPECT_EQ(snapshot.percentile_75th, 0);
  EXPECT_EQ(snapshot.percentile_95th, 0);
  EXPECT_EQ(snapshot.percentile_98th, 0);
  EXPECT_EQ(snapshot.percentile_99th, 0);
  EXPECT_EQ(snapshot.percentile_999th, 0);
}

TEST(MetricsUnitTest, HistogramWithRefreshInterval) {
  unsigned refresh_interval = 1000;
  Metrics::ThreadState thread_state(1);
  Metrics::Histogram histogram(&thread_state, refresh_interval);

  Metrics::Histogram::Snapshot snapshot;

  // Retrieval before the first interval runs will simply return zeros
  histogram.get_snapshot(&snapshot);
  EXPECT_EQ(snapshot.min, 0);
  EXPECT_EQ(snapshot.max, 0);
  EXPECT_EQ(snapshot.median, 0);
  EXPECT_EQ(snapshot.percentile_75th, 0);
  EXPECT_EQ(snapshot.percentile_95th, 0);
  EXPECT_EQ(snapshot.percentile_98th, 0);
  EXPECT_EQ(snapshot.percentile_99th, 0);
  EXPECT_EQ(snapshot.percentile_999th, 0);
  EXPECT_EQ(snapshot.mean, 0);
  EXPECT_EQ(snapshot.stddev, 0);

  // Values added during the first interval (or for that matter any
  // interval) will be buffered in per-thread counters and will be
  // included in the next generated snapshot
  for (uint64_t i = 1; i <= 100; ++i) {
    histogram.record_value(i);
  }
  test::Utils::msleep(1.2 * refresh_interval);

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

  // Generated snapshot should only include values added within
  // the current interval
  test::Utils::msleep(1.2 * refresh_interval);
  for (uint64_t i = 101; i <= 200; ++i) {
    histogram.record_value(i);
  }

  histogram.get_snapshot(&snapshot);
  EXPECT_EQ(snapshot.min, 101);
  EXPECT_EQ(snapshot.max, 200);
  EXPECT_EQ(snapshot.median, 150);
  EXPECT_EQ(snapshot.percentile_75th, 175);
  EXPECT_EQ(snapshot.percentile_95th, 195);
  EXPECT_EQ(snapshot.percentile_98th, 198);
  EXPECT_EQ(snapshot.percentile_99th, 199);
  EXPECT_EQ(snapshot.percentile_999th, 200);
  EXPECT_EQ(snapshot.mean, 150);
  EXPECT_EQ(snapshot.stddev, 28);
}

// Variant of the case above.  If we have no requests for the entirety
// of the refresh interval make sure the stats return zero
TEST(MetricsUnitTest, HistogramWithRefreshIntervalNoActivity) {
  unsigned refresh_interval = 1000;
  Metrics::ThreadState thread_state(1);
  Metrics::Histogram histogram(&thread_state, refresh_interval);

  Metrics::Histogram::Snapshot snapshot;

  // Initial refresh interval (where we always return zero) + another interval of
  // no activity
  test::Utils::msleep(2.2 * refresh_interval);

  histogram.get_snapshot(&snapshot);
  EXPECT_EQ(snapshot.min, 0);
  EXPECT_EQ(snapshot.max, 0);
  EXPECT_EQ(snapshot.median, 0);
  EXPECT_EQ(snapshot.percentile_75th, 0);
  EXPECT_EQ(snapshot.percentile_95th, 0);
  EXPECT_EQ(snapshot.percentile_98th, 0);
  EXPECT_EQ(snapshot.percentile_99th, 0);
  EXPECT_EQ(snapshot.percentile_999th, 0);
  EXPECT_EQ(snapshot.mean, 0);
  EXPECT_EQ(snapshot.stddev, 0);
}

TEST(MetricsUnitTest, HistogramWithThreads) {
  HistogramThreadArgs args[NUM_THREADS];

  Metrics::ThreadState thread_state(NUM_THREADS);
  Metrics::Histogram histogram(&thread_state);

  for (int i = 0; i < NUM_THREADS; ++i) {
    args[i].histogram = &histogram;
    args[i].id = i + 1;
    uv_thread_create(&args[i].thread, histogram_thread, &args[i]);
  }

  for (int i = 0; i < NUM_THREADS; ++i) {
    uv_thread_join(&args[i].thread);
  }

  Metrics::Histogram::Snapshot snapshot;
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
  Metrics::ThreadState thread_state(1);
  Metrics::Meter meter(&thread_state);

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
#ifndef _M_X64
  // 32-bit metrics are slower on Windows (split operations)
  tolerance *= 1.5;
#endif
#endif
  double expected = 10;
  double abs_error = tolerance * expected;
  EXPECT_NEAR(meter.mean_rate(), expected, abs_error);
  EXPECT_NEAR(meter.one_minute_rate(), expected, abs_error);
  EXPECT_NEAR(meter.five_minute_rate(), expected, abs_error);
  EXPECT_NEAR(meter.fifteen_minute_rate(), expected, abs_error);
}

TEST(MetricsUnitTest, MeterSpeculative) {
  Metrics::ThreadState thread_state(1);
  Metrics::Meter meter(&thread_state);

  // Emulate a situation where for a total of 60 requests sent on the wire,
  // where 15 are unique requests and 45 are dups (speculative executions).

  for (int i = 0; i < 15; ++i) {
    meter.mark();
  }

  // Test "no speculative execution configured" case while we're here.
  EXPECT_EQ(0.0, meter.speculative_request_percent());
  for (int i = 0; i < 45; ++i) {
    meter.mark_speculative();
  }

  EXPECT_EQ(75.0, meter.speculative_request_percent());
}

TEST(MetricsUnitTest, MeterWithThreads) {
  MeterThreadArgs args[NUM_THREADS];

  Metrics::ThreadState thread_state(NUM_THREADS);
  Metrics::Meter meter(&thread_state);

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
#ifndef _M_X64
  // 32-bit metrics are slower on Windows (split operations)
  tolerance *= 1.5;
#endif
#endif
  double expected = 10 * NUM_THREADS;
  double abs_error = tolerance * expected;
  EXPECT_NEAR(meter.mean_rate(), expected, abs_error);
  EXPECT_NEAR(meter.one_minute_rate(), expected, abs_error);
  EXPECT_NEAR(meter.five_minute_rate(), expected, abs_error);
  EXPECT_NEAR(meter.fifteen_minute_rate(), expected, abs_error);
}
