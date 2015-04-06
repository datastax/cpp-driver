/*
  Copyright (c) 2014-2015 DataStax

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

#include "metrics.hpp"

#include <boost/chrono.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/floating_point_comparison.hpp>
#include <boost/thread/thread.hpp>

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
    boost::this_thread::sleep_for(boost::chrono::milliseconds(100));
    args->meter->mark();
  }
}

BOOST_AUTO_TEST_SUITE(metrics)

BOOST_AUTO_TEST_CASE(counter)
{
  cass::Metrics::ThreadState thread_state(1);
  cass::Metrics::Counter counter(&thread_state);

  BOOST_CHECK(counter.sum() == 0);

  counter.inc();
  BOOST_CHECK(counter.sum() == 1);

  counter.dec();
  BOOST_CHECK(counter.sum() == 0);

  counter.inc();
  BOOST_CHECK(counter.sum_and_reset() == 1);
  BOOST_CHECK(counter.sum() == 0);
}

BOOST_AUTO_TEST_CASE(counter_threads)
{
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

  BOOST_CHECK(counter.sum() == static_cast<uint64_t>(NUM_THREADS * NUM_ITERATIONS));
}

BOOST_AUTO_TEST_CASE(histogram)
{
  cass::Metrics::ThreadState thread_state(1);
  cass::Metrics::Histogram histogram(&thread_state);

  for (uint64_t i = 1; i <= 100; ++i) {
    histogram.record_value(i);
  }

  cass::Metrics::Histogram::Snapshot snapshot;
  histogram.get_snapshot(&snapshot);

  BOOST_CHECK(snapshot.min == 1);
  BOOST_CHECK(snapshot.max == 100);
  BOOST_CHECK(snapshot.median == 50);
  BOOST_CHECK(snapshot.percentile_75th == 75);
  BOOST_CHECK(snapshot.percentile_95th == 95);
  BOOST_CHECK(snapshot.percentile_98th == 98);
  BOOST_CHECK(snapshot.percentile_99th == 99);
  BOOST_CHECK(snapshot.percentile_999th == 100);
  BOOST_CHECK(snapshot.mean == 50);
  BOOST_CHECK(snapshot.stddev == 28);
}

BOOST_AUTO_TEST_CASE(histogram_threads)
{
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

  BOOST_CHECK(snapshot.min == 1);
  BOOST_CHECK(snapshot.max == 100 * NUM_THREADS);
  BOOST_CHECK(snapshot.median == (50 * NUM_THREADS * 100) / 100);
  BOOST_CHECK(snapshot.percentile_75th == (75 * NUM_THREADS * 100) / 100);
  BOOST_CHECK(snapshot.percentile_95th == (95 * NUM_THREADS * 100) / 100);
  BOOST_CHECK(snapshot.percentile_98th == (98 * NUM_THREADS * 100) / 100);
  BOOST_CHECK(snapshot.percentile_99th == (99 * NUM_THREADS * 100) / 100);
  BOOST_CHECK(snapshot.percentile_999th == 100 * NUM_THREADS);
  BOOST_CHECK(snapshot.mean == snapshot.median);
}

BOOST_AUTO_TEST_CASE(meter)
{
  cass::Metrics::ThreadState thread_state(1);
  cass::Metrics::Meter meter(&thread_state);

  // ~10 requests a second (Needs to run for at least 5 seconds)
  for (int i = 0; i < 51; ++i) {
    boost::this_thread::sleep_for(boost::chrono::milliseconds(100));
    meter.mark();
  }

  // Sleep can be off by as much as 10+ ms on most systems (or >10% for 100ms)
  BOOST_CHECK_CLOSE(meter.mean_rate(), 10, 15.0);
  BOOST_CHECK_CLOSE(meter.one_minute_rate(), 10, 15.0);
  BOOST_CHECK_CLOSE(meter.five_minute_rate(), 10, 15.0);
  BOOST_CHECK_CLOSE(meter.fifteen_minute_rate(), 10, 15.0);
}

BOOST_AUTO_TEST_CASE(meter_threads)
{
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
  BOOST_CHECK_CLOSE(meter.mean_rate(), 10 * NUM_THREADS, 15.0);
  BOOST_CHECK_CLOSE(meter.one_minute_rate(), 10 * NUM_THREADS, 15.0);
  BOOST_CHECK_CLOSE(meter.five_minute_rate(), 10 * NUM_THREADS, 15.0);
  BOOST_CHECK_CLOSE(meter.fifteen_minute_rate(), 10 * NUM_THREADS, 15.0);
}

BOOST_AUTO_TEST_SUITE_END()
