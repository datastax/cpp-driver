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

// Based on implemenations of metrics (especially Meter) from Java library
// com.codehale.Metrics (https://github.com/dropwizard/metrics)

#ifndef __CASS_METRICS_HPP_INCLUDED__
#define __CASS_METRICS_HPP_INCLUDED__

#include "atomic.hpp"
#include "scoped_ptr.hpp"
#include "scoped_lock.hpp"

#include "third_party/hdr_histogram/hdr_histogram.hpp"

#include <uv.h>
#include <stdlib.h>

#if defined(WIN32) || defined(_WIN32)
#ifndef _WINSOCKAPI_
#define _WINSOCKAPI_
#endif
#include <Windows.h>
#else
#include <sched.h>
#endif

#include <limits>
#include <math.h>

namespace cass {

class Metrics {
public:
  class ThreadState {
  public:
#if UV_VERSION_MAJOR == 0
    ThreadState()
      : max_threads_(1) {}
#else
    ThreadState(size_t max_threads)
      : max_threads_(max_threads)
      , thread_count_(1) {
      uv_key_create(&thread_id_key_);
    }
#endif

#if UV_VERSION_MAJOR >= 1
    ~ThreadState() {
      uv_key_delete(&thread_id_key_);
    }
#endif

    size_t max_threads() const {
      return max_threads_;
    }

    size_t current_thread_id() {
#if UV_VERSION_MAJOR == 0
      return 0;
#else
      void* id = uv_key_get(&thread_id_key_);
      if (id == NULL) {
        size_t thread_id = thread_count_.fetch_add(1);
        assert(thread_id <= max_threads_);
        id = reinterpret_cast<void*>(thread_id);
        uv_key_set(&thread_id_key_, id);
      }
      return reinterpret_cast<size_t>(id) - 1;
#endif
    }

  private:
    const size_t max_threads_;
#if UV_VERSION_MAJOR >= 1
    Atomic<size_t> thread_count_;
    uv_key_t thread_id_key_;
#endif
  };

  class Counter {
  public:
    Counter(ThreadState* thread_state)
      : thread_state_(thread_state)
      , counters_(new PerThreadCounter[thread_state->max_threads()]) {}

    void inc() {
      counters_[thread_state_->current_thread_id()].add(1LL);
    }

    void dec() {
      counters_[thread_state_->current_thread_id()].sub(1LL);
    }

    int64_t sum() const {
      int64_t sum = 0;
      for (size_t i = 0; i < thread_state_->max_threads(); ++i) {
        sum += counters_[i].get();
      }
      return sum;
    }

    int64_t sum_and_reset() {
      int64_t sum = 0;
      for (size_t i = 0; i < thread_state_->max_threads(); ++i) {
        sum += counters_[i].get_and_reset();
      }
      return sum;
    }

  private:
    class PerThreadCounter {
    public:
      PerThreadCounter()
        : value_(0) {}

      void add(int64_t n) {
        value_.fetch_add(n, MEMORY_ORDER_RELEASE);
      }

      void sub(int64_t n) {
        value_.fetch_sub(n, MEMORY_ORDER_RELEASE);
      }

      int64_t get() const {
        return value_.load(MEMORY_ORDER_ACQUIRE);
      }

      int64_t get_and_reset() {
        return value_.exchange(0, MEMORY_ORDER_RELEASE);
      }

    private:
      Atomic<int64_t> value_;

      static const size_t cacheline_size = 64;
      char pad__[cacheline_size];
      void no_unused_private_warning__() { pad__[0] = 0; }
    };

  private:
    ThreadState* thread_state_;
    ScopedPtr<PerThreadCounter[]> counters_;

  private:
    DISALLOW_COPY_AND_ASSIGN(Counter);
  };

  class ExponentiallyWeightedMovingAverage {
    public:
      static const uint64_t INTERVAL = 5;

      ExponentiallyWeightedMovingAverage(double alpha, ThreadState* thread_state)
        : alpha_(alpha)
        , uncounted_(thread_state)
        , is_initialized_(false)
        , rate_(0.0) {}

      double rate() const {
        return rate_.load(MEMORY_ORDER_ACQUIRE);
      }

      void update() {
        uncounted_.inc();
      }

      void tick() {
        const int64_t count = uncounted_.sum_and_reset();
        double instant_rate = static_cast<double>(count) / INTERVAL;

        if (is_initialized_.load(MEMORY_ORDER_ACQUIRE)) {
          double rate = rate_.load(MEMORY_ORDER_ACQUIRE);
          rate_.store(rate + (alpha_ * (instant_rate - rate)), MEMORY_ORDER_RELEASE);
        } else {
          rate_.store(instant_rate, MEMORY_ORDER_RELEASE);
          is_initialized_.store(true, MEMORY_ORDER_RELEASE);
        }
      }

    private:
      const double alpha_;
      Counter uncounted_;
      Atomic<bool> is_initialized_;
      Atomic<double> rate_;
  };

  class Meter {
    public:
      Meter(ThreadState* thread_state)
        : one_minute_rate_(1.0 - exp(-static_cast<double>(ExponentiallyWeightedMovingAverage::INTERVAL) / 60.0 / 1), thread_state)
        , five_minute_rate_(1.0 - exp(-static_cast<double>(ExponentiallyWeightedMovingAverage::INTERVAL) / 60.0 / 5), thread_state)
        , fifteen_minute_rate_(1.0 - exp(-static_cast<double>(ExponentiallyWeightedMovingAverage::INTERVAL) / 60.0 / 15), thread_state)
        , count_(thread_state)
        , start_time_(uv_hrtime())
        , last_tick_(start_time_) {}

      void mark() {
        tick_if_necessary();
        count_.inc();
        one_minute_rate_.update();
        five_minute_rate_.update();
        fifteen_minute_rate_.update();
      }

      double one_minute_rate() const { return one_minute_rate_.rate(); }
      double five_minute_rate() const { return five_minute_rate_.rate(); }
      double fifteen_minute_rate() const { return fifteen_minute_rate_.rate(); }

      double mean_rate() const {
        if (count() == 0) {
          return 0.0;
        } else {
          double elapsed = static_cast<double>(uv_hrtime() - start_time_) / 1e9;
          return count() / elapsed;
        }
      }

      uint64_t count() const {
        return count_.sum();
      }

    private:
      static const uint64_t TICK_INTERVAL = ExponentiallyWeightedMovingAverage::INTERVAL * 1000LL * 1000LL * 1000LL;

      void tick_if_necessary() {
        uint64_t old_tick = last_tick_.load();
        uint64_t new_tick = uv_hrtime();
        uint64_t elapsed = new_tick - old_tick;

        if (elapsed > TICK_INTERVAL) {
          uint64_t new_interval_start_tick = new_tick - elapsed % TICK_INTERVAL;
          if (last_tick_.compare_exchange_strong(old_tick, new_interval_start_tick)) {
            uint64_t required_ticks = elapsed / TICK_INTERVAL;
            for (uint64_t i = 0; i < required_ticks; ++i) {
              one_minute_rate_.tick();
              five_minute_rate_.tick();
              fifteen_minute_rate_.tick();
            }
          }
        }
      }

      ExponentiallyWeightedMovingAverage one_minute_rate_;
      ExponentiallyWeightedMovingAverage five_minute_rate_;
      ExponentiallyWeightedMovingAverage fifteen_minute_rate_;
      Counter count_;
      const uint64_t start_time_;
      Atomic<uint64_t> last_tick_;

  private:
      DISALLOW_COPY_AND_ASSIGN(Meter);
  };

  class Histogram {
  public:
    static const int64_t HIGHEST_TRACKABLE_VALUE = 3600LL * 1000LL * 1000LL;

    struct Snapshot {
      int64_t min;
      int64_t max;
      int64_t mean;
      int64_t stddev;
      int64_t median;
      int64_t percentile_75th;
      int64_t percentile_95th;
      int64_t percentile_98th;
      int64_t percentile_99th;
      int64_t percentile_999th;
    };

    Histogram(ThreadState* thread_state)
      : thread_state_(thread_state)
      , histograms_(new PerThreadHistogram[thread_state->max_threads()]) {
      hdr_init(1LL, HIGHEST_TRACKABLE_VALUE, 3, &histogram_);
      uv_mutex_init(&mutex_);
    }

    ~Histogram() {
      free(histogram_);
      uv_mutex_destroy(&mutex_);
    }

    void record_value(int64_t value) {
      // All threads share the same histogram so access needs to be synchronized
#if UV_VERSION_MAJOR == 0
      ScopedMutex l(&mutex_);
#endif
      histograms_[thread_state_->current_thread_id()].record_value(value);
    }

    void get_snapshot(Snapshot* snapshot) const {
      ScopedMutex l(&mutex_);
      hdr_histogram* h = histogram_;
      for (size_t i = 0; i < thread_state_->max_threads(); ++i) {
        histograms_[i].add(h);
      }
      snapshot->min = hdr_min(h);
      snapshot->max = hdr_max(h);
      snapshot->mean = static_cast<int64_t>(hdr_mean(h));
      snapshot->stddev = static_cast<int64_t>(hdr_stddev(h));
      snapshot->median = hdr_value_at_percentile(h, 50.0);
      snapshot->percentile_75th = hdr_value_at_percentile(h, 75.0);
      snapshot->percentile_95th = hdr_value_at_percentile(h, 95.0);
      snapshot->percentile_98th = hdr_value_at_percentile(h, 98.0);
      snapshot->percentile_99th = hdr_value_at_percentile(h, 99.0);
      snapshot->percentile_999th = hdr_value_at_percentile(h, 99.9);
    }

  private:
#if UV_VERSION_MAJOR == 0
    class PerThreadHistogram {
    public:
      PerThreadHistogram() {
        hdr_init(1LL, HIGHEST_TRACKABLE_VALUE, 3, &histogram_);
      }

      ~PerThreadHistogram() {
        free(histogram_);
      }

      void record_value(int64_t value) {
        hdr_record_value(histogram_, value);

      }

      void add(hdr_histogram* to) {
        hdr_add(to, histogram_);
      }

    private:
      hdr_histogram* histogram_;
    };
#else
    class WriterReaderPhaser {
    public:
      WriterReaderPhaser()
        : start_epoch_(0)
        , even_end_epoch_(0)
        , odd_end_epoch_(std::numeric_limits<int64_t>::min()) {}

      int64_t writer_critical_section_enter() {
        return start_epoch_.fetch_add(1);
      }

      void writer_critical_section_end(int64_t critical_value_enter) {
        if (critical_value_enter < 0) {
          odd_end_epoch_.fetch_add(1);
        } else {
          even_end_epoch_.fetch_add(1);
        }
      }

      // The reader is protected by a mutex in Histogram
      void flip_phase() {
        bool is_next_phase_even = (start_epoch_.load() < 0);

        int64_t initial_start_value;

        if (is_next_phase_even) {
          initial_start_value = 0;
          even_end_epoch_.store(initial_start_value, MEMORY_ORDER_RELAXED);
        } else {
          initial_start_value = std::numeric_limits<int64_t>::min();
          odd_end_epoch_.store(initial_start_value, MEMORY_ORDER_RELAXED);
        }

        int64_t start_value_at_flip = start_epoch_.exchange(initial_start_value);

        bool is_caught_up = false;
        do {
          if (is_next_phase_even) {
            is_caught_up = (odd_end_epoch_.load() == start_value_at_flip);
          } else {
            is_caught_up = (even_end_epoch_.load() == start_value_at_flip);
          }
          if (!is_caught_up) {
#if defined(WIN32) || defined(_WIN32)
            SwitchToThread();
#else
            sched_yield();
#endif
          }
        } while(!is_caught_up);
      }

    private:
      Atomic<int64_t> start_epoch_;
      Atomic<int64_t> even_end_epoch_;
      Atomic<int64_t> odd_end_epoch_;
    };

    class PerThreadHistogram {
    public:
      PerThreadHistogram()
        : active_index_(0) {
        hdr_init(1LL, HIGHEST_TRACKABLE_VALUE, 3, &histograms_[0]);
        hdr_init(1LL, HIGHEST_TRACKABLE_VALUE, 3, &histograms_[1]);
      }

      ~PerThreadHistogram() {
        free(histograms_[0]);
        free(histograms_[1]);
      }

      void record_value(int64_t value) {
        int64_t critical_value_enter = phaser_.writer_critical_section_enter();
        hdr_histogram* h = histograms_[active_index_.load()];
        hdr_record_value(h, value);
        phaser_.writer_critical_section_end(critical_value_enter);
      }

      void add(hdr_histogram* to) {
        int inactive_index = active_index_.exchange(!active_index_.load());
        hdr_histogram* from = histograms_[inactive_index];
        phaser_.flip_phase();
        hdr_add(to, from);
      }

    private:
      hdr_histogram* histograms_[2];
      Atomic<int> active_index_;
      WriterReaderPhaser phaser_;
    };
#endif

    ThreadState* thread_state_;
    ScopedPtr<PerThreadHistogram[]> histograms_;
    hdr_histogram* histogram_;
    mutable uv_mutex_t mutex_;

  private:
    DISALLOW_COPY_AND_ASSIGN(Histogram);
  };

  Metrics(size_t max_threads)
  // Note: For best performance use libuv 1.X!

  // libuv 0.10.X doesn't support thread-local variables so that means
  // per-thread counters and histograms are disabled which means
  // that these shared objects need to have their access synchronized
  // (and means increased contention). Counters already uses an atomic
  // type so this doesn't need any update, but it means that all threads
  // use the same counter. Histogram uses a shared lock instead
  // of accumulating latencies from per-thread instances.
#if UV_VERSION_MAJOR == 0
    : thread_state_()
#else
    : thread_state_(max_threads)
#endif
    , request_latencies(&thread_state_)
    , request_rates(&thread_state_)
    , total_connections(&thread_state_)
    , available_connections(&thread_state_)
    , exceeded_pending_requests_water_mark(&thread_state_)
    , exceeded_write_bytes_water_mark(&thread_state_)
    , connection_timeouts(&thread_state_)
    , pending_request_timeouts(&thread_state_)
    , request_timeouts(&thread_state_) {}

  void record_request(uint64_t latency_ns) {
    // Final measurement is in microseconds
    request_latencies.record_value(latency_ns / 1000);
    request_rates.mark();
  }

private:
  ThreadState thread_state_;

public:
  Histogram request_latencies;
  Meter request_rates;

  Counter total_connections;
  Counter available_connections;
  Counter exceeded_pending_requests_water_mark;
  Counter exceeded_write_bytes_water_mark;

  Counter connection_timeouts;
  Counter pending_request_timeouts;
  Counter request_timeouts;

private:
  DISALLOW_COPY_AND_ASSIGN(Metrics);
};

} // namespace cass

#endif
