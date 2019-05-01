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

#ifndef DATASTAX_INTERNAL_HISTOGRAM_WRAPPER_HPP
#define DATASTAX_INTERNAL_HISTOGRAM_WRAPPER_HPP

#ifdef CASS_INTERNAL_DIAGNOSTICS
#include "string.hpp"
#include "third_party/hdr_histogram/hdr_histogram.hpp"

#include <uv.h>

namespace datastax { namespace internal { namespace core {

class HistogramWrapper {
public:
  static const int64_t HIGHEST_TRACKABLE_VALUE = 1000LL * 1000LL;

  HistogramWrapper(const String& name)
      : thread_id_(0)
      , name_(name) {
    hdr_init(1LL, HIGHEST_TRACKABLE_VALUE, 3, &histogram_);
  }

  ~HistogramWrapper() {
    dump();
    free(histogram_);
  }

  void record_value(int64_t value) {
    if (thread_id_ == 0) {
      thread_id_ = uv_thread_self();
    }
    hdr_record_value(histogram_, value);
  }

  void dump() {
    fprintf(stderr,
            "\n%10s %18s"
            "%10s, %10s, %10s, %10s, "
            "%10s, %10s, %10s, %10s, "
            "%10s\n"
            "%10s, %18llx"
            "%10llu, %10llu, %10llu, %10llu, "
            "%10llu, %10llu, %10llu, %10llu, "
            "%10llu\n",
            "name", "thread", "min", "mean", "median", "75th", "95th", "98th", "99th", "99.9th",
            "max", name_.c_str(), static_cast<unsigned long long int>(thread_id_),
            (unsigned long long int)hdr_min(histogram_),
            (unsigned long long int)hdr_mean(histogram_),
            (unsigned long long int)hdr_value_at_percentile(histogram_, 50.0),
            (unsigned long long int)hdr_value_at_percentile(histogram_, 75.0),
            (unsigned long long int)hdr_value_at_percentile(histogram_, 95.0),
            (unsigned long long int)hdr_value_at_percentile(histogram_, 98.0),
            (unsigned long long int)hdr_value_at_percentile(histogram_, 99.0),
            (unsigned long long int)hdr_value_at_percentile(histogram_, 99.9),
            (unsigned long long int)hdr_max(histogram_));
  }

private:
  uv_thread_t thread_id_;
  hdr_histogram* histogram_;
  String name_;
};

}}} // namespace datastax::internal::core

#endif // CASS_INTERNAL_DIAGNOSTICS

#endif
