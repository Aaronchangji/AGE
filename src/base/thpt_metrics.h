// Copyright 2021 HDL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <glog/logging.h>
#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "base/serializer.h"

#pragma once

namespace AGE {

using std::string;

class InnerMetrics {
   public:
    InnerMetrics()
        : qps_(.0),
          instant_qps_(.0),
          lat_p50_(.0),
          lat_p90_(.0),
          lat_p95_(.0),
          lat_p99_(.0),
          batchwidth_p99_(.0),
          qos_(.0),
          req_thpt_(.0),
          heavy_req_thpt_(.0),
          bytes_thpt_(.0),
          heavy_bytes_thpt_(.0),
          cache_req_thpt_(.0),
          cache_heavy_req_thpt_(.0),
          cache_bytes_thpt_(.0),
          cache_heavy_bytes_thpt_(.0) {}

    void collect(double qps, double lat_p50, double lat_p90, double lat_p95, double lat_p99, double batchwidth_p99, double qos = 0.0) {
        qps_ = qps;
        lat_p50_ = lat_p50;
        lat_p90_ = lat_p90;
        lat_p95_ = lat_p95;
        lat_p99_ = lat_p99;
        batchwidth_p99_ = batchwidth_p99;
        qos_ = qos;
    }

    void collect_request_info(double req_thpt, double heavy_req_thpt, double bytes_thpt, double heavy_bytes_thpt, bool is_cache = false) {
        if (is_cache) {
            cache_req_thpt_ = req_thpt;
            cache_heavy_req_thpt_ = heavy_req_thpt;
            cache_bytes_thpt_ = bytes_thpt;
            cache_heavy_bytes_thpt_ = heavy_bytes_thpt;
        } else {
            req_thpt_ = req_thpt;
            heavy_req_thpt_ = heavy_req_thpt;
            bytes_thpt_ = bytes_thpt;
            heavy_bytes_thpt_ = heavy_bytes_thpt;
        }
    }

    string print(string& delimiter, bool with_timeout = false) {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(4) << qps_ << delimiter << instant_qps_ << delimiter << lat_p50_
            << delimiter << lat_p90_ << delimiter << lat_p95_ << delimiter << lat_p99_ << delimiter << batchwidth_p99_;
        if (with_timeout) oss << delimiter << qos_;
        oss << "\n" << req_thpt_ << delimiter << heavy_req_thpt_ << delimiter << bytes_thpt_ << delimiter
            << heavy_bytes_thpt_;
        oss << "\n" << cache_req_thpt_ << delimiter << cache_heavy_req_thpt_ << delimiter << cache_bytes_thpt_ << delimiter
            << cache_heavy_bytes_thpt_;
        return oss.str();
    }

    void ToString(string* s, bool with_timeout = false) {
        Serializer::appendDouble(s, qps_);
        Serializer::appendDouble(s, instant_qps_);
        Serializer::appendDouble(s, lat_p50_);
        Serializer::appendDouble(s, lat_p90_);
        Serializer::appendDouble(s, lat_p95_);
        Serializer::appendDouble(s, lat_p99_);
        Serializer::appendDouble(s, batchwidth_p99_);
        if (with_timeout) Serializer::appendDouble(s, qos_);
        Serializer::appendDouble(s, req_thpt_);
        Serializer::appendDouble(s, heavy_req_thpt_);
        Serializer::appendDouble(s, bytes_thpt_);
        Serializer::appendDouble(s, heavy_bytes_thpt_);
        Serializer::appendDouble(s, cache_req_thpt_);
        Serializer::appendDouble(s, cache_heavy_req_thpt_);
        Serializer::appendDouble(s, cache_bytes_thpt_);
        Serializer::appendDouble(s, cache_heavy_bytes_thpt_);
    }

    void FromString(const string& s, size_t& pos, bool with_timeout = false) {
        Serializer::readDouble(s, pos, &qps_);
        Serializer::readDouble(s, pos, &instant_qps_);
        Serializer::readDouble(s, pos, &lat_p50_);
        Serializer::readDouble(s, pos, &lat_p90_);
        Serializer::readDouble(s, pos, &lat_p95_);
        Serializer::readDouble(s, pos, &lat_p99_);
        Serializer::readDouble(s, pos, &batchwidth_p99_);
        if (with_timeout) Serializer::readDouble(s, pos, &qos_);
        Serializer::readDouble(s, pos, &req_thpt_);
        Serializer::readDouble(s, pos, &heavy_req_thpt_);
        Serializer::readDouble(s, pos, &bytes_thpt_);
        Serializer::readDouble(s, pos, &heavy_bytes_thpt_);
        Serializer::readDouble(s, pos, &cache_req_thpt_);
        Serializer::readDouble(s, pos, &cache_heavy_req_thpt_);
        Serializer::readDouble(s, pos, &cache_bytes_thpt_);
        Serializer::readDouble(s, pos, &cache_heavy_bytes_thpt_);
    }

    void reset() {
        qps_ = 0;
        instant_qps_ = 0;
        lat_p50_ = 0;
        lat_p90_ = 0;
        lat_p95_ = 0;
        lat_p99_ = 0;
        batchwidth_p99_ = 0;
        qos_ = 0;
        req_thpt_ = 0;
        heavy_req_thpt_ = 0;
        bytes_thpt_ = 0;
        heavy_bytes_thpt_ = 0;
        cache_req_thpt_ = 0;
        cache_heavy_req_thpt_ = 0;
        cache_bytes_thpt_ = 0;
        cache_heavy_bytes_thpt_ = 0;
    }

    void merge(const InnerMetrics& other, bool with_timeout = false) {
        qps_ += other.qps_;
        instant_qps_ += other.instant_qps_;
        lat_p50_ += other.lat_p50_;
        lat_p90_ += other.lat_p90_;
        lat_p95_ += other.lat_p95_;
        lat_p99_ += other.lat_p99_;
        batchwidth_p99_ = batchwidth_p99_ ? std::min(batchwidth_p99_, other.batchwidth_p99_) : other.batchwidth_p99_;
        if (with_timeout) qos_ += other.qos_;
        req_thpt_ += other.req_thpt_;
        heavy_req_thpt_ += other.heavy_req_thpt_;
        bytes_thpt_ += other.bytes_thpt_;
        heavy_bytes_thpt_ += other.heavy_bytes_thpt_;
        cache_req_thpt_ += other.cache_req_thpt_;
        cache_heavy_req_thpt_ += other.cache_heavy_req_thpt_;
        cache_bytes_thpt_ += other.cache_bytes_thpt_;
        cache_heavy_bytes_thpt_ += other.cache_heavy_bytes_thpt_;
    }

    void settle_down(size_t sz, bool with_timeout = false) {
        lat_p50_ /= sz;
        lat_p90_ /= sz;
        lat_p95_ /= sz;
        lat_p99_ /= sz;
        if (with_timeout) qos_ /= sz;
    }

#define _DEF_GET_(key) \
    double get_##key() { return key##_; }

    _DEF_GET_(qps)
    _DEF_GET_(instant_qps)
    _DEF_GET_(lat_p50)
    _DEF_GET_(lat_p90)
    _DEF_GET_(lat_p95)
    _DEF_GET_(lat_p99)
    _DEF_GET_(batchwidth_p99)
    _DEF_GET_(qos)
    _DEF_GET_(req_thpt)
    _DEF_GET_(heavy_req_thpt)
    _DEF_GET_(bytes_thpt)
    _DEF_GET_(heavy_bytes_thpt)
    _DEF_GET_(cache_req_thpt)
    _DEF_GET_(cache_heavy_req_thpt)
    _DEF_GET_(cache_bytes_thpt)
    _DEF_GET_(cache_heavy_bytes_thpt)

#undef _DEF_GET_

#define _DEF_SET_(key) \
    void set_##key(double val) { key##_ = val; }

    _DEF_SET_(qps)
    _DEF_SET_(instant_qps)
    _DEF_SET_(lat_p50)
    _DEF_SET_(lat_p90)
    _DEF_SET_(lat_p95)
    _DEF_SET_(lat_p99)
    _DEF_SET_(batchwidth_p99)
    _DEF_SET_(qos)
    _DEF_SET_(req_thpt)
    _DEF_SET_(heavy_req_thpt)
    _DEF_SET_(bytes_thpt)
    _DEF_SET_(heavy_bytes_thpt)
    _DEF_SET_(cache_req_thpt)
    _DEF_SET_(cache_heavy_req_thpt)
    _DEF_SET_(cache_bytes_thpt)
    _DEF_SET_(cache_heavy_bytes_thpt)

#undef _DEF_SET_

   private:
    double qps_;
    double instant_qps_;
    double lat_p50_;
    double lat_p90_;
    double lat_p95_;
    double lat_p99_;
    double batchwidth_p99_;
    double qos_;

    // network perf
    double req_thpt_;
    double heavy_req_thpt_;
    double bytes_thpt_;
    double heavy_bytes_thpt_;
    double cache_req_thpt_;
    double cache_heavy_req_thpt_;
    double cache_bytes_thpt_;
    double cache_heavy_bytes_thpt_;
};
}  // namespace AGE
