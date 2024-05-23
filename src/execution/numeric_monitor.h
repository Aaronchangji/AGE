// Copyright 2022 HDL
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

#pragma once

#include <stdint.h>
#include <atomic>
#include <string>
#include <utility>
#include <vector>

#include "util/tool.h"

namespace AGE {

template <typename T, typename = typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
class NumericMonitor {
   public:
    explicit NumericMonitor(bool is_stream = false)
        : data_(0), last_value_(0), last_time_(Tool::getTimeNs()), is_stream_(is_stream) {}

    void record() { data_++; }
    void record(T val) { data_ += val; }

    T get() {
        T ret = data_.load();

        if (is_stream_) {
            ret -= last_value_;
        }

        last_value_ = ret;
        last_time_ = Tool::getTimeNs();

        return ret;
    }

    uint64_t get_last_time() { return last_time_; }
    uint64_t get_last_value() { return last_value_; }

    void reset() {
        data_ = 0;
        last_value_ = 0;
        last_time_ = Tool::getTimeNs();
    }

   private:
    std::atomic<T> data_;
    T last_value_;
    uint64_t last_time_;
    bool is_stream_;
};

class NetworkMonitors {
   public:
    NetworkMonitors() : msgs_(false), heavy_msgs_(false), bytes_(false), heavy_bytes_(false) {}

    void record(uint64_t bytes, bool is_heavy) {
        if (is_heavy) {
            heavy_msgs_.record();
            heavy_bytes_.record(bytes);
        }
        msgs_.record();
        bytes_.record(bytes);
    }

    void get(uint64_t& msgs, uint64_t& heavy_msgs, uint64_t& bytes, uint64_t& heavy_bytes) {
        msgs = msgs_.get();
        heavy_msgs = heavy_msgs_.get();
        bytes = bytes_.get();
        heavy_bytes = heavy_bytes_.get();
    }

    void reset() {
        msgs_.reset();
        heavy_msgs_.reset();
        bytes_.reset();
        heavy_bytes_.reset();
    }

    void print_last() {
        uint64_t last_time = msgs_.get_last_time();
        uint64_t last_value = msgs_.get_last_value();
        uint64_t cur_value = msgs_.get();
        if (cur_value == 0) { return; }
        float msg_thpt = (cur_value - last_value) / ((Tool::getTimeNs() - last_time) / 1e9);

        last_time = bytes_.get_last_time();
        last_value = bytes_.get_last_value();
        cur_value = bytes_.get();
        float byte_thpt = (cur_value - last_value) / ((Tool::getTimeNs() - last_time) / 1e9);

        LOG(INFO) << "Network Thpt: " << msg_thpt << " msg/s, " << byte_thpt / 1e6 << " MB/s";
    }

   private:
    NumericMonitor<uint64_t> msgs_;
    NumericMonitor<uint64_t> heavy_msgs_;
    NumericMonitor<uint64_t> bytes_;
    NumericMonitor<uint64_t> heavy_bytes_;
};
}  // namespace AGE
