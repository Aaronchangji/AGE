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

#pragma once

#include <glog/logging.h>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "base/serializer.h"
#include "operator/op_type.h"
#include "util/tool.h"

namespace AGE {
namespace MLModel {

struct dist_info_t {
    double mean_;
    double min_;
    double p50_;
    double p99_;
    double max_;
    bool init_;

    dist_info_t() : mean_(-1), min_(-1), p50_(-1), p99_(-1), max_(-1), init_(false) {}
    dist_info_t(double mean, double min, double p50, double p99, double max)
        : mean_(mean), min_(min), p50_(p50), p99_(p99), max_(max), init_(true) {}

    void set_init() { init_ = true; }
    inline bool is_init() { return init_; }

    string DebugString(string title) {
        if (!init_) return "";
        string ret = title + " {";
        ret += "mean: " + std::to_string(mean_) + " ";
        ret += "min: " + std::to_string(min_) + " ";
        ret += "p50: " + std::to_string(p50_) + " ";
        ret += "p99: " + std::to_string(p99_) + " ";
        ret += "max: " + std::to_string(max_) + "}\n";
        return ret;
    }

    void transfer(std::ostringstream& oss, string& delimiter) {
        oss << "\t\t" << mean_ << delimiter << min_ << delimiter << p50_ << delimiter << p99_ << delimiter << max_
            << "\n";
    }

    void ToString(string* s) {
        Serializer::appendDouble(s, mean_);
        Serializer::appendDouble(s, min_);
        Serializer::appendDouble(s, p50_);
        Serializer::appendDouble(s, p99_);
        Serializer::appendDouble(s, max_);
    }

    void FromString(const string& s, size_t& pos) {
        Serializer::readDouble(s, pos, &mean_);
        Serializer::readDouble(s, pos, &min_);
        Serializer::readDouble(s, pos, &p50_);
        Serializer::readDouble(s, pos, &p99_);
        Serializer::readDouble(s, pos, &max_);
        init_ = true;
    }

    void merge(const dist_info_t& other) {
        if (!other.init_) return;
        if (!init_) {
            mean_ = other.mean_;
            min_ = other.min_;
            p50_ = other.p50_;
            p99_ = other.p99_;
            max_ = other.max_;
            init_ = true;
            return;
        }
        mean_ += other.mean_;
        min_ += other.min_;
        p50_ += other.p50_;
        p99_ += other.p99_;
        max_ += other.max_;
    }

    void settle_down(size_t sz) {
        if (!init_) return;
        mean_ /= sz;
        min_ /= sz;
        p50_ /= sz;
        p99_ /= sz;
        max_ /= sz;
    }

    void reset() {
        mean_ = -1;
        min_ = -1;
        p50_ = -1;
        p99_ = -1;
        max_ = -1;
        init_ = false;
    }
};

struct percentile_dist_info_t {
    double mean_, sz_;
    double p50_, p90_, p95_, p99_;
    double np50_, np90_, np95_, np99_;  // normalized value

    percentile_dist_info_t() : mean_(-1), sz_(-1), p50_(-1), p90_(-1), p95_(-1), p99_(-1), np50_(-1), np90_(-1), np95_(-1), np99_(-1) {}

    void collect(double mean, double sz, double p50, double p90, double p95, double p99, double np50, double np90, double np95, double np99) {
        mean_ = mean;
        sz_ = sz;
        p50_ = p50;
        p90_ = p90;
        p95_ = p95;
        p99_ = p99;
        np50_ = np50;
        np90_ = np90;
        np95_ = np95;
        np99_ = np99;
    }

    void ToString(string* s) {
        Serializer::appendDouble(s, mean_);
        Serializer::appendDouble(s, sz_);
        Serializer::appendDouble(s, p50_);
        Serializer::appendDouble(s, p90_);
        Serializer::appendDouble(s, p95_);
        Serializer::appendDouble(s, p99_);
        Serializer::appendDouble(s, np50_);
        Serializer::appendDouble(s, np90_);
        Serializer::appendDouble(s, np95_);
        Serializer::appendDouble(s, np99_);
    }

    void FromString(const string& s, size_t& pos) {
        Serializer::readDouble(s, pos, &mean_);
        Serializer::readDouble(s, pos, &sz_);
        Serializer::readDouble(s, pos, &p50_);
        Serializer::readDouble(s, pos, &p90_);
        Serializer::readDouble(s, pos, &p95_);
        Serializer::readDouble(s, pos, &p99_);
        Serializer::readDouble(s, pos, &np50_);
        Serializer::readDouble(s, pos, &np90_);
        Serializer::readDouble(s, pos, &np95_);
        Serializer::readDouble(s, pos, &np99_);
    }

    void reset() {
        mean_ = -1;
        sz_ = -1;
        p50_ = -1;
        p90_ = -1;
        p95_ = -1;
        p99_ = -1;
        np50_ = -1;
        np90_ = -1;
        np95_ = -1;
        np99_ = -1;
    }

    string DebugString(string title) {
        string ret = title + " {";
        ret += "mean: " + std::to_string(mean_) + " ";
        ret += " sz: " + std::to_string(mean_) + " ";
        ret += "{p50: " + std::to_string(p50_) + " ";
        ret += "p90: " + std::to_string(p90_) + " ";
        ret += "p95: " + std::to_string(p95_) + " ";
        ret += "p99: " + std::to_string(p99_) + "}, ";
        ret += "{np50: " + std::to_string(np50_) + " ";
        ret += "np90: " + std::to_string(np90_) + " ";
        ret += "np95: " + std::to_string(np95_) + " ";
        ret += "np99: " + std::to_string(np99_) + "}\n";
        return ret;
    }

    void transfer(std::ostringstream& oss, string& delimiter) {
        oss << mean_ << delimiter << sz_ << delimiter << p50_ << delimiter << p90_ << delimiter << p95_ << delimiter << p99_
            << delimiter << np50_ << delimiter << np90_ << delimiter << np95_ << delimiter << np99_ << "\n";
    }
};

class ModelUtil {
   public:
    template <typename T, typename = typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
    static dist_info_t calc_distribution(const std::vector<T>& v) {
        dist_info_t ret;
        if (v.empty()) return ret;
        ret.min_ = v[0];
        ret.max_ = v[0];
        double sum = 0;
        for (const T& t : v) {
            sum += t;
            ret.min_ = std::min(ret.min_, static_cast<double>(t));
            ret.max_ = std::max(ret.max_, static_cast<double>(t));
        }
        ret.mean_ = sum / v.size();
        std::vector<T> tmp = v;
        std::sort(tmp.begin(), tmp.end());
        ret.p50_ = Tool::get_percentile_data(tmp, 50);
        ret.p99_ = Tool::get_percentile_data(tmp, 99);
        ret.set_init();
        return ret;
    }

    template <typename T, typename = typename std::enable_if<std::is_arithmetic<T>::value, T>::type>
    static percentile_dist_info_t calc_percentile_distribution(const std::vector<T>& v) {
        percentile_dist_info_t ret;
        if (v.empty()) return ret;
        auto [mean, var] = Tool::calc_distribution(v);
        ret.mean_ = mean;
        std::vector<T> tmp = v;
        std::sort(tmp.begin(), tmp.end());
        ret.p50_ = Tool::get_percentile_data(tmp, 50);
        ret.p90_ = Tool::get_percentile_data(tmp, 90);
        ret.p95_ = Tool::get_percentile_data(tmp, 95);
        ret.p99_ = Tool::get_percentile_data(tmp, 99);
        return ret;
    }
};

typedef enum : uint16_t {
    OpModelType_EXPAND = 0,
    OpModelType_FILTER = 1,
    OpModelType_PROPERTY = 2,
    OpModelType_AGGREGATE_CACHE = 3,
    OpModelType_PROJECT = 4,
    OpModelType_AGGREGATE_COMPUTE = 5,
    OpModelType_FLOWCONTROL = 6,
    OpModelType_SCAN = 7,
    OpModelType_USELESS = 8,
    COUNT = 9
} OpModelType;

constexpr int NUM_OP_MODEL_TYPE = static_cast<int>(OpModelType::OpModelType_USELESS) - 1;

inline OpModelType OpType2OpModelType(OpType t) {
    switch (t) {
    case OpType_EXPAND:
    case OpType_EXPAND_INTO:
    case OpType_EXPAND_NN:
    case OpType_EXPAND_NC:
    case OpType_EXPAND_CN:
    case OpType_EXPAND_CC:
    case OpType_EXPAND_UL:
        return OpModelType_EXPAND;
    case OpType_FILTER:
        return OpModelType_FILTER;
    case OpType_PROPERTY:
        return OpModelType_PROPERTY;
    case OpType_AGGREGATE_LOCAL:
    case OpType_BARRIER_PROJECT_LOCAL:
        return OpModelType_AGGREGATE_CACHE;
    case OpType_PROJECT:
        return OpModelType_PROJECT;
    case OpType_AGGREGATE:
    case OpType_AGGREGATE_GLOBAL:
    case OpType_COUNT:
    case OpType_BARRIER_PROJECT:
    case OpType_BARRIER_PROJECT_GLOBAL:
        return OpModelType_AGGREGATE_COMPUTE;
    case OpType_BRANCH_AND:
    case OpType_BRANCH_OR:
    case OpType_BRANCH_NOT:
    case OpType_LOOP:
    case OpType_OPTIONAL_MATCH:
        return OpModelType_FLOWCONTROL;
    case OpType_VERTEX_SCAN:
    case OpType_INDEX_SCAN:
        return OpModelType_SCAN;
    case OpType_INIT:
    case OpType_END:
    case OpType_INDEX:
        return OpModelType_USELESS;
    default:
        LOG(ERROR) << "Unexpected op type: " << OpTypeStr[t];
        CHECK(false);
    }
}

}  // namespace MLModel
}  // namespace AGE
