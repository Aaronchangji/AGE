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

#include <algorithm>
#include <cstddef>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "base/item.h"
#include "base/row.h"
#include "base/serializer.h"
#include "util/tool.h"

#pragma once

namespace AGE {

class GeneralHistogram {
   public:
    struct HistElement {
        Item key, value;
        HistElement() = default;
        explicit HistElement(const Item& v, const Item* k = nullptr) : value(v) { key = k == nullptr ? v : *k; }
        bool operator<(const HistElement& other) const { return key < other.key; }
    };

    explicit GeneralHistogram(u16 granularity = 100) : granularity_(granularity) {}

    explicit GeneralHistogram(vector<HistElement>&& data, u16 granularity = 100) : granularity_(granularity) {
        data_.resize(granularity);
        size_ = data.size();
        std::sort(data.begin(), data.end());
        size_t size = data.size();
        for (size_t percent = 0; percent < granularity_; percent++) {
            size_t index = static_cast<size_t>(percent * size / granularity_);
            data_[percent] = data[index];
        }
    }

    std::pair<float, float> get_percentile_by_value(const Item& value) const {
        float low_percent = std::lower_bound(data_.begin(), data_.end(), HistElement(value)) - data_.begin();
        float high_percent = std::upper_bound(data_.begin(), data_.end(), HistElement(value)) - data_.begin();
        CHECK_LE(low_percent, high_percent);
        return std::make_pair(low_percent / granularity_, high_percent / granularity_);
    }

    u16 get_granularity() const { return granularity_; }

    u64 get_size() const { return size_; }

    string DebugString() const {
        std::stringstream ss;
        ss << "Histogram: (size: " << size_ << ", granularity: " + std::to_string(granularity_) + ")\n";
        for (size_t i = 0; i < granularity_; i++) {
            float percent = static_cast<float>(i * 100) / granularity_;
            ss << std::setw(4) << std::setprecision(4) << percent << "%:\tvalue: " << data_[i].key.DebugString()
               << "\t\tkey: " << data_[i].value.DebugString() << "\n";
        }
        return ss.str();
    }

    void ToData(Row& tar) {
        tar.emplace_back(Item(T_INTEGER, granularity_));
        tar.emplace_back(Item(T_INTEGER, static_cast<i64>(size_)));
        for (auto& item : data_) {
            tar.emplace_back(item.key);
            tar.emplace_back(item.value);
        }
    }

    void FromData(Row& src) {
        granularity_ = src[0].integerVal;
        size_ = src[1].integerVal;
        data_.resize(granularity_);
        for (size_t i = 0; i < granularity_; i++) {
            data_[i].key = std::move(src[2 * i + 2]);
            data_[i].value = std::move(src[2 * i + 3]);
        }
    }

    void ToString(string* s) {
        Serializer::appendU16(s, granularity_);
        Serializer::appendU64(s, size_);
        for (auto& item : data_) {
            item.key.ToString(s);
            item.value.ToString(s);
        }
    }

    void FromString(const string& s, size_t& pos) {
        granularity_ = Serializer::readU16(s, pos);
        size_ = Serializer::readU64(s, pos);
        data_.resize(granularity_);
        for (size_t i = 0; i < granularity_; i++) {
            data_[i].key.FromString(s, pos);
            data_[i].value.FromString(s, pos);
        }
    }

   private:
    vector<HistElement> data_;
    u16 granularity_;
    u64 size_;
};

}  // namespace AGE
