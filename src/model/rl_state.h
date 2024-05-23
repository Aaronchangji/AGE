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
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "base/serializer.h"
#include "execution/monitor.h"
#include "model/util.h"
#include "operator/op_type.h"
#include "util/config.h"

#pragma once

namespace AGE {
namespace MLModel {

using std::vector;

class RLState {
   public:
    RLState() : input_qps_(.0), heavy_ratio_(.0), jct_(.0) { input_size_dists_.resize(NUM_OP_MODEL_TYPE); }

    void collect_op_metrics(std::shared_ptr<OpMonitor> op_monitor, bool is_all = false) {
        if (is_all) {
            op_monitor->ExtractRLModelData(queue_lat_dist_, net_dist_, input_size_dists_);
        } else {
            op_monitor->ExtractRLModelData(queue_lat_dist_, net_dist_, input_size_dists_,
                                           1E9 * Config::GetInstance()->model_data_collection_duration_sec_);
        }
    }

    void set_input_qps(double input_qps) { input_qps_ = input_qps; }
    void set_heavy_ratio(double heavy_ratio) { heavy_ratio_ = heavy_ratio; }
    void set_jct(double jct) { jct_ = jct; }
    void set_unfinished_queries(int num_unfinished_queries) { num_unfinished_queries_ = num_unfinished_queries; }
    void set_env(int m, int t) {
        num_machines_ = m;
        num_threads_ = t;
    }
    void set_retry() { retry_ = 1; }

    void set_inter_data(vector<uint16_t> inter_data_types, vector<percentile_dist_info_t> inter_data_dists) {
        inter_data_types_ = inter_data_types;
        inter_data_dists_ = inter_data_dists;
    }

    string print(string& delimiter) {
        std::ostringstream oss;
        oss << input_qps_ << delimiter << jct_ << delimiter;
        oss << std::fixed << std::setprecision(4) << heavy_ratio_ << delimiter << num_unfinished_queries_ << "\n";
        queue_lat_dist_.transfer(oss, delimiter);
        net_dist_.transfer(oss, delimiter);
        for (auto& i : input_size_dists_) i.transfer(oss, delimiter);
        for (size_t i = 0; i < inter_data_types_.size(); i++) {
            oss << "\t\t" << inter_data_types_[i] << delimiter;
            inter_data_dists_[i].transfer(oss, delimiter);
        }
        oss << std::to_string(num_machines_) << delimiter << std::to_string(num_threads_) << "\n";
        return oss.str();
    }

    void ToString(string* s) {
        if (!is_init()) return;
        Serializer::appendDouble(s, input_qps_);
        Serializer::appendDouble(s, heavy_ratio_);
        Serializer::appendDouble(s, jct_);
        queue_lat_dist_.ToString(s);
        net_dist_.ToString(s);
        Serializer::appendU64(s, input_size_dists_.size());
        for (auto& i : input_size_dists_) i.ToString(s);
        Serializer::appendU64(s, num_unfinished_queries_);
        Serializer::appendU64(s, inter_data_types_.size());
        for (size_t i = 0; i < inter_data_types_.size(); i++) {
            Serializer::appendU16(s, inter_data_types_[i]);
            inter_data_dists_[i].ToString(s);
        }
        Serializer::appendU16(s, num_machines_);
        Serializer::appendU16(s, num_threads_);
        Serializer::appendU8(s, retry_);
    }

    void FromString(const string& s, size_t& pos) {
        Serializer::readDouble(s, pos, &input_qps_);
        Serializer::readDouble(s, pos, &heavy_ratio_);
        Serializer::readDouble(s, pos, &jct_);
        queue_lat_dist_.FromString(s, pos);
        net_dist_.FromString(s, pos);
        size_t len;
        Serializer::readU64(s, pos, &len);
        input_size_dists_.resize(len);
        for (auto& i : input_size_dists_) i.FromString(s, pos);
        Serializer::readU64(s, pos, &num_unfinished_queries_);
        Serializer::readU64(s, pos, &len);
        inter_data_types_.resize(len);
        inter_data_dists_.resize(len);
        for (size_t i = 0; i < len; i++) {
            Serializer::readU16(s, pos, &inter_data_types_[i]);
            inter_data_dists_[i].FromString(s, pos);
        }
        Serializer::readU16(s, pos, &num_machines_);
        Serializer::readU16(s, pos, &num_threads_);
        Serializer::readU8(s, pos, &retry_);
    }

    void reset() {
        input_qps_ = 0;
        heavy_ratio_ = 0;
        jct_ = 0;
        queue_lat_dist_.reset();
        net_dist_.reset();
        for (auto& i : input_size_dists_) i.reset();
        num_unfinished_queries_ = 0;
        inter_data_types_.clear();
        inter_data_dists_.clear();
        retry_ = 0;
    }

    void merge(const RLState& other) {
        input_qps_ += other.input_qps_;
        heavy_ratio_ += other.heavy_ratio_;
        jct_ += other.jct_;
        queue_lat_dist_.merge(other.queue_lat_dist_);
        net_dist_.merge(other.net_dist_);
        for (size_t i = 0; i < input_size_dists_.size(); i++) input_size_dists_[i].merge(other.input_size_dists_[i]);
        num_unfinished_queries_ += other.num_unfinished_queries_;
    }

    void settle_down(size_t sz, size_t num_input_queries = 0) {
        if (num_input_queries == 0) {
            heavy_ratio_ /= sz;
            jct_ /= sz;
        } else {
            LOG(INFO) << "There are " << heavy_ratio_ << " heavy queries";
            heavy_ratio_ = 100.0 * heavy_ratio_ / num_input_queries;
            jct_ = jct_ / num_input_queries;
        }
        queue_lat_dist_.settle_down(sz);
        net_dist_.settle_down(sz);
        for (auto& i : input_size_dists_) i.settle_down(sz);
    }

    // Use the init_ of queue_lat_dist_ to indicate whether the state is usable
    inline bool is_init() { return queue_lat_dist_.init_; }

   private:
    double input_qps_;
    double heavy_ratio_;
    double jct_;
    dist_info_t queue_lat_dist_;
    dist_info_t net_dist_;
    vector<dist_info_t> input_size_dists_;
    uint64_t num_unfinished_queries_{0};
    vector<uint16_t> inter_data_types_;
    vector<percentile_dist_info_t> inter_data_dists_;
    uint16_t num_machines_;
    uint16_t num_threads_;
    uint8_t retry_{0};
};

}  // namespace MLModel
}  // namespace AGE
