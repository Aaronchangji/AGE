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
#include <deque>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/type.h"
#include "execution/message.h"
#include "execution/op_profiler.h"
#include "execution/physical_plan.h"
#include "execution/query_meta.h"
#include "model/util.h"
#include "operator/abstract_op.h"
#include "operator/op_type.h"
#include "util/tool.h"

namespace AGE {

using std::deque;
using std::shared_mutex;
using std::vector;

struct op_monitor_data_t {
    bool processed_;
    u16 op_type_, stage_idx_, op_idx_;

    bool src_col_from_compress_, dst_col_to_compress_;
    u8 compress_begin_idx_, compress_col_dst_;

    u64 item_size_, tuple_size_, line_size_;
    u64 proc_start_ts_, proc_time_;
    u64 que_start_ts_, que_time_;
    u64 net_start_ts_, net_time_;

    op_monitor_data_t() = default;
    op_monitor_data_t(u16 op_type, u8 compress_begin_idx, u16 stage_idx, u16 op_idx, bool processed,
                      bool src_col_from_compress, bool dst_col_to_compress, u8 compress_col_dst, u64 item_sz,
                      u64 tuple_sz, u64 line_sz, u64 proc_ts, u64 proc_time, u64 que_ts, u64 que_time, u64 net_ts,
                      u64 net_time)
        : processed_(processed),
          op_type_(op_type),
          stage_idx_(stage_idx),
          op_idx_(op_idx),
          src_col_from_compress_(src_col_from_compress),
          dst_col_to_compress_(dst_col_to_compress),
          compress_begin_idx_(compress_begin_idx),
          compress_col_dst_(compress_col_dst),
          item_size_(item_sz),
          tuple_size_(tuple_sz),
          line_size_(line_sz),
          proc_start_ts_(proc_ts),
          proc_time_(proc_time),
          que_start_ts_(que_ts),
          que_time_(que_time),
          net_start_ts_(net_ts),
          net_time_(net_time) {}
    op_monitor_data_t(u16 op_type, u8 compress_begin_idx, u16 stage_idx, u16 op_idx, SingleProcStat& stat)
        : op_monitor_data_t(op_type, compress_begin_idx, stage_idx, op_idx, stat.getIfProcessed(),
                            stat.getIfSrcFromCompressed(), stat.getIfDstToCompressed(), stat.getCompressedDst(),
                            stat.getItemSize(), stat.getTupleSize(), stat.getLineSize(), stat.getProcTs(),
                            stat.getProcTime(), stat.getQueTs(), stat.getQueTime(), stat.getNetTs(),
                            stat.getNetTime()) {}

    OpId GetOpId() const { return OpId(stage_idx_, op_idx_); }

    string DebugString() const {
        std::stringstream ss;
        ss.precision(3);
        ss << "(" << stage_idx_ << ", " << op_idx_ << "): " << OpType_DebugString(static_cast<OpType>(op_type_)) << " ";
        if (!processed_) {
            // Only I/O statistics
            if (que_start_ts_) ss << "n:[" << (net_start_ts_ / 1e6) << "ms, " << (net_time_ / 1e3) << "us], ";
            ss << "q:[" << (que_start_ts_ / 1e6) << "ms, " << (que_time_ / 1e3) << "us]";
        } else {
            if (que_start_ts_) ss << "n:[" << (net_start_ts_ / 1e6) << "ms, " << (net_time_ / 1e3) << "us], ";
            ss << "q:[" << (que_start_ts_ / 1e6) << "ms, " << (que_time_ / 1e3) << "us], ";
            ss << "p:[" << (proc_start_ts_ / 1e6) << "ms, " << (proc_time_ / 1e3) << "us]";
            ss << " | ";
            ss << "comp_beg:" << static_cast<u16>(compress_begin_idx_)
               << " comp_dst:" << static_cast<u16>(compress_col_dst_) << " src:" << src_col_from_compress_
               << " dst:" << dst_col_to_compress_;
            ss << " | ";
            ss << "item: " << item_size_ << " "
               << "tuple: " << tuple_size_ << " "
               << "line: " << line_size_;
        }
        return ss.str();
    }
};

struct thpt_result_data_t {
    u64 latency_;  // us
    u64 batchwidth_;
    int query_template_idx_;

    thpt_result_data_t() = default;
    thpt_result_data_t(int query_template_idx, u64 lat, u64 width = 0)
        : latency_(lat), batchwidth_(width), query_template_idx_(query_template_idx) {}

    bool operator<(const thpt_result_data_t& rhs) { return latency_ < rhs.latency_; }
    static bool order_by_batchwidth(const thpt_result_data_t& lhs, const thpt_result_data_t& rhs) {
        return lhs.batchwidth_ < rhs.batchwidth_;
    }
};

struct thpt_input_data_t {
    bool is_heavy_;

    thpt_input_data_t() : is_heavy_(false) {}
    explicit thpt_input_data_t(bool is_heavy) : is_heavy_(is_heavy) {}
};

struct inter_data_info_t {
    u16 inter_stage_type_;
    u64 size_;
    inter_data_info_t() = default;
    explicit inter_data_info_t(u16 inter_stage_type, u64 size) : inter_stage_type_(inter_stage_type), size_(size) {}
};

template <typename T>
class Monitor {
   public:
    Monitor() : window_duration_(1E9), last_print_time_(Tool::getTimeNs()), enable_(true) {}
    explicit Monitor(uint64_t window_duration)
        : window_duration_(window_duration), last_print_time_(Tool::getTimeNs()), enable_(true) {}

    void record_data(vector<T>& data) {
        if (!enable_) return;
        auto ts = Tool::getTimeNs();
        vector<TimestampData<T>> ts_data;
        for (auto& item : data) {
            ts_data.emplace_back(ts, item);
        }
        add_data_bulk(ts_data);
    }

    void record_single_data(T& data) {
        if (!enable_) return;
        auto ts = Tool::getTimeNs();
        TimestampData<T> ts_data(ts, data);
        add_data(std::move(ts_data));
    }

    void get_data(vector<T>& data, u64 customize_window = 0) {
        std::shared_lock<shared_mutex> lock(mutex_);
        u64 tw = customize_window == 0 ? window_duration_ : customize_window;
        uint64_t window_start = Tool::getTimeNs() - tw;

        auto itr = data_queue_.rbegin();
        while (itr != data_queue_.rend()) {
            if (itr->timestamp_ < window_start) break;
            data.emplace_back(itr->data_);
            itr++;
        }
    }

    void get_all_data(vector<T>& data) {
        std::shared_lock<shared_mutex> lock(mutex_);
        for (const auto& item : data_queue_) {
            data.emplace_back(item.data_);
        }
    }

    size_t get_data_size(u64 customize_window = 0) {
        std::shared_lock<shared_mutex> lock(mutex_);
        u64 tw = customize_window == 0 ? window_duration_ : customize_window;
        uint64_t window_start = Tool::getTimeNs() - tw;
        size_t ret = 0;
        for (const auto& item : data_queue_) {
            if (item.timestamp_ < window_start) break;
            ret++;
        }
        return ret;
    }

    size_t get_all_data_size() {
        std::shared_lock<shared_mutex> lock(mutex_);
        return data_queue_.size();
    }

    uint64_t get_print_time() { return last_print_time_; }
    void set_print_time(uint64_t t) { last_print_time_ = t; }  // only one thread will invoke this

    uint64_t get_data_duration() {
        std::shared_lock<shared_mutex> lock(mutex_);
        CHECK_NE(data_queue_.size(), static_cast<u64>(0));
        return data_queue_.back().timestamp_ - data_queue_.front().timestamp_;
    }

    inline uint64_t get_last_data_ts() {
        CHECK_NE(data_queue_.size(), static_cast<u64>(0));
        return data_queue_.back().timestamp_;
    }

    void clear_data() {
        std::lock_guard<shared_mutex> lock(mutex_);
        data_queue_.clear();
        enable();
    }

    inline void disable() { enable_ = false; }
    inline void enable() { enable_ = true; }

   protected:
    template <typename DataType>
    class TimestampData {
       public:
        TimestampData(u64 ts, DataType data) : timestamp_(ts), data_(data) {}

        bool operator<(const TimestampData& rhs) const { return timestamp_ < rhs.timestamp_; }

        u64 timestamp_;  // ns
        DataType data_;
    };

    void add_data(TimestampData<T>&& data) {
        std::lock_guard<shared_mutex> lock(mutex_);
        data_queue_.emplace_back(std::move(data));
    }

    void add_data_bulk(vector<TimestampData<T>>& data) {
        std::lock_guard<shared_mutex> lock(mutex_);
        data_queue_.insert(data_queue_.end(), data.begin(), data.end());
    }

    void prune_data() {
        uint64_t window_start = Tool::getTimeNs() - window_duration_;
        TimestampData<T> threshold = {window_start, T()};

        auto prune_end = std::lower_bound(data_queue_.begin(), data_queue_.end(), threshold);
        data_queue_.erase(data_queue_.begin(), prune_end);
    }

    uint64_t window_duration_;
    deque<TimestampData<T>> data_queue_;
    shared_mutex mutex_;
    uint64_t last_print_time_;
    bool enable_;
};

class OpMonitor : public Monitor<op_monitor_data_t> {
   public:
    explicit OpMonitor(u64 window_duration) : Monitor(window_duration) {}

    void DumpCurrentSplitPerfStat(const Message& msg) {
        const auto& collected_stat = msg.header.opPerfInfos.back();
        vector<op_monitor_data_t> monitor_data;

        AbstractOp* op = nullptr;
        for (u64 i = 0; i < collected_stat.size(); i++) {
            const OpPerfInfo& info = collected_stat[i];
            OpId id = info.op_id_;
            SingleProcStat stat = info.op_proc_stat_;

            op = msg.plan->getStage(id.stage_id_)->ops_[id.in_stage_op_id_];
            OpType type = op->type;
            u8 comp_beg_idx = op->compressBeginIdx;

            // Update Profiler, the current, not-yet-processed op will be ignored
            if (msg.header.currentStageIdx == id.stage_id_ && msg.header.currentOpIdx == id.in_stage_op_id_ &&
                i == collected_stat.size() - 1)
                continue;
            // Update Monitor.
            monitor_data.emplace_back(type, comp_beg_idx, id.stage_id_, id.in_stage_op_id_, stat);
        }
        record_data(monitor_data);
    }

    void DisplayTimeline() {
        vector<op_monitor_data_t> data;
        get_data(data);
        if (data.size() == 0) return;
        std::sort(data.begin(), data.end(), [](const auto& lhs, const auto& rhs) {
            if (lhs.que_start_ts_ != rhs.que_start_ts_) return lhs.que_start_ts_ < rhs.que_start_ts_;
            return lhs.GetOpId() < rhs.GetOpId();
        });
        for (const auto& item : data) LOG(INFO) << item.DebugString();
    }

    void DisplayProcIORatio(const shared_ptr<PhysicalPlan>& plan, const QueryMeta& meta) {
        u64 total_proc_time = 0, total_que_time = 0, total_net_time = 0;
        for (const auto& item : data_queue_) {
            const auto& data = item.data_;
            total_proc_time += data.proc_time_;
            total_que_time += data.que_time_;
            total_net_time += data.net_time_;
        }
        LOG(INFO) << "qid " << plan->qid << ", temp id " << plan->query_template_idx << ": "
                  << "wall time " << (Tool::getTimeMs() - meta.proc_time_ / 1e3) << "ms "
                  << "| msg agg stat ["
                  << "proc:" << total_proc_time / 1e6 << "ms, "
                  << "netw:" << total_net_time / 1e6 << "ms, "
                  << "que:" << total_que_time / 1e6 << "ms] "
                  << "| proc/IO ratio " << (static_cast<double>(total_proc_time) / (total_que_time + total_net_time));
    }

    void DisplayOpStatsAndProcIORatio(const shared_ptr<PhysicalPlan>& plan, const QueryMeta& meta) {
        struct PrintedOpStat {
            OpType op_type_;
            u64 item_cnt_;
            PrintedOpStat() = default;
            PrintedOpStat(OpType type, u64 cnt) : op_type_(type), item_cnt_(cnt) {}
            string DebugString() const { return OpType_DebugString(op_type_) + ":" + std::to_string(item_cnt_); }
        };

        std::map<OpId, PrintedOpStat> input_size_map;
        u64 total_proc_time = 0, total_que_time = 0, total_net_time = 0;
        for (const auto& item : data_queue_) {
            const auto& data = item.data_;
            // Get Proc / Queue ratio for this query
            total_proc_time += data.proc_time_;
            total_que_time += data.que_time_;
            total_net_time += data.net_time_;

            // Get input size for each processed op
            if (!data.processed_ || !data.item_size_) continue;
            OpId id(data.stage_idx_, data.op_idx_);
            if (!input_size_map.count(id)) {
                input_size_map.insert(
                    std::make_pair(id, PrintedOpStat(static_cast<OpType>(data.op_type_), data.item_size_)));
            } else {
                input_size_map[id].item_cnt_ += data.item_size_;
            }
        }

        LOG(INFO) << "qid " << plan->qid << ", template id " << plan->query_template_idx << ": "
                  << "proc wall time " << (Tool::getTimeMs() - meta.proc_time_ / 1e3) << "ms "
                  << "proc/IO ratio " << (static_cast<double>(total_proc_time) / (total_que_time + total_net_time));

        for (const auto& iter : input_size_map) {
            LOG(INFO) << "qid " << plan->qid << ", " << iter.first.DebugString() << " " << iter.second.DebugString();
        }
    }

    /**
     * @brief Extract the optype, input, output size, and queuing time, and net time
     */
    void ExtractRLModelData(MLModel::dist_info_t& queue_time_dist, MLModel::dist_info_t& net_time_dist,
                            vector<MLModel::dist_info_t>& op_input_size_dist, uint64_t duration = 0) {
        vector<op_monitor_data_t> tw_data;
        if (duration == 0) {
            get_all_data(tw_data);
        } else {
            get_data(tw_data, duration);
        }

        vector<vector<u64>> op_input_data;
        vector<u64> queue_times, net_times;
        op_input_data.resize(MLModel::NUM_OP_MODEL_TYPE);

        for (auto& item : tw_data) {
            if (item.que_time_ != 0) queue_times.emplace_back(item.que_time_ / 1E3);
            if (item.net_time_ != 0) net_times.emplace_back(item.net_time_ / 1E3);

            int op_idx = MLModel::OpType2OpModelType(static_cast<OpType>(item.op_type_));
            if (op_idx == MLModel::OpModelType_USELESS || op_idx == MLModel::OpModelType_SCAN) continue;
            CHECK(op_idx >= 0 && op_idx < MLModel::NUM_OP_MODEL_TYPE);
            op_input_data[op_idx].emplace_back(item.tuple_size_);
        }

        dump_cdf(queue_times, "Queue Latency", "us");
        dump_cdf(net_times, "Network Latency", "us");

        queue_time_dist = MLModel::ModelUtil::calc_distribution(queue_times);
        net_time_dist = MLModel::ModelUtil::calc_distribution(net_times);
        for (size_t i = 0; i < op_input_data.size(); i++) {
            op_input_size_dist[i] = MLModel::ModelUtil::calc_distribution(op_input_data[i]);
        }
    }

    template<typename T>
    void dump_cdf(vector<T>& data, string title, string unit) {
        std::sort(data.begin(), data.end());
        string s = title + "\n";
        for (int i = 0; i < 20; i++) {
            int idx = i * 5;
            if (idx == 0) { idx = 1; }
            double sz_ = Tool::get_percentile_data(data, idx);
            s += "\tP" + to_string(idx) + ":\t" + to_string(sz_) + " " + unit + "\n";
        }
        for (int i = 96; i < 100; i++) {
            double sz_ = Tool::get_percentile_data(data, i);
            s += "\tP" + to_string(i) + ":\t" + to_string(sz_) + " " + unit +  + "\n";
        }
        LOG(INFO) << s;
    }

    void CleanData() { prune_data(); }
};

}  // namespace AGE
