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
#include <tbb/concurrent_hash_map.h>
#include <algorithm>
#include <limits>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include "base/serializer.h"
#include "base/type.h"
#include "glog/logging.h"
#include "util/tool.h"

namespace AGE {

using std::to_string;

struct OpId {
    u16 stage_id_;
    u16 in_stage_op_id_;

    explicit OpId(u16 stage_id = -1, u16 in_stage_op_id = -1) : stage_id_(stage_id), in_stage_op_id_(in_stage_op_id) {}
    OpId(const OpId& other) = default;

    inline bool operator<(const OpId& other) const {
        if (stage_id_ != other.stage_id_) return stage_id_ < other.stage_id_;
        return in_stage_op_id_ < other.in_stage_op_id_;
    }

    inline bool operator==(const OpId& other) const {
        return (stage_id_ == other.stage_id_) && (in_stage_op_id_ == other.in_stage_op_id_);
    }

    u32 value() const {
        u32 ret = 0;
        ret |= (stage_id_ << 16);
        ret |= in_stage_op_id_;
        return ret;
    }

    void ToString(string* s) const {
        Serializer::appendU16(s, stage_id_);
        Serializer::appendU16(s, in_stage_op_id_);
    }

    void FromString(const string& s, size_t& pos) {
        Serializer::readU16(s, pos, &stage_id_);
        Serializer::readU16(s, pos, &in_stage_op_id_);
    }

    string DebugString() const {
        string ret = "OpId: (";
        ret += to_string(stage_id_) + ", ";
        ret += to_string(in_stage_op_id_) + ")";
        return ret;
    }
};

struct OpIdCompare {
    static size_t hash(const OpId& id) {
        u32 value = id.value();
        return Math::MurmurHash64_x64(&value, sizeof(u32));
    }

    size_t operator()(const OpId& id) const { return hash(id); }

    static bool equal(const OpId& lhs, const OpId& rhs) { return lhs == rhs; }
};

// start_time and end_time counted in Nano (i.e., 10^-9) seconds
struct SingleProcStat {
    bool processed_;

    bool src_col_from_compress_, dst_col_to_compress_;  // 0: normal columns; 1: compressed columns
    u8 compress_col_dst_;                               // 0: decompressed; 1: keep compressed 2; discarded

    u64 item_size_,           // # total items
        logical_tuple_size_,  // # logical lines (>= # physical lines for compressed lines)
        physical_line_size_;  // # physical lines (== data.size())

    u64 query_start_ts_,             // starting timestamp for this query
        proc_start_ts_, proc_time_,  // offset timestamp for the current processing
        que_start_ts_, que_time_,    // offset timestamp for the current queueing
        net_start_ts_, net_time_;    // offset timestamp for the network traffic

    using ColStat = std::tuple<bool, bool, u8>;
    using DataStat = std::tuple<u64, u64, u64>;

    explicit SingleProcStat(u64 query_start_ts = 0)
        : processed_(false),
          src_col_from_compress_(false),
          dst_col_to_compress_(false),
          compress_col_dst_(0),
          item_size_(0),
          logical_tuple_size_(0),
          physical_line_size_(0),
          query_start_ts_(query_start_ts),
          proc_start_ts_(0),
          proc_time_(0),
          que_start_ts_(0),
          que_time_(0),
          net_start_ts_(0),
          net_time_(0) {}

    void setColStat(ColStat col_stat) {
        src_col_from_compress_ = std::get<0>(col_stat);
        dst_col_to_compress_ = std::get<1>(col_stat);
        compress_col_dst_ = std::get<2>(col_stat);
    }
    void setDataStat(DataStat data_stat) {
        item_size_ = std::get<0>(data_stat);
        logical_tuple_size_ = std::get<1>(data_stat);
        physical_line_size_ = std::get<2>(data_stat);
    }
    void setProcStartTime(u64 start_time) {
        u64 now_ts = Tool::getDateNs();
        if (query_start_ts_ && now_ts > query_start_ts_) proc_start_ts_ = now_ts - query_start_ts_;
        proc_time_ = start_time;
    }
    void setQueStartTime(u64 start_time) {
        u64 now_ts = Tool::getDateNs();
        if (query_start_ts_ && now_ts > query_start_ts_) que_start_ts_ = now_ts - query_start_ts_;
        que_time_ = start_time;
    }
    void setProcEndTime(u64 end_time) {
        processed_ = true;
        proc_time_ = end_time - proc_time_;
    }
    void setQueEndTime(u64 end_time) { que_time_ = end_time - que_time_; }
    void setNetStartTime(u64 start_time) {
        if (query_start_ts_ && start_time > query_start_ts_) net_start_ts_ = start_time - query_start_ts_;
        net_time_ = start_time;
    }
    void setNetEndTime(u64 end_time) { net_time_ = (net_time_ && end_time > net_time_) ? end_time - net_time_ : 0; }

    bool getIfProcessed() const { return processed_; }
    bool getIfSrcFromCompressed() const { return src_col_from_compress_; }
    bool getIfDstToCompressed() const { return dst_col_to_compress_; }
    u8 getCompressedDst() const { return compress_col_dst_; }

    u64 getProcTime() const { return proc_time_; }
    u64 getProcTs() const { return proc_start_ts_; }
    u64 getQueTime() const { return que_time_; }
    u64 getQueTs() const { return que_start_ts_; }
    u64 getNetTime() const { return net_time_; }
    u64 getNetTs() const { return net_start_ts_; }

    u64 getItemSize() const { return item_size_; }
    u64 getTupleSize() const { return logical_tuple_size_; }
    u64 getLineSize() const { return physical_line_size_; }
    u64 getNewLineSize() const {
        CHECK_GE(logical_tuple_size_, physical_line_size_);
        return logical_tuple_size_ - physical_line_size_;
    }

    void ToString(string* s) const {
        Serializer::appendBool(s, processed_);
        Serializer::appendBool(s, src_col_from_compress_);
        Serializer::appendBool(s, dst_col_to_compress_);
        Serializer::appendU8(s, compress_col_dst_);
        Serializer::appendU64(s, item_size_);
        Serializer::appendU64(s, logical_tuple_size_);
        Serializer::appendU64(s, physical_line_size_);
        Serializer::appendU64(s, query_start_ts_);
        Serializer::appendU64(s, proc_start_ts_);
        Serializer::appendU64(s, proc_time_);
        Serializer::appendU64(s, que_start_ts_);
        Serializer::appendU64(s, que_time_);
        Serializer::appendU64(s, net_start_ts_);
        Serializer::appendU64(s, net_time_);
    }
    void FromString(const string& s, size_t& pos) {
        Serializer::readBool(s, pos, &processed_);
        Serializer::readBool(s, pos, &src_col_from_compress_);
        Serializer::readBool(s, pos, &dst_col_to_compress_);
        Serializer::readU8(s, pos, &compress_col_dst_);
        Serializer::readU64(s, pos, &item_size_);
        Serializer::readU64(s, pos, &logical_tuple_size_);
        Serializer::readU64(s, pos, &physical_line_size_);
        Serializer::readU64(s, pos, &query_start_ts_);
        Serializer::readU64(s, pos, &proc_start_ts_);
        Serializer::readU64(s, pos, &proc_time_);
        Serializer::readU64(s, pos, &que_start_ts_);
        Serializer::readU64(s, pos, &que_time_);
        Serializer::readU64(s, pos, &net_start_ts_);
        Serializer::readU64(s, pos, &net_time_);
    }

    string DebugString() const {
        std::stringstream ss;
        ss.precision(3);
        if (!processed_) {
            ss << "proc:{";
            ss << "q:[" << (que_start_ts_ / 1e6) << "ms, " << (getQueTime() / 1e3) << "us]}";
        } else {
            ss << "op:{";
            ss << "src:" << src_col_from_compress_ << " dst:" << dst_col_to_compress_
               << " com:" << static_cast<u32>(compress_col_dst_) << "} ";

            ss << "proc:{";
            ss << "q:[" << (que_start_ts_ / 1e6) << "ms, " << (getQueTime() / 1e3) << "us], ";
            ss << "n:[" << (net_start_ts_ / 1e6) << "ms, " << (getNetTime() / 1e3) << "us], ";
            ss << "p:[" << (proc_start_ts_ / 1e6) << "ms, " << (getProcTime() / 1e3) << "us]} ";

            ss << "data:{";
            ss << "item: " << getItemSize() << " "
               << "tuple: " << getTupleSize() << " "
               << "line: " << getLineSize() << "}";
        }
        return ss.str();
    }
};

struct OpPerfStat {
    u64 tuple_size_, msg_cnt_;
    u64 min_proc_time_, max_proc_time_, avg_proc_time_;
    u64 min_que_time_, max_que_time_, avg_que_time_;
    double min_proc_rate_, max_proc_rate_, avg_proc_rate_;

    OpPerfStat() {
        tuple_size_ = msg_cnt_ = 0;
        min_proc_time_ = max_proc_time_ = avg_proc_time_ = 0;
        min_proc_rate_ = max_proc_rate_ = avg_proc_rate_ = 0;
        min_que_time_ = max_que_time_ = avg_que_time_ = 0;
    }

    void ToString(string* s) const {
        Serializer::appendU64(s, tuple_size_);
        Serializer::appendU64(s, msg_cnt_);
        Serializer::appendU64(s, min_proc_time_);
        Serializer::appendU64(s, max_proc_time_);
        Serializer::appendU64(s, avg_proc_time_);
        Serializer::appendU64(s, min_que_time_);
        Serializer::appendU64(s, max_que_time_);
        Serializer::appendU64(s, avg_que_time_);
        Serializer::appendVar(s, min_proc_rate_);
        Serializer::appendVar(s, max_proc_rate_);
        Serializer::appendVar(s, avg_proc_rate_);
    }

    void FromString(const string& s, size_t& pos) {
        Serializer::readU64(s, pos, &tuple_size_);
        Serializer::readU64(s, pos, &msg_cnt_);
        Serializer::readU64(s, pos, &min_proc_time_);
        Serializer::readU64(s, pos, &max_proc_time_);
        Serializer::readU64(s, pos, &avg_proc_time_);
        Serializer::readU64(s, pos, &min_que_time_);
        Serializer::readU64(s, pos, &max_que_time_);
        Serializer::readU64(s, pos, &avg_que_time_);
        Serializer::readVar(s, pos, &min_proc_rate_);
        Serializer::readVar(s, pos, &max_proc_rate_);
        Serializer::readVar(s, pos, &avg_proc_rate_);
    }

    string DebugString() const {
        string ret = "{";
        ret += "\n\tmsg count: " + to_string(msg_cnt_) + ", tuple count: " + to_string(tuple_size_);
        ret += "\n\tqueue time        (ms): [" + to_string(min_que_time_ / 1e6) + ", " +
               to_string(avg_que_time_ / 1e6) + ", " + to_string(max_que_time_ / 1e6) + "]";
        ret += "\n\tprocess time      (ms): [" + to_string(min_proc_time_ / 1e6) + ", " +
               to_string(avg_proc_time_ / 1e6) + ", " + to_string(max_proc_time_ / 1e6) + "]";
        ret += "\n\tprocess rate (tuple/s): [" + to_string(min_proc_rate_) + ", " + to_string(avg_proc_rate_) + ", " +
               to_string(max_proc_rate_) + "]";
        ret += "\n}\n";
        return ret;
    }

    void update(const SingleProcStat& msg_stat) {
        updateQueTime(msg_stat);
        if (!msg_stat.processed_) return;
        updateProcTime(msg_stat);
        updateProcRate(msg_stat);
        updateCnt(msg_stat);
    }

   private:
    void updateCnt(const SingleProcStat& msg_stat) {
        tuple_size_ += msg_stat.logical_tuple_size_;
        msg_cnt_++;
    }
    void updateQueTime(const SingleProcStat& msg_stat) {
        u64 duration = msg_stat.getQueTime();
        if (!min_que_time_) min_que_time_ = std::numeric_limits<u64>::max();
        min_que_time_ = std::min(min_que_time_, duration);
        max_que_time_ = std::max(max_que_time_, duration);
        avg_que_time_ = (avg_que_time_ * msg_cnt_ + duration) / (msg_cnt_ + 1);
    }
    void updateProcTime(const SingleProcStat& msg_stat) {
        u64 duration = msg_stat.getProcTime();
        if (!min_proc_time_) min_proc_time_ = std::numeric_limits<u64>::max();
        min_proc_time_ = std::min(min_proc_time_, duration);
        max_proc_time_ = std::max(max_proc_time_, duration);
        avg_proc_time_ = (avg_proc_time_ * msg_cnt_ + duration) / (msg_cnt_ + 1);
    }
    void updateProcRate(const SingleProcStat& msg_stat) {
        double process_rate = static_cast<double>(msg_stat.logical_tuple_size_) / (msg_stat.getProcTime() / 1e9);
        if (!min_proc_rate_) min_proc_rate_ = std::numeric_limits<double>::max();
        min_proc_rate_ = std::min(min_proc_rate_, process_rate);
        max_proc_rate_ = std::max(max_proc_rate_, process_rate);
        avg_proc_rate_ = (avg_proc_rate_ * msg_cnt_ + process_rate) / (msg_cnt_ + 1);
    }
};

using PerfMapType = tbb::concurrent_hash_map<OpId, OpPerfStat, OpIdCompare>;

struct OpPerfInfo {
    OpId op_id_;                   // which operator (i.e., <stage_id, in_stage_op_id>)
    SingleProcStat op_proc_stat_;  // processing statistics

    OpPerfInfo() = default;
    OpPerfInfo(const OpId& op_id, const SingleProcStat& op_proc_stat) : op_id_(op_id), op_proc_stat_(op_proc_stat) {}

    bool getIfProcessed() const { return op_proc_stat_.getIfProcessed(); }

    void ToString(string* s) const {
        op_id_.ToString(s);
        op_proc_stat_.ToString(s);
    }

    void FromString(const string& s, size_t& pos) {
        op_id_.FromString(s, pos);
        op_proc_stat_.FromString(s, pos);
    }

    string DebugString() const {
        string ret = op_id_.DebugString() + " " + op_proc_stat_.DebugString();
        return ret;
    }
};

}  // namespace AGE
