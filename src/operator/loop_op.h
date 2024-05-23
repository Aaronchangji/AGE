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
#include <tbb/concurrent_hash_map.h>
#include <tbb/parallel_for.h>

#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/expr_util.h"
#include "base/expression.h"
#include "base/row.h"
#include "base/serializer.h"
#include "execution/barrier_util.h"
#include "execution/message_header.h"
#include "execution/op_profiler.h"
#include "operator/abstract_op.h"
#include "operator/op_type.h"
#include "operator/subquery_entry_op.h"
#include "operator/subquery_util.h"
#include "plan/op_params.h"

using std::unordered_map;

namespace AGE {
struct LoopMeta : SubqueryUtil::BaseMeta {
   public:
    struct LoopBatchKey {
        u32 loop_idx;
        u32 batch_idx;

        LoopBatchKey(u32 loop_idx, u32 batch_idx) : loop_idx(loop_idx), batch_idx(batch_idx) {}
        bool operator==(const LoopBatchKey& rhs) const {
            return loop_idx == rhs.loop_idx && batch_idx == rhs.batch_idx;
        }

        u64 value() const {
            u64 ret = loop_idx;
            ret <<= 32;
            ret |= batch_idx;
            return ret;
        }
        string DebugString() const {
            string s = "";
            s += "loop_idx: " + to_string(loop_idx) + ", ";
            s += "batch_idx: " + to_string(batch_idx);
            return s;
        }
    };
    struct LoopBatchKeyHash {
        size_t operator()(const LoopBatchKey& k) const {
            u64 v = k.value();
            return Math::MurmurHash64_x64(&v, sizeof(v));
        }
    };
    struct LoopBatchMeta {
        unordered_map<string, int> split_history_counter;
        vector<Row> ready_set;
    };

   public:
    LoopMeta() : SubqueryUtil::BaseMeta() {}

    LoopBatchMeta& get_batch_meta(const Message& msg) {
        LoopBatchKey key(msg.header.subQueryInfos.back().loop_idx, msg.header.batchIdx);
        auto iter = batch_map.find(key);
        if (iter == batch_map.end()) {
            batch_map[key] = LoopBatchMeta();
            iter = batch_map.find(key);
        }
        return iter->second;
    }

   private:
    // BatchKey: <loop_idx, batch_idx> -> BatchMeta: <history_counter, ready_set>
    unordered_map<LoopBatchKey, LoopBatchMeta, LoopBatchKeyHash> batch_map;
};

/**
 * The iteration range is [pass_loop_num, end_loop_num)
 * e.g. Match (n)-[:*2..4]-(m)
 *      pass_loop_num = 2, end_loop_num = 4
 * e.g. Match (n)-[:*1..3]-(m)
 *      pass_loop_num = 1, end_loop_num = 3
 */
class LoopOp : public SubqueryEntryOp<LoopMeta> {
   public:
    explicit LoopOp(ColIndex compressBeginIdx_ = COMPRESS_BEGIN_IDX_UNDECIDED)
        : SubqueryEntryOp<LoopMeta>(OpType_LOOP, compressBeginIdx_) {}
    LoopOp(ColIndex compressBeginIdx_, const vector<ColIndex>& input_columns_, ColIndex compress_dst_,
           const SubqueryUtil::ColumnMapT& output_columns_, ColIndex inner_compress_begin_index_, u8 pass_loop_num_,
           u8 end_loop_num_)
        : SubqueryEntryOp<LoopMeta>(OpType_LOOP, compressBeginIdx_, 1, input_columns_, compress_dst_, output_columns_,
                                    inner_compress_begin_index_),
          pass_loop_num(pass_loop_num_),
          end_loop_num(end_loop_num_) {}

    void ToString(string* s) const override {
        SubqueryEntryOp::ToString(s);
        Serializer::appendU8(s, pass_loop_num);
        Serializer::appendU8(s, end_loop_num);
    }

    void FromString(const string& s, size_t& pos) override {
        SubqueryEntryOp::FromString(s, pos);
        pass_loop_num = Serializer::readU8(s, pos);
        end_loop_num = Serializer::readU8(s, pos);
    }

    void FromPhysicalOpParams(const PhysicalOpParams& params) override {
        const string& s = params.params;
        size_t pos = 0;
        AbstractOp::FromPhysicalOpParams(params);
        inner_compress_begin_index = Serializer::readU32(s, pos);
        num_branches = Serializer::readU32(s, pos);
        input_columns.resize(Serializer::readU32(s, pos));
        for (size_t i = 0; i < input_columns.size(); i++) input_columns[i] = Serializer::readI32(s, pos);
        compress_dst = Serializer::readI32(s, pos);
        output_columns = SubqueryUtil::ReadColumnMap(s, pos);
        pass_loop_num = Serializer::readU8(s, pos);
        end_loop_num = Serializer::readU8(s, pos);
        Init();
    }

    using typename SubqueryEntryOp<LoopMeta>::MetaMapType;
    bool process_collected_messages(Message& msg, vector<Message>& output) override {
        CHECK(msg.header.subQueryInfos.size() > 0) << "Received message without SubqueryInfo\n";
        SubqueryUtil::MetaMapKey mkey = SubqueryEntryOp<LoopMeta>::get_meta_map_key(msg);
        typename MetaMapType::accessor map_ac;
        SubqueryEntryOp<LoopMeta>::get_meta_map_accessor(mkey, map_ac);

        LoopMeta::LoopBatchMeta& batch_meta = map_ac->second.get_batch_meta(msg);
        if (!is_ready(msg, batch_meta)) {
            // put the data into the meta
            Tool::VecMoveAppend(msg.data, batch_meta.ready_set);
            return false;
        }

        u32 loop_num = msg.header.subQueryInfos.back().loop_idx + 1;
        CHECK(loop_num <= end_loop_num) << "Loop Op receives a message with outbound loop_num: " << loop_num
                                        << std::flush;

        // This iteration is done, get all data first
        Tool::VecMoveAppend(batch_meta.ready_set, msg.data);
        finish_iteration(msg, output, map_ac);
        return true;
    }

    bool is_ready(Message& msg, LoopMeta::LoopBatchMeta& meta) {
        unordered_map<string, int>& path_counter = meta.split_history_counter;
        uint8_t end_size = msg.header.subQueryInfos.back().parentSplitHistorySize;
        if (BarrierUtil::Sync(msg, path_counter, monitor_, end_size)) {
            // Current Loop is done
            return true;
        }
        return false;
    }

    void finish_iteration(Message& msg, vector<Message>& output, typename MetaMapType::accessor& map_ac) {
        u32 loop_num = msg.header.subQueryInfos.back().loop_idx + 1;
        if (msg.data.size() == 0) {
            // No data anymore, no need to generate iterative message
            if (loop_num <= pass_loop_num) {
                // This is the first output-able iteration, generate one message to end it
                // No splitHistory modified
                process_output_msg_header(msg, false, true);
                output.emplace_back(std::move(msg));
            } else {
                // generate all the output messages to satisfy the splitHistory
                if (msg.execution_strategy.get_is_dfs()) {
                    process_output_msg_header(msg, false, true);
                    create_output_msg(msg, output, false);
                } else {
                    process_output_msg_header(msg, true, false);
                    for (u32 i = loop_num; i <= end_loop_num; i++) {
                        create_output_msg(msg, output, i != end_loop_num);
                    }
                }
            }
        } else {
            if (loop_num < end_loop_num) {
                LOG_IF(INFO, Config::GetInstance()->verbose_ >= 2)
                    << "Create iterate message where " << to_string(loop_num) << " < " << to_string(end_loop_num)
                    << std::flush;
                create_iterate_msg(msg, output, loop_num >= pass_loop_num);
            }
            if (loop_num >= pass_loop_num) {
                LOG_IF(INFO, Config::GetInstance()->verbose_ >= 2)
                    << "Create output message where " << to_string(loop_num) << " >= " << to_string(pass_loop_num)
                    << std::flush;
                vector<Row> newData;
                SubqueryEntryOp<LoopMeta>::merge_results(msg, newData, map_ac);
                msg.data.swap(newData);

                process_output_msg_header(msg, true, loop_num == end_loop_num);
                create_output_msg(msg, output, false);
            }
        }
    }

    inline void create_iterate_msg(Message& msg, vector<Message>& output, bool is_copy = true) {
        // mostly copy
        if (is_copy)
            output.emplace_back(msg);
        else
            output.emplace_back(std::move(msg));
        output.back().header.subQueryInfos.back().loop_idx++;
        output.back().header.type = MessageType::SPAWN;
    }

    void process_output_msg_header(Message& msg, bool need_split_history = true, bool is_last_iter = false) {
        if (msg.execution_strategy.get_is_dfs() && is_last_iter) {
            // QC needs this info to correctly handle batch completion
            msg.header.loopHistory = msg.header.subQueryInfos.back().loop_idx;
        }
        if (!msg.execution_strategy.get_is_dfs() && need_split_history) {
            // DFS mode doesn't need this info to barrier all iteration
            msg.header.splitHistory += "\t" + to_string(get_effective_iteration_number());
            // A virtual split that makes splitHistory & opPerfInfos consistent
            if (Config::GetInstance()->op_profiler_) {
                // Move the current loop perf to the latest split
                auto loop_op_perf_info = msg.header.opPerfInfos.back().back();
                msg.header.opPerfInfos.back().pop_back();
                msg.header.opPerfInfos.emplace_back(vector<OpPerfInfo>{loop_op_perf_info});
            } else {
                msg.header.opPerfInfos.emplace_back();
            }
        }
        msg.header.type = MessageType::LOOPEND;
        msg.header.subQueryInfos.pop_back();
    }

    inline void create_output_msg(Message& msg, vector<Message>& output, bool is_copy = false) {
        // TODO(cjli): decide whether send or store output messages
        // mostly move
        if (is_copy)
            output.emplace_back(msg);
        else
            output.emplace_back(std::move(msg));
    }

    inline u8 get_effective_iteration_number() { return end_loop_num - pass_loop_num + 1; }
    inline u8 get_end_iteration_number() { return end_loop_num; }

   private:
    u8 pass_loop_num;
    u8 end_loop_num;
};
}  // namespace AGE
