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

#include <iterator>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/aggregation.h"
#include "base/math.h"
#include "base/row.h"
#include "execution/barrier_util.h"
#include "operator/op_type.h"
#include "operator/subquery_entry_op.h"
#include "util/tool.h"

namespace AGE {

struct BranchFilterMeta : public SubqueryUtil::BaseMeta {
    BranchFilterMeta() : SubqueryUtil::BaseMeta(), subquery_counter(0) {}
    // number of collected subqueries
    size_t subquery_counter;
    // Checking whether all messages from one message are collected
    std::unordered_map<string, int> split_history_counter;
};

class BranchFilterEntryOp : public SubqueryEntryOp<BranchFilterMeta> {
   public:
    explicit BranchFilterEntryOp(OpType type, ColIndex compressBeginIdx_ = COMPRESS_BEGIN_IDX_UNDECIDED)
        : SubqueryEntryOp<BranchFilterMeta>(type, compressBeginIdx_) {}
    BranchFilterEntryOp(OpType type, ColIndex compressBeginIdx_, uint32_t num_branches_,
                        const vector<ColIndex>& input_columns_, ColIndex compress_dst_,
                        const SubqueryUtil::ColumnMapT& output_columns_, u32 inner_compress_begin_index_)
        : SubqueryEntryOp<BranchFilterMeta>(type, compressBeginIdx_, num_branches_, input_columns_, compress_dst_,
                                            output_columns_, inner_compress_begin_index_) {}

    using typename SubqueryEntryOp<BranchFilterMeta>::MetaMapType;
    bool process_collected_messages(Message& msg, vector<Message>& ret) override {
        CHECK(msg.header.subQueryInfos.size() > 0) << "Received message without SubqueryInfo\n";
        SubqueryUtil::MetaMapKey mkey = SubqueryEntryOp<BranchFilterMeta>::get_meta_map_key(msg);
        typename MetaMapType::accessor map_ac;
        SubqueryEntryOp<BranchFilterMeta>::get_meta_map_accessor(mkey, map_ac);

        vector<Row> newData;
        update_metamap(msg, map_ac);
        if (is_ready(msg, map_ac)) {
            send_ready_set(msg, map_ac, ret);
            return true;
        }
        return false;
    }

    // Returns the condition (i.e. number of subqueries) when each message collects all generated messages
    // 111 (2^3-1) for an AND subquery with 3 branches; 001 for a OR subquery with 3 branches
    u32 get_finished_condition() {
        switch (type) {
        case OpType_BRANCH_AND:
            return (1 << num_branches) - 1;
        case OpType_BRANCH_OR:
            return 1;
        case OpType_BRANCH_NOT:
            return 0;
        default:
            LOG(ERROR) << "Unexpected operator type" << std::flush;
            exit(-1);
        }
    }
    // Check whether a single input row is ready
    // Process the original rows after collection; Return true if current input can be deleted;
    bool send_ready_set(Message& msg, typename MetaMapType::accessor& map_ac, vector<Message>& ret) {
        msg.data.clear();
        for (auto& [key, pair] : map_ac->second.input_map) {
            if (pair.first < get_finished_condition()) continue;
            // Move the whole output vector to the end of the message data
            Tool::VecMoveAppend(pair.second, msg.data);
        }
        meta_map_.erase(map_ac);

        msg.header.subQueryInfos.pop_back();

        // LOG(INFO) << msg.DebugString();
        ret.emplace_back(std::move(msg));
        return true;
    }

   private:
    void update_metamap(const Message& msg, typename MetaMapType::accessor& map_ac) {
        SubqueryUtil::InputMapType& input_map = map_ac->second.input_map;
        Item* input_r = new Item[SubqueryEntryOp<BranchFilterMeta>::get_input_vec_sizes()];
        if (type == OpType_BRANCH_NOT) {
            for (auto& r : msg.data) {
                // LOG(INFO) << r.DebugString();
                SubqueryEntryOp<BranchFilterMeta>::extract_input(input_r, r, false);
                // In BRANCH_NOT subquery, input_r may has been removed from the input map
                auto itr = input_map.find(input_r);
                if (itr == input_map.end()) continue;

                delete[] itr->first;
                input_map.erase(itr);
            }
        } else if (type == OpType_BRANCH_AND || type == OpType_BRANCH_OR) {
            for (auto& r : msg.data) {
                // LOG(INFO) << r.DebugString();
                SubqueryEntryOp<BranchFilterMeta>::extract_input(input_r, r, false);
                auto itr = input_map.find(input_r);
                if (itr == input_map.end()) {
                    LOG(ERROR) << "Op " << OpTypeStr[SubqueryEntryOp<BranchFilterMeta>::type]
                               << " receives non-existed input row" << std::flush;
                    CHECK(false);
                }

                // Two messages to different branches could have some same input_r's
                int subqueryIdx = msg.header.subQueryInfos.back().subquery_idx;
                itr->second.first |= (1 << subqueryIdx);
            }
        } else {
            CHECK(false) << "Invalid subquery type";
        }
        delete[] input_r;
    }

    // Check whether collected msg is the last message from an input message
    bool is_ready(Message& msg, typename MetaMapType::accessor& map_ac) {
        unordered_map<string, int>& path_counter = map_ac->second.split_history_counter;
        uint8_t end_size = msg.header.subQueryInfos.back().parentSplitHistorySize;
        if (BarrierUtil::Sync(msg, path_counter, monitor_, end_size)) {
            // Check whether the all branches are collected
            if (++map_ac->second.subquery_counter == num_branches) {
                return true;
            }
        }
        return false;
    }
};
}  // namespace AGE
