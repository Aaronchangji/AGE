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
#include <unordered_set>
#include <utility>
#include <vector>

#include "base/aggregation.h"
#include "base/expr_util.h"
#include "base/expression.h"
#include "base/row.h"
#include "execution/barrier_util.h"
#include "execution/message.h"
#include "operator/op_type.h"
#include "operator/subquery_entry_op.h"
#include "operator/subquery_util.h"

// OptionalMatch need to do extra operation after generating all output messages from an input message.  (Need split
// counter) It will append null values for the input Row that is not returned by the subquery.

namespace AGE {
struct OptionalMeta : public SubqueryUtil::BaseMeta {
    OptionalMeta() : SubqueryUtil::BaseMeta() {}
    // Checking whether all messages from one message are collected
    std::unordered_map<string, int> split_history_counter;
    // A compromise method with double spaces, can be optimized in future
    vector<Row> ready_set;
};

class OptionalMatchOp : public SubqueryEntryOp<OptionalMeta> {
   public:
    // OptionalMatch allows eagerly send.
    explicit OptionalMatchOp(ColIndex compressBeginIdx_ = COMPRESS_BEGIN_IDX_UNDECIDED)
        : SubqueryEntryOp<OptionalMeta>(OpType_OPTIONAL_MATCH, compressBeginIdx_) {}
    OptionalMatchOp(ColIndex compressBeginIdx_, uint32_t num_branches_, const vector<ColIndex>& input_columns_,
                    ColIndex compress_dst_, const SubqueryUtil::ColumnMapT& output_columns_,
                    ColIndex inner_compress_begin_index_)
        : SubqueryEntryOp<OptionalMeta>(OpType_OPTIONAL_MATCH, compressBeginIdx_, num_branches_, input_columns_,
                                        compress_dst_, output_columns_, inner_compress_begin_index_) {}

    using typename SubqueryEntryOp<OptionalMeta>::MetaMapType;

    /**
     * For each collected message, first mark the returned key as visited using the input_map,
     * then merge the returning values with original input Rows if needed. If all meessages
     * are collected, append NULL values to remaining tuples. Eventually Send out the results.
     */
    bool process_collected_messages(Message& msg, vector<Message>& output) override {
        CHECK(msg.header.subQueryInfos.size() > 0) << "Received message without SubqueryInfo\n";
        SubqueryUtil::MetaMapKey mkey = SubqueryEntryOp<OptionalMeta>::get_meta_map_key(msg);
        typename MetaMapType::accessor map_ac;
        SubqueryEntryOp<OptionalMeta>::get_meta_map_accessor(mkey, map_ac);

        // Mark the returned keys as visited by incrementing the counter
        SubqueryUtil::InputMapType& input_map = map_ac->second.input_map;
        Item* input_r = new Item[SubqueryEntryOp<OptionalMeta>::get_input_vec_sizes()];
        for (auto& r : msg.data) {
            SubqueryEntryOp<OptionalMeta>::extract_input(input_r, r, false);
            auto itr = input_map.find(input_r);
            if (itr == input_map.end()) {
                LOG(ERROR) << "Op " << OpTypeStr[SubqueryEntryOp<OptionalMeta>::type]
                           << " receives non-existed input row" << std::flush;
                CHECK(false);
            }

            itr->second.first++;
        }
        delete[] input_r;

        if (!is_ready(msg, map_ac)) {
            Tool::VecMoveAppend<Row>(msg.data, map_ac->second.ready_set);
            return false;
        }

        // Collect successful input tuples
        Tool::VecMoveAppend<Row>(map_ac->second.ready_set, msg.data);
        vector<Row> newData;
        SubqueryEntryOp<OptionalMeta>::merge_results(msg, newData, map_ac);
        msg.data.swap(newData);

        // Collect failed input tuples
        for (auto& [_, pair] : map_ac->second.input_map) {
            if (pair.first) continue;
            SubqueryUtil::nullify_rows(pair.second, output_columns, inner_compress_begin_index, compressBeginIdx);
            Tool::VecMoveAppend(pair.second, msg.data);
        }
        meta_map_.erase(map_ac);

        // Send out result
        create_output_msg(msg, output);
        return true;
    }

   private:
    // Check whether collected msg is the last message from an input message (Similar to BranchFilterEntryOp)
    //  Only one modification: Do not check if all branches have been collected.
    bool is_ready(Message& msg, typename MetaMapType::accessor& map_ac) {
        unordered_map<string, int>& path_counter = map_ac->second.split_history_counter;
        uint8_t end_size = msg.header.subQueryInfos.back().parentSplitHistorySize;
        return BarrierUtil::Sync(msg, path_counter, monitor_, end_size);
    }

    inline void create_output_msg(Message& msg, vector<Message>& output) {
        msg.header.subQueryInfos.pop_back();
        output.emplace_back(std::move(msg));
    }
};  // End of class declaration
}  // namespace AGE
