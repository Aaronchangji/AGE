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

#include <algorithm>
#include <climits>
#include <cmath>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/aggregation.h"
#include "base/math.h"
#include "base/row.h"
#include "base/serializer.h"
#include "execution/monitor.h"
#include "operator/abstract_op.h"
#include "operator/op_type.h"
#include "operator/subquery_util.h"
#include "plan/op_params.h"

namespace AGE {

template <typename T = SubqueryUtil::BaseMeta>
class SubqueryEntryOp : public AbstractOp {
   public:
    bool process(Message& msg, std::vector<Message>& output) override {
        bool ready_to_send = true;
        if (msg.header.type == MessageType::SPAWN) {
            spwan_subquery_messages(msg, output);
        } else if (msg.header.type == MessageType::SUBQUERY) {
            ready_to_send = process_collected_messages(msg, output);
        } else {
            LOG(ERROR) << "Unexpected Message Type:\n" << msg.DebugString();
            CHECK(false);
        }

        return ready_to_send;
    }

    void setMonitor(std::shared_ptr<OpMonitor> monitor) { monitor_ = monitor; }

    static_assert(std::is_base_of<SubqueryUtil::BaseMeta, T>::value, "T must be derived from BaseMeta");
    using MetaMapType = tbb::concurrent_hash_map<SubqueryUtil::MetaMapKey, T, SubqueryUtil::MetaMapKeyCompare>;

    void ToString(string* s) const override {
        AbstractOp::ToString(s);
        Serializer::appendU32(s, inner_compress_begin_index);
        Serializer::appendU32(s, num_branches);
        Serializer::appendU32(s, input_columns.size());
        for (int col : input_columns) Serializer::appendI32(s, col);
        Serializer::appendI32(s, compress_dst);
        SubqueryUtil::AppendColumnMap(s, output_columns);
    }

   protected:
    explicit SubqueryEntryOp(OpType type, ColIndex compressBeginIdx_ = COMPRESS_BEGIN_IDX_UNDECIDED)
        : AbstractOp(type, compressBeginIdx_) {}
    SubqueryEntryOp(OpType type, ColIndex compressBeginIdx_, uint32_t num_branches_,
                    const vector<ColIndex>& input_columns_, ColIndex compress_dst_,
                    const SubqueryUtil::ColumnMapT& output_columns_, ColIndex inner_compress_begin_index_)
        : AbstractOp(type, compressBeginIdx_),
          num_branches(num_branches_),
          input_columns(input_columns_),
          compress_dst(compress_dst_),
          output_columns(output_columns_),
          inner_compress_begin_index(inner_compress_begin_index_) {
        Init();
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
        Init();
    }

    void FromString(const string& s, size_t& pos) override {
        AbstractOp::FromString(s, pos);
        inner_compress_begin_index = Serializer::readU32(s, pos);
        num_branches = Serializer::readU32(s, pos);
        input_columns.resize(Serializer::readU32(s, pos));
        for (size_t i = 0; i < input_columns.size(); i++) input_columns[i] = Serializer::readI32(s, pos);
        compress_dst = Serializer::readI32(s, pos);
        output_columns = SubqueryUtil::ReadColumnMap(s, pos);
        Init();
    }

    SingleProcStat::ColStat ExtractColStat(const Message& msg) const override {
        u16 compress_dst_abs = compress_dst < 0 ? std::abs(compress_dst) : 0;
        return std::make_tuple(input_from_compressed, output_to_compressed, compress_dst_abs);
    }

    virtual void spwan_subquery_messages(Message& msg, vector<Message>& ret) {
        // LOG(INFO) << "Input message: " << msg_idx_assigner_ << ":\n" << msg.DebugString();
        CHECK(msg_idx_assigner_ < UINT_MAX) << "MsgId assigner exceeds its maximum value, use a bigger type";
        u32 msg_id = msg_idx_assigner_++;
        extract_inputs(msg_id, msg);
        create_subquery_msg(msg_id, msg, ret);
    }

    virtual bool process_collected_messages(Message& msg, vector<Message>& ret) {
        vector<Row> newData;
        merge_results(msg, newData);
        msg.data.swap(newData);

        ret.emplace_back(std::move(msg));
        return true;
    }

   protected:
    inline size_t get_input_vec_sizes() {
        // For loop subqueries, the last input_column is LoopExpandDst,
        // which is ONLY used for spawning the subqMsgData (i.e., copy src to dst for loop initialization)
        return type == OpType_LOOP ? input_columns.size() - 1 : input_columns.size();
    }
    inline bool need_extract_input() { return true; }

    // =============MetaMap Related Functions==================
    virtual inline void init_meta_map(const Message& msg, typename MetaMapType::accessor& ac) {
        ac->second.input_map = SubqueryUtil::InputMapType(16, Item::VecHashFunc(get_input_vec_sizes()),
                                                          Item::VecEqualFunc(get_input_vec_sizes()));
    }
    void metamap_insert(const Message& msg, u32 msg_id, typename MetaMapType::accessor& m_ac) {
        SubqueryUtil::MetaMapKey key = SubqueryUtil::MetaMapKey(msg_id, msg.header.currentStageIdx);
        if (!meta_map_.insert(m_ac, key)) {
            LOG(ERROR) << "SubQueryEntry receives existed msg id: " << msg_id;
            CHECK(false);
        }
        init_meta_map(msg, m_ac);
    }

    inline SubqueryUtil::MetaMapKey get_meta_map_key(Message& msg) {
        CHECK_GT(msg.header.subQueryInfos.size(), (size_t)0);
        return SubqueryUtil::MetaMapKey(msg.header.subQueryInfos.back().msg_id,
                                        msg.header.subQueryInfos.back().subquery_entry_pos);
    }
    inline void get_meta_map_accessor(const SubqueryUtil::MetaMapKey& mkey, typename MetaMapType::accessor& map_ac) {
        if (!meta_map_.find(map_ac, mkey)) {
            LOG(ERROR) << "SubQueryEntry collected non-existed key: " << mkey.DebugString() << std::flush;
            CHECK(false);
        }
    }
    inline void get_meta_map_accessor(const SubqueryUtil::MetaMapKey& mkey,
                                      typename MetaMapType::const_accessor& map_ac) {
        if (!meta_map_.find(map_ac, mkey)) {
            LOG(ERROR) << "SubQueryEntry collected non-existed key: " << mkey.DebugString() << std::flush;
            CHECK(false);
        }
    }
    // =============MetaMap Related Functions End==============

    virtual void create_subquery_msg(uint32_t msg_id, Message& msg, vector<Message>& output) {
        // Create SubQueryInfo
        SubQueryInfo subQueryInfo;
        subQueryInfo.parentSplitHistorySize = msg.header.splitHistory.size();
        subQueryInfo.subquery_entry_pos = msg.header.currentStageIdx;

        msg.header.subQueryInfos.emplace_back(SubQueryInfo());
        for (size_t i = 0; i < num_branches; ++i) {
            subQueryInfo.msg_id = msg_id;
            subQueryInfo.subquery_idx = i;
            msg.header.subQueryInfos.back() = subQueryInfo;
            output.emplace_back(msg);
        }
    }

    virtual void extract_inputs(uint32_t msg_id, Message& msg) {
        // get meta and input map for this message
        typename MetaMapType::accessor m_ac;
        metamap_insert(msg, msg_id, m_ac);
        SubqueryUtil::InputMapType& inputMap = m_ac->second.input_map;

        // subqMsgData goes inside the subquery; outputData will be sent after this subquery
        vector<Row> subqMsgData, outputData;
        Item* inputVec = new Item[get_input_vec_sizes()];
        if (input_from_compressed) {
            // Use 2-phase decompression since the compressed column may be removed after this subquery
            // (i.e., compress_dst = COLINDEX_NONE)
            for (Row& r : msg.data) {
                // LOG(INFO) << "input row: " << r.DebugString();
                outputData.clear();
                for (Row& decompressRow : r.decompress(compressBeginIdx, COLINDEX_COMPRESS)) {
                    // LOG(INFO) << "first-step decompressed row: " << decompressRow.DebugString();

                    // generate output message data by moving compressed column to compressDst.
                    decompressRow.decompress(outputData, compressBeginIdx, compress_dst);
                    // LOG(INFO) << "second-step decompressed row: " << outputData.back().DebugString();

                    // construct inputVec and insert outputData to the input map
                    // for each NEW inputVec, generate subquery message data using input_columns
                    extract_input(inputVec, decompressRow, true);
                    auto itr = inputMap.find(inputVec);
                    if (itr == inputMap.end()) {
                        inputMap.emplace(inputVec, make_pair(0, vector<Row>{outputData.back()}));
                        inputVec = new Item[get_input_vec_sizes()];

                        subqMsgData.emplace_back(decompressRow, input_columns, inner_compress_begin_index);
                        // LOG(INFO) << "subquery input row: " << subqMsgData.back().DebugString();
                    } else {
                        // a set of output rows mapped by this input vector
                        itr->second.second.emplace_back(outputData.back());
                    }
                }
            }
        } else {
            // No input taken from the compressed column (i.e., no need to decompress it for subqMsgData)
            // Only decompress it to compress_dst for outputData
            for (Row& r : msg.data) {
                // LOG(INFO) << "input row: " << r.DebugString();
                outputData.clear();
                if (compress_dst == COLINDEX_COMPRESS) {
                    outputData.emplace_back(r);
                } else {
                    // May generate multiple output rows
                    r.decompress(outputData, compressBeginIdx, compress_dst);
                }

                // construct inputVec and insert outputData to the input map
                // for each NEW inputVec, generate subquery message data using input_columns
                extract_input(inputVec, r, true);
                auto itr = inputMap.find(inputVec);
                if (itr == inputMap.end()) {
                    itr = inputMap.emplace(inputVec, make_pair(0, vector<Row>{})).first;
                    inputVec = new Item[get_input_vec_sizes()];

                    subqMsgData.emplace_back(r, input_columns, inner_compress_begin_index);
                    // LOG(INFO) << "subquery input row: " << subqMsgData.back().DebugString();
                }
                Tool::VecMoveAppend(outputData, itr->second.second);
            }
        }
        delete[] inputVec;
        msg.data.swap(subqMsgData);
    }

    void extract_input(Item*& ret, const Row& r, bool usingInputColumns) {
        size_t inputVecSize = get_input_vec_sizes();
        if (usingInputColumns) {
            for (u32 i = 0; i < inputVecSize; i++) {
                CHECK_GE(input_columns[i], 0);
                ret[i] = r.at(input_columns[i]);
            }
        } else {
            for (u32 i = 0; i < inputVecSize; i++) {
                ret[i] = r.at(i);
            }
        }
    }

    virtual void merge_results(Message& msg, vector<Row>& newData) {
        SubqueryUtil::MetaMapKey mkey = get_meta_map_key(msg);
        typename MetaMapType::accessor m_ac;
        get_meta_map_accessor(mkey, m_ac);

        merge_results(msg, newData, m_ac);
    }

    virtual void merge_results(Message& msg, vector<Row>& newData, typename MetaMapType::accessor& map_ac) {
        Item* input_r = new Item[get_input_vec_sizes()];
        for (auto& r : msg.data) {
            if (r.size() < get_input_vec_sizes()) continue;
            extract_input(input_r, r, false);
            auto itr = map_ac->second.input_map.find(input_r);
            if (itr == map_ac->second.input_map.end()) {
                LOG(ERROR) << "Op " << OpTypeStr[type] << " receives non-existed input row" << std::flush;
                CHECK(false);
            }

            // merge results
            SubqueryUtil::merge_rows(newData, r, itr->second.second, output_columns, inner_compress_begin_index,
                                     compressBeginIdx);
        }
        delete[] input_r;
    }

   protected:
    string DebugString(int depth = 0) const override {
        // CHECK(nextOp.size() > 1) << "Subquery Entry Op should have at least two nextOps";
        string ret = AbstractOp::DebugString(depth) + " ";
        ret += "CompressDst: " + to_string(compress_dst);
        ret += ", ";
        ret += "InputCols: [";
        for (ColIndex col : input_columns) ret += to_string(col) + ", ";
        ret += "], ";
        ret += "InnerCompressBeginIdx: " + to_string(inner_compress_begin_index);
        ret += ", ";
        ret += "outputCols: [";
        for (const auto& [in, out] : output_columns) ret += to_string(in) + " -> " + to_string(out) + ", ";
        ret += "]";
        return ret;
    }

    void Init() {
        msg_idx_assigner_ = 0;
        input_from_compressed = false;
        for (ColIndex& col : input_columns) {
            if (col == COLINDEX_COMPRESS || col == static_cast<ColIndex>(compressBeginIdx))
                input_from_compressed = true;
            if (col == COLINDEX_COMPRESS) col = compressBeginIdx;
        }
        output_to_compressed = false;
        for (auto& mapping : output_columns) {
            ColIndex& col = mapping.second;
            if (col == COLINDEX_COMPRESS || col == static_cast<ColIndex>(compressBeginIdx)) output_to_compressed = true;
        }
    }

    uint32_t num_branches;

    // Parameters about input data
    vector<ColIndex> input_columns;  // the colindex of inputs for all subqueries
    ColIndex compress_dst;           // the destination of compress column for input rows if necessary

    // Parameters about subquery data
    SubqueryUtil::ColumnMapT output_columns;  // the colindex maps for the output for each subquery
    u32 inner_compress_begin_index;

    // local variables
    // meta_map_: mapping from input message to metadata (e.g. counters, intermediate results)
    MetaMapType meta_map_;
    // monitor_: for collecting perf stat from msg when sync
    std::shared_ptr<OpMonitor> monitor_;
    std::atomic<u32> msg_idx_assigner_;

    bool input_from_compressed, output_to_compressed;
};
}  // namespace AGE
