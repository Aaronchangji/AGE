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
#include <algorithm>
#include <functional>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "base/row.h"
#include "base/type.h"
#include "execution/message.h"
#include "execution/physical_plan.h"
#include "glog/logging.h"

namespace AGE {

using std::string;
using std::vector;
static const size_t MAX_BUFFER_SIZE = std::numeric_limits<size_t>::max();

struct BufferKey {
    u8 prev_stage_idx;
    u8 curr_stage_idx;
    u16 loop_iter_num;

    BufferKey() : prev_stage_idx(0), curr_stage_idx(0), loop_iter_num(0) {}
    BufferKey(u8 prev_stage_idx, u8 curr_stage_idx, u16 loop_iter_num = 0)
        : prev_stage_idx(prev_stage_idx), curr_stage_idx(curr_stage_idx), loop_iter_num(loop_iter_num) {}
    BufferKey(const BufferKey& key) = default;
    explicit BufferKey(u32 key) {
        loop_iter_num = key & ((1 << 16) - 1);
        curr_stage_idx = (key >> 16) & ((1 << 8) - 1);
        prev_stage_idx = (key >> 24) & ((1 << 8) - 1);
    }

    inline bool operator==(const BufferKey& rhs) const {
        return (prev_stage_idx == rhs.prev_stage_idx) && (curr_stage_idx == rhs.curr_stage_idx) &&
               (loop_iter_num == rhs.loop_iter_num);
    }

    u32 value() const {
        u32 ret = 0;
        ret |= prev_stage_idx;
        ret <<= 8;
        ret |= curr_stage_idx;
        ret <<= 16;
        ret |= loop_iter_num;
        return ret;
    }

    static string DebugString(u32 key_value) { return BufferKey(key_value).DebugString(); }

    string DebugString() const {
        string ret = "BufferKey: (";
        ret += std::to_string(prev_stage_idx);
        ret += ", ";
        ret += std::to_string(curr_stage_idx);
        ret += ": ";
        ret += std::to_string(loop_iter_num);
        ret += ")";
        return ret;
    }
};

struct BufferKeyCompare {
    static size_t hash(const BufferKey& key) {
        u32 value = key.value();
        return Math::MurmurHash64_x64(&value, sizeof(BufferKey));
    }

    static bool equal(const BufferKey& lhs, const BufferKey& rhs) { return lhs == rhs; }
};

// Intermediate Data Buffer for a query
class InterDataBuffer {
    u32 kNonSubqMsgId = std::numeric_limits<u32>::max();

   private:
    // The minimum unit of a data buffer (i.e., buffer is separated using subq_msg_id)
    // For now ONLY loop can have multiple message buffers
    struct MsgDataBuffer {
        vector<Row> data;
        u32 buf_data_size;

        MsgDataBuffer() : buf_data_size(0) {}
        size_t size(bool with_compress) const { return with_compress ? buf_data_size : data.size(); }
        string DebugString() const {
            string ret = "";
            ret += "Size (Num tuples): " + std::to_string(buf_data_size) + "\n";
            u32 output_counter = Config::GetInstance()->max_log_data_size_;
            for (const Row& r : data) {
                if (output_counter-- == 0) break;
                ret += r.DebugString() + "\n";
            }
            return ret;
        }
    };

   public:
    static bool is_loop_buffer(const Message& msg) {
        u8 prev_stage_idx = msg.header.prevStageIdx, curr_stage_idx = msg.header.currentStageIdx;
        CHECK(msg.plan != nullptr);
        return msg.plan->getStage(prev_stage_idx)->getOpType(0) == OpType_LOOP &&
               msg.plan->getStage(curr_stage_idx)->getNextStages().front() == prev_stage_idx;
    }

    InterDataBuffer() : compress_begin_index(0), batch_idx_assigner(0), num_input_tuples(0), num_ongoing_batch(0) {}

    u64 append(Message& msg) {
        // At least we have to construct the buffer
        u32 subq_msg_id = try_create_msg_buffer(msg), input_tuple_cnt = msg.TupleCnt();
        if (msg.data.empty()) return 0;
        num_input_tuples += input_tuple_cnt;

        MsgDataBuffer& buffer = get_msg_buffer(subq_msg_id);
        u32 buf_data_size = buffer.buf_data_size;
        auto que_iter = get_buf_queue_element(subq_msg_id, buf_data_size);
        size_buffer_queue.erase(que_iter);
        size_buffer_queue.emplace(get_buf_queue_key(subq_msg_id, buf_data_size += input_tuple_cnt));

        buffer.buf_data_size = buf_data_size;
        Tool::VecMoveAppend(msg.data, buffer.data);
        return input_tuple_cnt;
    }

    int pop(Message& msg, bool with_compress, size_t size = MAX_BUFFER_SIZE) {
        u32 subq_msg_id, buf_data_size;
        std::tie(subq_msg_id, buf_data_size) = pop_buf_queue_element();
        MsgDataBuffer& buffer = get_msg_buffer(subq_msg_id);

        // LOG(INFO) << "[DFS] Pop " << size << (with_compress ? " (w/ compress)" : "") << " from buf " << subq_msg_id;
        int batch_idx = pop_data(buffer, msg.data, with_compress, size);
        launch_batch();

        CHECK_GE(buf_data_size -= std::min(size, static_cast<u64>(buf_data_size)), 0);
        buffer.buf_data_size = buf_data_size;
        size_buffer_queue.emplace(get_buf_queue_key(subq_msg_id, buf_data_size));

        if (subq_msg_id != kNonSubqMsgId) {
            CHECK_GT(msg.header.subQueryInfos.size(), 0) << "Msg with empty subquey info: " << msg.DebugString();
            msg.header.subQueryInfos.back().msg_id = subq_msg_id;
        }
        // LOG(INFO) << "[DFS] Pop batch: " << batch_idx << ", buffer now:\n" << DebugString();
        return batch_idx;
    }

    bool empty() const {
        if (size_buffer_queue.empty()) return true;
        auto iter = size_buffer_queue.begin();
        u32 buf_data_size = (((*iter) >> 32) & kNonSubqMsgId);
        return buf_data_size == 0;
    }
    bool full(bool with_compress) const { return false; }
    u32 get_num_input_tuples() const { return num_input_tuples; }
    bool is_finish() const { return empty() && num_ongoing_batch == 0; }

    void launch_batch() { num_ongoing_batch++; }
    void finish_batch() {
        num_ongoing_batch--;
        CHECK_GE(num_ongoing_batch, 0) << "num_ongoing_batch should be non-negative";
    }

    string DebugString() const {
        string ret = "";
        ret += "Compress Begin: " + std::to_string(compress_begin_index) + "\n";
        ret += "Num Subqs: " + std::to_string(inter_data.size()) + "\n";
        ret += "Batch Idx: " + std::to_string(batch_idx_assigner) + "\n";
        ret += "Num Ongoing Batch: " + std::to_string(num_ongoing_batch) + "\n";
        for (const auto& buffer : inter_data) {
            ret += "Buffer " + to_string(buffer.first) + "\n";
            ret += buffer.second.DebugString();
        }
        return ret;
    }

    string collect_info() {
        string ret = "";
        ret += "#Batches: " + std::to_string(batch_idx_assigner) + "\n";
        return ret;
    }

   private:
    /**
     * @param src   buffer of input rows
     * @param dst   output rows
     * @param size  number of rows popped out
     * @param with_compress  whether count the number of compressed items into output size
     * @return batch idx
     */
    int pop_data(InterDataBuffer::MsgDataBuffer& src, vector<Row>& dst, bool with_compress, size_t size) {
        size_t cur_size = src.size(with_compress), num_popped_rows = 0;
        if (cur_size <= size) {
            // pop all
            Tool::VecMoveAppend(src.data, dst);
            return batch_idx_assigner++;
        }
        if (with_compress) {
            while (size) {
                Row& cur_source_row = src.data[num_popped_rows];
                size_t row_size = cur_source_row.size(),
                       num_tuple = std::max(row_size - compress_begin_index, static_cast<u64>(1));
                if (size >= num_tuple) {
                    dst.emplace_back(std::move(cur_source_row));
                    num_popped_rows++;
                    size -= num_tuple;
                } else {
                    Row tmp_row(cur_source_row, 0, compress_begin_index, size);
                    for (size_t src_idx = row_size - size, dst_idx = compress_begin_index; src_idx < row_size;
                         src_idx++, dst_idx++) {
                        tmp_row[dst_idx] = std::move(cur_source_row[src_idx]);
                    }
                    dst.emplace_back(std::move(tmp_row));
                    cur_source_row.resize(row_size - size);
                    size = 0;
                }
            }
            src.data.erase(src.data.begin(), src.data.begin() + num_popped_rows);
        } else {
            for (size_t i = 0; i < size; i++) {
                dst.emplace_back(std::move(src.data.at(i)));
            }
            src.data.erase(src.data.begin(), src.data.begin() + size);
        }
        return batch_idx_assigner++;
    }

    u32 try_create_msg_buffer(const Message& msg) {
        if (!compress_begin_index) {
            compress_begin_index = msg.GetCompressBeginIdx();
        } else {
            CHECK_EQ(compress_begin_index, msg.GetCompressBeginIdx());
        }
        u32 subq_msg_id = msg.execution_strategy.get_is_dfs() && msg.header.subQueryInfos.size()
                              ? msg.header.subQueryInfos.back().msg_id
                              : kNonSubqMsgId;
        if (subq_msg_id != kNonSubqMsgId) {
            CHECK(is_loop_buffer(msg)) << "Non-loop buffer " << static_cast<u32>(msg.header.prevStageIdx) << ", "
                                       << static_cast<u32>(msg.header.currentStageIdx)
                                       << " receives message with subq info:\n"
                                       << msg.DebugString();
        }
        if (!inter_data.count(subq_msg_id)) {
            inter_data[subq_msg_id] = MsgDataBuffer();
            size_buffer_queue.emplace(get_buf_queue_key(subq_msg_id, 0));
        }
        return subq_msg_id;
    }

    MsgDataBuffer& get_msg_buffer(u32 subq_msg_id) {
        MsgDataBuffer& buffer = inter_data.at(subq_msg_id);
        return buffer;
    }

    u64 get_buf_queue_key(u32 subq_msg_id, u32 buf_data_size) {
        u64 ret = buf_data_size;
        ret <<= 32;
        ret |= subq_msg_id;
        return ret;
    }

    std::set<u64, std::greater<u64>>::iterator get_buf_queue_element(u32 subq_msg_id, u32 buf_data_size) {
        auto iter = size_buffer_queue.find(get_buf_queue_key(subq_msg_id, buf_data_size));
        CHECK(iter != size_buffer_queue.end()) << "buffer queue element not found";
        return iter;
    }

    std::pair<u32, u32> pop_buf_queue_element() {
        CHECK_GT(size_buffer_queue.size(), 0) << "Empty buffer queue";
        auto iter = size_buffer_queue.begin();
        u32 subq_msg_id = ((*iter) & kNonSubqMsgId), buf_data_size = (((*iter) >> 32) & kNonSubqMsgId);
        size_buffer_queue.erase(iter);
        return std::make_pair(subq_msg_id, buf_data_size);
    }

    /**
     * inter_data separated by subq_msg_id (subq_msg_id = INTMAX for non-subquery messages)
     * size_buffer_queue is maintained so that each time we pop from the buffer having the largest data set
     * (i.e., each time we're most likely to pop data of batch size)
     */
    std::unordered_map<u32, MsgDataBuffer> inter_data;
    std::set<u64, std::greater<u64>> size_buffer_queue;

    // We assume for each IDBUffer, all rows follows the same layout (i.e. the compress_begin_index is the same)
    u32 compress_begin_index;
    u32 batch_idx_assigner;
    u32 num_input_tuples;
    int num_ongoing_batch;
};
}  // namespace AGE
