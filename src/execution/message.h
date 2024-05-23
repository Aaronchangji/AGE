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
#include <memory>
#include <sstream>
#include <string>
#include <tuple>
#include <typeinfo>
#include <utility>
#include <vector>

#include "base/row.h"
#include "base/serializer.h"
#include "base/type.h"
#include "execution/execution_strategy.h"
#include "execution/message_header.h"
#include "execution/op_profiler.h"
#include "operator/op_type.h"
#include "util/config.h"

using std::shared_ptr;
using std::string;
using std::stringstream;
using std::to_string;

namespace AGE {
class PhysicalPlan;

// The unit to execute operator.
// Including intermedate result & PhysicalPlan process metadata.
class Message {
   public:
    Message() : plan(nullptr) {}
    explicit Message(MessageHeader&& header) : header(std::move(header)), plan(nullptr) {}
    Message(const Message& m, bool copy_data = true)
        : header(m.header), plan(m.plan), execution_strategy(m.execution_strategy) {
        if (copy_data) data = m.data;
    }
    Message& operator=(const Message& m) = delete;

    Message(Message&& m) noexcept
        : header(std::move(m.header)),
          plan(std::move(m.plan)),
          data(std::move(m.data)),
          execution_strategy(std::move(m.execution_strategy)) {}

    Message& operator=(Message&& m) noexcept {
        header = std::move(m.header);
        plan = std::move(m.plan);
        data = std::move(m.data);
        execution_strategy = std::move(m.execution_strategy);
        return *this;
    }

    MessageHeader header;
    shared_ptr<PhysicalPlan> plan;
    std::vector<Row> data;

    ExecutionStrategy execution_strategy;

    void PrintData() const {
        for (const Row& r : data) PrintRow(r);
    }

    size_t DataSize() const {
        size_t size = 0;
        for (const Row& r : data) size += r.size();
        return size;
    }

    size_t TupleCnt(ClusterRole role = ClusterRole::COMPUTE) const {
        size_t cnt = 0, compress_begin_idx = GetCompressBeginIdx(role);
        for (const Row& r : data) {
            cnt += r.size() > compress_begin_idx ? r.size() - compress_begin_idx : 1;
        }
        return cnt;
    }

    std::tuple<u64, u64, u64> GetDataStat(ClusterRole role = ClusterRole::COMPUTE) const {
        u32 compress_begin_idx = GetCompressBeginIdx(role);
        u64 line_size = data.size(), tuple_size = 0, item_size = 0;
        for (const Row& r : data) {
            item_size += r.size();
            tuple_size += r.size() > compress_begin_idx ? r.size() - compress_begin_idx : 1;
        }
        CHECK_GE(item_size, tuple_size);
        CHECK_GE(tuple_size, line_size);
        return std::make_tuple(item_size, tuple_size, line_size);
    }

    // Get the column number of current op
    u32 GetCompressBeginIdx(ClusterRole role = ClusterRole::COMPUTE) const;

    SingleProcStat& GetCurrentSplitPerfStat() { return header.GetCurrentSplitPerfStat(); }

    void ToString(string* s, ClusterRole target_role) const {
        // header.ToString(s, target_role);
        Serializer::appendU32(s, data.size());
        for (const Row& row : data) row.ToString(s);
        if (target_role == ClusterRole::CACHE) execution_strategy.ToString(s);
    }

    void FromString(const string& s, size_t& pos, ClusterRole target_role) {
        // header.FromString(s, pos, target_role);
        u32 len;
        Serializer::readU32(s, pos, &len);
        data.resize(len);
        for (Row& row : data) row.FromString(s, pos);
        if (target_role == ClusterRole::CACHE) execution_strategy.FromString(s, pos);
    }

    static Message CreateFromString(const string& header_s, const string& data_s, ClusterRole target_role) {
        Message msg;
        u64 pos = 0;
        msg.header.FromString(header_s, pos, target_role);
        pos = 0;
        msg.FromString(data_s, pos, target_role);
        return msg;
    }

    static Message CreateFromString(const string& msg, ClusterRole target_role, size_t& pos) {
        Message ret;
        // u64 pos = 0;
        ret.header.FromString(msg, pos, target_role);
        ret.FromString(msg, pos, target_role);
        return ret;
    }

    // Split some data into a new Message so that:
    //     - ReturnMessage.data.size() <= this->header.maxDataSize
    //     - ReturnMessage.data.size() should be as much as big as possible
    Message split(u32 compressBeginIdx) {
        // printf("Message::split() %u\n", maxDataSize);
        assert(compressBeginIdx <= header.maxDataSize);

        Message ret(*this, false);

        // Notice that we need to try to keep the order of Row so that the result of Order/Limit is right
        // So firstly we count how many rows need to move to return msg
        size_t lastIdx = data.size();
        for (i64 i = data.size() - 1, curSize = 0; i >= 0; i--) {
            if (curSize + data[i].size() <= header.maxDataSize) {
                // Case 1: this row can be fully moved to return msg
                curSize += data[i].size();
                lastIdx = i;
            } else if (curSize + compressBeginIdx + 1 <= header.maxDataSize) {
                // Case 2: this row can be partially moved to return msg by split compress col
                size_t fillSize = header.maxDataSize - curSize - compressBeginIdx;
                ret.data.emplace_back(data[i], 0, compressBeginIdx, fillSize);
                for (size_t j = data[i].size() - fillSize, k = compressBeginIdx; j < data[i].size(); j++, k++) {
                    ret.data.back()[k] = std::move(data[i][j]);
                }
                data[i].resize(data[i].size() - fillSize);
                lastIdx = i + 1;
                break;
            } else {
                // Case 3: this row can't be moved to return msg
                lastIdx = i + 1;
                break;
            }
        }

        // Then we move the rows which can be fully moved to return msg
        for (size_t i = lastIdx; i < data.size(); i++) {
            ret.data.emplace_back(std::move(data[i]));
        }

        // Delete moved rows in this->data
        data.resize(lastIdx);

        return ret;
    }

    string DebugString() const {
        string s = header.DebugString();
        s += "\tStrategy:\n" + execution_strategy.DebugString() + "\n";
        s += "\tData Size " + to_string(data.size()) + "\n";
        u32 output_counter = Config::GetInstance()->max_log_data_size_;
        for (const Row& r : data) {
            if (output_counter-- == 0) break;
            s += r.DebugString() + "\n";
        }
        return s;
    }

    static Message BuildEmptyMessage(QueryId qid);
    static Message CreateInitMsg(MessageHeader&& header);
    // Split msg into some messages (i.e. vector<Message>& ret)
    // So that the size of data of every new messages <= msg.header.maxDataSize
    static void SplitMessage(Message& msg, vector<Message>& ret, ClusterRole role = ClusterRole::COMPUTE);
    static void EarlyStop(Message& msg, uint8_t reset_path_size);
};
}  // namespace AGE
