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
#include <limits>
#include <sstream>
#include <string>
#include <typeinfo>
#include <utility>
#include <vector>
#include "base/node.h"
#include "base/serializer.h"
#include "base/type.h"
#include "execution/op_profiler.h"
#include "util/config.h"

using std::string;
using std::stringstream;
using std::to_string;

namespace AGE {
class PhysicalPlan;

/**
 * INIT: message is just inited, wait query coordinator to prepare
 * SPAWN: message is spawned from last operator
 * SUBQUERY: message is collected from subquery
 * EMPTY: message is empty, currently used to weak-up the waiting thread when shutdown the executor
 * TRACKBACK: [DFS only] message is used to trackback to previous stage under DFS execution model
 * TIMEOUT: message is used to timeout the query
 * BARRIERREADY: [DFS only] message is processed by barrier op and this batch is ready
 * BARRIEREND: [DFS only] barrier op is finished and output all results
 * LOOPEND: [DFS only] all iterations in the loop are finished and this batch can be finished
 * EARLYSTOP: barrier op is early stopped by limit
 */
enum MessageType : uint8_t {
    INIT = 0,
    SPAWN = 1,
    SUBQUERY = 2,
    EMPTY = 3,
    TRACKBACK = 4,
    TIMEOUT = 5,
    BARRIERREADY = 6,
    BARRIEREND = 7,
    LOOPEND = 8,
    EARLYSTOP = 9
};
struct SubQueryInfo {
    u32 msg_id;                 // locally unique on each machine for each query
    u8 subquery_idx;            // indicates the subQ this info belongs to
    u8 subquery_entry_pos;      // records the position of associated subquery entry op
    u8 loop_idx;                // the iterations this message at, for loop_op
    u8 parentSplitHistorySize;  // The size of parent split history, used to check whether all msgs are collected.

    void ToString(string* s) const {
        Serializer::appendU32(s, msg_id);
        Serializer::appendU8(s, subquery_idx);
        Serializer::appendU8(s, subquery_entry_pos);
        Serializer::appendU8(s, loop_idx);
        Serializer::appendU8(s, parentSplitHistorySize);
    }

    void FromString(const string& s, size_t& pos) {
        Serializer::readU32(s, pos, &msg_id);
        Serializer::readU8(s, pos, &subquery_idx);
        Serializer::readU8(s, pos, &subquery_entry_pos);
        Serializer::readU8(s, pos, &loop_idx);
        Serializer::readU8(s, pos, &parentSplitHistorySize);
    }

    SubQueryInfo() : msg_id(0), subquery_idx(0), subquery_entry_pos(0), loop_idx(0), parentSplitHistorySize(0) {}
    SubQueryInfo(const SubQueryInfo& binfo)
        : msg_id(binfo.msg_id),
          subquery_idx(binfo.subquery_idx),
          subquery_entry_pos(binfo.subquery_entry_pos),
          loop_idx(binfo.loop_idx),
          parentSplitHistorySize(binfo.parentSplitHistorySize) {}

    SubQueryInfo& operator=(const SubQueryInfo& rhs) {
        if (this == &rhs) return *this;
        msg_id = rhs.msg_id;
        subquery_idx = rhs.subquery_idx;
        subquery_entry_pos = rhs.subquery_entry_pos;
        loop_idx = rhs.loop_idx;
        parentSplitHistorySize = rhs.parentSplitHistorySize;

        return *this;
    }

    string DebugString() const {
        string ret = "SubQueryInfo of msg " + to_string(msg_id) + ", subquery idx " + to_string(subquery_idx) + "\n";
        ret += "\t\tSubquery Entry Position: " + to_string(subquery_entry_pos) + "\n";
        ret += "\t\tLoop Iteration: " + to_string(loop_idx) + "\n";
        ret += "\t\tParentSplitHistorySize: " + to_string(parentSplitHistorySize) + "\n";
        return ret;
    }
};

static const uint32_t MessageDefaultSize = 4096;
class MessageHeader {
   public:
    static const u32 kNonLoopOutput = std::numeric_limits<u32>::max();

#define _CP_(varName) varName(rhs.varName)
    explicit MessageHeader(u32 maxDataSize = MessageDefaultSize) : maxDataSize(maxDataSize) {}
    MessageHeader(QueryId qid, MessageType type, u32 maxDataSize, u8 prevStageIdx, u8 currentStageIdx, u16 batchIdx,
                  u8 currentOpIdx, u32 loopHistory, const string& splitHistory, u32 recverNode, int recverPort,
                  u32 senderNode, u16 senderPort)
        : qid(qid),
          type(type),
          maxDataSize(maxDataSize),
          stTimeStamp(Tool::getDateNs()),
          prevStageIdx(prevStageIdx),
          currentStageIdx(currentStageIdx),
          batchIdx(batchIdx),
          currentOpIdx(currentOpIdx),
          loopHistory(loopHistory),
          splitHistory(splitHistory),
          recverNode(recverNode),
          recverPort(recverPort),
          senderNode(senderNode),
          senderPort(senderPort),
          opPerfInfos(1) {}

    MessageHeader(const MessageHeader& rhs)
        : _CP_(qid),
          _CP_(type),
          _CP_(maxDataSize),
          _CP_(stTimeStamp),
          _CP_(prevStageIdx),
          _CP_(currentStageIdx),
          _CP_(batchIdx),
          _CP_(currentOpIdx),
          _CP_(loopHistory),
          _CP_(splitHistory),
          _CP_(globalHistorySize),
          _CP_(recverNode),
          _CP_(recverPort),
          _CP_(senderNode),
          _CP_(senderPort),
          _CP_(subQueryInfos),
          _CP_(opPerfInfos),
          _CP_(assignedThread) {}

    MessageHeader(MessageHeader&& rhs)
        : _CP_(qid),
          _CP_(type),
          _CP_(maxDataSize),
          _CP_(stTimeStamp),
          _CP_(prevStageIdx),
          _CP_(currentStageIdx),
          _CP_(batchIdx),
          _CP_(currentOpIdx),
          _CP_(loopHistory),
          splitHistory(std::move(rhs.splitHistory)),
          _CP_(globalHistorySize),
          _CP_(recverNode),
          _CP_(recverPort),
          _CP_(senderNode),
          _CP_(senderPort),
          subQueryInfos(std::move(rhs.subQueryInfos)),
          opPerfInfos(std::move(rhs.opPerfInfos)),
          _CP_(assignedThread) {}

#undef _CP_
#define _CP_(varName) varName = rhs.varName
    MessageHeader& operator=(MessageHeader&& rhs) noexcept {
        if (this == &rhs) return *this;
        _CP_(qid);
        _CP_(type);
        _CP_(maxDataSize);
        _CP_(stTimeStamp);
        _CP_(prevStageIdx);
        _CP_(currentStageIdx);
        _CP_(batchIdx);
        _CP_(currentOpIdx);
        _CP_(loopHistory);
        splitHistory = std::move(rhs.splitHistory);
        _CP_(globalHistorySize);
        _CP_(recverNode);
        _CP_(recverPort);
        _CP_(senderNode);
        _CP_(senderPort);
        subQueryInfos = std::move(rhs.subQueryInfos);
        opPerfInfos = std::move(rhs.opPerfInfos);
        _CP_(assignedThread);

        return *this;
    }
#undef _CP_

    static const uint32_t UNDEF_NUM = 0;

    /* Query field */
    QueryId qid;
    MessageType type;
    u32 maxDataSize;  // max data size. Can be different among queries
    u64 stTimeStamp;  // starting time stamp

    /* Message-specific field */
    // <prevStageIdx, currentStageIdx> defines the execution edge in QC
    u8 prevStageIdx;       // the stage of which the message is the output
    u8 currentStageIdx;    // the stage of which the message is the input
    u16 batchIdx;          // the batch id of the message, unique for each execution edge
    u8 currentOpIdx;       // the op of which the message is the input
    u32 loopHistory;       // index of the last loop iteration
    string splitHistory;   // number of split messages for current stage
    u8 globalHistorySize;  // the depth of global history, used to barrier the msgs on cache worker

    /* Communication field */
    // This field is initialized by MsgDispatcher only
    u32 recverNode;  // [Unsend] Used when building brpc channel
    int recverPort;  // [Unsend] Used when building brpc channel
    // only for ComputeWorker
    u32 senderNode;  // [Unsend] Get from brpc::Controller::remote_side()
    int senderPort;  // Set by sender

    vector<SubQueryInfo> subQueryInfos;      // a stack of subquery meta
    vector<vector<OpPerfInfo>> opPerfInfos;  // a stack of vectors of performance counters (msg split triggers a push)

    /* Runtime Config */
    uint32_t assignedThread;  // Local use

    uint64_t msg_signature() {
        uint64_t ret = 0;
        ret += qid;
        ret <<= 8;
        ret += type;
        ret <<= 8;
        ret += prevStageIdx;
        ret <<= 8;
        ret += currentStageIdx;
        ret <<= 8;
        ret += batchIdx;
        ret <<= 8;
        ret += currentOpIdx;
        return ret;
    }

    /**
     * @target_role: send from compute worker to cache worker or cache to compute
     */
    void ToString(string* s, ClusterRole target_role) const {
        Serializer::appendVar(s, qid);
        Serializer::appendVar(s, type);
        Serializer::appendU32(s, maxDataSize);
        Serializer::appendU64(s, stTimeStamp);
        Serializer::appendU8(s, prevStageIdx);
        Serializer::appendU8(s, currentStageIdx);
        Serializer::appendU16(s, batchIdx);
        Serializer::appendU8(s, currentOpIdx);
        Serializer::appendU32(s, loopHistory);
        Serializer::appendStr(s, splitHistory);
        if (target_role == ClusterRole::CACHE) {
            Serializer::appendI32(s, senderPort);
        }

        Serializer::appendU32(s, subQueryInfos.size());
        for (const auto& info : subQueryInfos) info.ToString(s);

        Serializer::appendU32(s, opPerfInfos.size());
        for (const auto& vec : opPerfInfos) {
            Serializer::appendU32(s, vec.size());
            for (const auto& info : vec) info.ToString(s);
        }
    }

    void FromString(const string& s, size_t& pos, ClusterRole target_role) {
        Serializer::readVar(s, pos, &qid);
        Serializer::readVar(s, pos, &type);
        Serializer::readU32(s, pos, &maxDataSize);
        Serializer::readU64(s, pos, &stTimeStamp);
        Serializer::readU8(s, pos, &prevStageIdx);
        Serializer::readU8(s, pos, &currentStageIdx);
        Serializer::readU16(s, pos, &batchIdx);
        Serializer::readU8(s, pos, &currentOpIdx);
        Serializer::readU32(s, pos, &loopHistory);
        Serializer::readStr(s, pos, &splitHistory);
        if (target_role == ClusterRole::CACHE) {
            Serializer::readI32(s, pos, &senderPort);
        }

        uint32_t len;
        Serializer::readU32(s, pos, &len);
        subQueryInfos.resize(len);
        for (auto& info : subQueryInfos) info.FromString(s, pos);

        Serializer::readU32(s, pos, &len);
        opPerfInfos.resize(len);
        for (auto& vec : opPerfInfos) {
            Serializer::readU32(s, pos, &len);
            vec.resize(len);
            for (auto& info : vec) info.FromString(s, pos);
        }
    }

    // ensure the prevStageIdx is also forwarded when moving to next op
    inline void forward(bool forwardStage = false, int next = 0) {
        if (forwardStage) {
            // LOG(INFO) << "Forward stage from " << (int) currentStageIdx << " to " << next << std::flush;
            prevStageIdx = currentStageIdx;
            currentStageIdx = next;
            currentOpIdx = 0;
        } else {
            // LOG(INFO) << "Forward Op" << std::flush;
            currentOpIdx++;
        }
    }
    inline bool historyIsEmpty() { return splitHistory == ""; }

    inline void finish(int end_stage_idx, int end_op_idx) {
        // Move to the End operator
        prevStageIdx = end_stage_idx - 1;
        currentStageIdx = end_stage_idx;
        currentOpIdx = end_op_idx;
    }

    inline void reset(int prev_stage_idx, int cur_stage_idx) {
        prevStageIdx = prev_stage_idx;
        currentStageIdx = cur_stage_idx;
        currentOpIdx = 0;
    }

    void PreparePerfStatForCurrOp() {
        CHECK(opPerfInfos.size()) << "Empty perf info stack";
        opPerfInfos.back().emplace_back(OpId(currentStageIdx, currentOpIdx), SingleProcStat(stTimeStamp));
    }

    SingleProcStat& GetCurrentSplitPerfStat() {
        CheckPerfStatConsistency();
        CHECK(opPerfInfos.size() && opPerfInfos.back().size()) << "Unable to find current perf stat";
        return opPerfInfos.back().back().op_proc_stat_;
    }

    // Pop out the last perf stat vector. Make sure the stack isn't empty; Keep info for the upcoming op
    void ClearCurrentSplitPerfStat() {
        CheckPerfStatConsistency();
        if (opPerfInfos.back().size()) {  // check for the last entry
            OpPerfInfo curr_op_perf_info = opPerfInfos.back().back();
            opPerfInfos.pop_back();
            if (opPerfInfos.empty()) opPerfInfos.emplace_back();
            if (!curr_op_perf_info.getIfProcessed()) opPerfInfos.back().emplace_back(std::move(curr_op_perf_info));
        } else {  // pop out only
            opPerfInfos.pop_back();
            if (opPerfInfos.empty()) opPerfInfos.emplace_back();
        }
    }

    void CheckPerfStatConsistency() const {
        CHECK_EQ(opPerfInfos.size(), static_cast<u64>(std::count(splitHistory.begin(), splitHistory.end(), '\t')) + 1)
            << DebugString() << "OpPerfInfo Vec size != split time + 1";
        if (opPerfInfos.back().empty()) return;
        if (opPerfInfos.back().back().op_id_.stage_id_ != currentStageIdx ||
            opPerfInfos.back().back().op_id_.in_stage_op_id_ != currentOpIdx)
            CHECK(false) << "Current Perf info " << opPerfInfos.back().back().op_id_.DebugString()
                         << " doesn't match current op " << static_cast<u32>(currentStageIdx) << ", "
                         << static_cast<u32>(currentOpIdx) << "\n"
                         << DebugString();
    }

#define _appendDigit(x) ss << "\t" << #x << " : " << (u64)x << "\n";
#define _appendStr(x) ss << "\t" << #x << " : " << x << "\n";
    virtual string DebugString() const {
        stringstream ss;
        _appendDigit(qid);
        _appendDigit(type);
        _appendDigit(maxDataSize);
        _appendDigit(stTimeStamp);
        _appendDigit(prevStageIdx);
        _appendDigit(currentStageIdx);
        _appendDigit(batchIdx);
        _appendDigit(currentOpIdx);
        _appendDigit(loopHistory);
        _appendStr(splitHistory);
        _appendDigit(globalHistorySize);
        _appendDigit(recverNode);
        _appendDigit(recverPort);
        _appendDigit(senderNode);
        _appendDigit(senderPort);
        _appendDigit(subQueryInfos.size());
        for (auto& info : subQueryInfos) {
            ss << "\t\t" << info.DebugString() << "\n";
        }
        _appendDigit(opPerfInfos.size());
        for (u32 i = 0; i < opPerfInfos.size(); i++) {
            if (i) ss << "\t\t----- split -----\n";
            for (auto& info : opPerfInfos[i]) ss << "\t\t" << info.DebugString() << "\n";
        }
        return ss.str();
    }
#undef _appendDigit
#undef _appendStr

    static MessageHeader InitHeader(QueryId qid) {
        MessageHeader header(qid, MessageType::SPAWN, static_cast<uint32_t>(Config::GetInstance()->message_size_), 0, 0,
                             0, 0, kNonLoopOutput, "", UNDEF_NUM, UNDEF_NUM, UNDEF_NUM, UNDEF_NUM);
        return header;
    }

    static bool NeedToUpdatePrevBuffer(MessageType type) {
        return !(type == MessageType::TRACKBACK || type == MessageType::BARRIEREND || type == MessageType::EARLYSTOP);
    }
    static bool NeedToCollectPerfStat(MessageType type) { return type != TIMEOUT && type != EARLYSTOP; }
    static bool NoSplit(MessageType type) { return type == MessageType::EARLYSTOP; }
};
}  // namespace AGE
