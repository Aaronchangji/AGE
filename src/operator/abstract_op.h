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
#include <stdint.h>
#include <cassert>
#include <string>
#include <tuple>
#include <vector>

#include "base/serializer.h"
#include "base/type.h"
#include "execution/message.h"
#include "execution/message_header.h"
#include "execution/op_profiler.h"
#include "operator/op_type.h"
#include "plan/op_params.h"

using std::string;

namespace AGE {
class AbstractOp {
   public:
    // AbstractOp() : compressBeginIdx(COMPRESS_BEGIN_IDX_UNDECIDED), storageFetchCol(COLINDEX_NONE) {}
    virtual ~AbstractOp() = default;

    OpType type;
    u32 compressBeginIdx;

    // Column to fetch VP/EP/ADJ in storage
    ColIndex storageFetchCol;

    // What process should do:
    //  Compute the operator result and put it into output.
    //  Return if it is ok to send message.
    virtual bool process(Message& m, std::vector<Message>& output) {
        assert(false && "Invalid call to AbstractOp::process()");
        return false;
    }

    virtual bool processWithProfiling(Message& m, std::vector<Message>& output, ClusterRole exe_side) {
        m.GetCurrentSplitPerfStat().setColStat(ExtractColStat(m));
        m.GetCurrentSplitPerfStat().setDataStat(m.GetDataStat(exe_side));
        m.GetCurrentSplitPerfStat().setProcStartTime(Tool::getTimeNs());
        bool ret = process(m, output);
        if (!OpType_IsBarrier(type)) {
            CHECK(output.size() == 1) << "non-barrier op always produces one output msg";
            output.front().GetCurrentSplitPerfStat().setProcEndTime(Tool::getTimeNs());
        } else {
            // Barrier: 1) ends; 2) ready 3) NOT ready
            if (output.size()) {
                for (auto& m : output) m.GetCurrentSplitPerfStat().setProcEndTime(Tool::getTimeNs());
            } else if (m.header.type == MessageType::BARRIERREADY) {
                m.GetCurrentSplitPerfStat().setProcEndTime(Tool::getTimeNs());
            }
        }
        return ret;
    }

    // Recover from serialized byte string
    virtual void FromString(const string& s, size_t& pos) {
        compressBeginIdx = Serializer::readU8(s, pos);
        storageFetchCol = Serializer::readI8(s, pos);
    }

    // Build from PhysicalOpParams
    virtual void FromPhysicalOpParams(const PhysicalOpParams& params) { compressBeginIdx = params.compressBeginIdx; }

    virtual void ToString(string* s) const {
        Serializer::appendU8(s, compressBeginIdx);
        Serializer::appendU8(s, storageFetchCol);
    }

    virtual string DebugString(int depth = 0) const {
        return std::string(OpTypeStr[type]) + ": compressBeginIdx: " + std::to_string(compressBeginIdx) + ",";
    }

    void appendType(string* s) { Serializer::appendVar(s, type); }

    virtual SingleProcStat::ColStat ExtractColStat(const Message& msg) const {
        LOG(WARNING) << "ExtractColStat not defined by this op: " << OpType_DebugString(type);
        return std::make_tuple(false, false, 0);
    }

   protected:
    explicit AbstractOp(OpType type, u32 compressBeginIdx = COMPRESS_BEGIN_IDX_UNDECIDED)
        : type(type), compressBeginIdx(compressBeginIdx), storageFetchCol(COLINDEX_NONE) {}

    AbstractOp(const string& s, size_t& pos, OpType type) : type(type) {
        compressBeginIdx = Serializer::readU8(s, pos);
        storageFetchCol = Serializer::readI8(s, pos);
    }

    void SetStorageFetchCol(ColIndex sfc) { storageFetchCol = sfc; }

    bool HasCompress(const Message& m) const { return m.data.size() ? compressBeginIdx < m.data[0].size() : 0; }
};
}  // namespace AGE
