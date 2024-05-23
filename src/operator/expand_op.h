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
#include <cmath>
#include <cstdio>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include "base/row.h"
#include "base/serializer.h"
#include "execution/graph_tool.h"
#include "execution/physical_plan.h"
#include "glog/logging.h"
#include "operator/abstract_op.h"
#include "operator/op_type.h"
#include "plan/op_params.h"
#include "storage/graph.h"

namespace AGE {
class ExpandOp : public AbstractOp {
   public:
    /**
     * @brief Construct a new Expand Op object
     *
     * @param src source column
     * @param dst destination column
     * @param compress the column that current compress column will move to after processing
     * @param dstVLabel label constraints for destination vertex
     */
    ExpandOp(OpType type, ColIndex src, ColIndex dst, ColIndex compress, LabelId eLabel, LabelId dstVLabel,
             DirectionType dir, u32 compressBeginIdx)
        : AbstractOp(type, compressBeginIdx),
          src(src),
          dst(dst),
          compress(compress),
          eLabel(eLabel),
          dstVLabel(dstVLabel),
          dir(dir) {
        SetStorageFetchCol(src);
    }

    bool process(Message &m, std::vector<Message> &output) override = 0;
    static ExpandOp *FromPhysicalParams(const PhysicalOpParams &params);

    void ToString(string *s) const override {
        AbstractOp::ToString(s);
        Serializer::appendI8(s, src);
        Serializer::appendI8(s, dst);
        Serializer::appendI8(s, compress);
        Serializer::appendVar(s, eLabel);
        Serializer::appendVar(s, dstVLabel);
        Serializer::appendVar(s, dir);
    }

    void FromString(const string &s, size_t &pos) override {
        AbstractOp::FromString(s, pos);
        src = Serializer::readI8(s, pos);
        dst = Serializer::readI8(s, pos);
        compress = Serializer::readI8(s, pos);
        Serializer::readVar(s, pos, &eLabel);
        Serializer::readVar(s, pos, &dstVLabel);
        Serializer::readVar(s, pos, &dir);
    }

    std::string DebugString(int depth = 0) const override {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s compressDst: %d, [%d]%s[%d, label:%u]", AbstractOp::DebugString(depth).c_str(),
                 compress, src, Edge_DebugString(eLabel, dir).c_str(), dst, dstVLabel);
        return buf;
    }

    SingleProcStat::ColStat ExtractColStat(const Message &msg) const override {
        u16 compress_dst = compress < 0 ? std::abs(compress) : 0;
        switch (type) {
        case OpType_EXPAND_CC:
            CHECK_NE(compress_dst, 1);
            return std::make_tuple(true, true, compress_dst);
        case OpType_EXPAND_CN:
            CHECK_NE(compress_dst, 1);
            return std::make_tuple(true, false, compress_dst);
        case OpType_EXPAND_NC:
            CHECK_NE(compress_dst, 1);
            return std::make_tuple(false, true, compress_dst);
        case OpType_EXPAND_NN:
            return std::make_tuple(false, false, compress_dst);
        case OpType_EXPAND_UL:
            return std::make_tuple(src == COLINDEX_COMPRESS, false, compress_dst);
        default:
            CHECK(false) << "Unexpected op type: " << OpType_DebugString(type);
        }
    }

   protected:
    explicit ExpandOp(OpType type) : AbstractOp(type) {}
    ColIndex src, dst, compress;
    LabelId eLabel, dstVLabel;
    DirectionType dir;

   private:
};
}  // namespace AGE
