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

#include <shared_mutex>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include "base/intervals.h"
#include "base/row.h"
#include "base/serializer.h"
#include "base/type.h"
#include "execution/message.h"
#include "execution/physical_plan.h"
#include "operator/abstract_op.h"
#include "plan/op_params.h"
#include "storage/graph.h"

namespace AGE {

using std::vector;

// Scan by index operator
class IndexScanOp : public AbstractOp {
   public:
    IndexScanOp() : AbstractOp(OpType_INDEX_SCAN) {}
    IndexScanOp(ColIndex dstCol, LabelId labelId, PropId propId, Intervals &&intervals, u32 compressBeginIdx)
        : AbstractOp(OpType_INDEX_SCAN, compressBeginIdx),
          dstCol(dstCol),
          labelId(labelId),
          propId(propId),
          intervals(std::move(intervals)) {}

    void ToString(string *s) const override {
        AbstractOp::ToString(s);
        Serializer::appendI8(s, dstCol);
        Serializer::appendU8(s, labelId);
        Serializer::appendU8(s, propId);
        intervals.ToString(s);
    }

    void FromString(const string &s, size_t &pos) override {
        AbstractOp::FromString(s, pos);
        dstCol = Serializer::readI8(s, pos);
        labelId = Serializer::readU8(s, pos);
        propId = Serializer::readU8(s, pos);
        intervals.FromString(s, pos);
    }

    void FromPhysicalOpParams(const PhysicalOpParams &params) override {
        AbstractOp::FromPhysicalOpParams(params);
        dstCol = params.cols[0];
        size_t pos = 0;
        intervals.FromString(params.params, pos);
        Serializer::readVar(params.params, pos, &propId);
        Serializer::readVar(params.params, pos, &labelId);
    }

    SingleProcStat::ColStat ExtractColStat(const Message &msg) const override {
        return std::make_tuple(false, dstCol == COLINDEX_COMPRESS, 1);
    }

    bool process(Message &m, std::vector<Message> &output) override {
        // LOG(INFO) << "IndexScan::process():\n" << intervals.DebugString() << "\n" << std::flush;
        Graph *g = m.plan->g;
        IndexStore *indexStore = g->GetIndexStore();
        vector<Row> newData;

        if (dstCol == COLINDEX_COMPRESS) {
            newData.emplace_back(compressBeginIdx);
            for (auto [itr, end] : indexStore->IndexQuery(labelId, propId, intervals)) {
                for (; itr != end; ++itr) {
                    newData.back().emplace_back(T_VERTEX, itr->second);
                }
            }
            newData.back().setCount(1);
            if (newData.back().size() == static_cast<u64>(compressBeginIdx)) newData.pop_back();
        } else {
            for (auto [itr, end] : indexStore->IndexQuery(labelId, propId, intervals)) {
                for (; itr != end; ++itr) {
                    newData.emplace_back(compressBeginIdx);
                    newData.back()[dstCol] = Item(T_VERTEX, itr->second);
                    newData.back().setCount(1);
                }
            }
        }

        m.data.swap(newData);
        output.emplace_back(std::move(m));
        return true;
    }

    // private:
    ColIndex dstCol;
    LabelId labelId;
    PropId propId;
    Intervals intervals;
};

}  // namespace AGE
