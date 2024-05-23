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
#include <cstdio>
#include <string>
#include <utility>
#include <vector>
#include "execution/graph_tool.h"
#include "execution/physical_plan.h"
#include "operator/abstract_op.h"
#include "operator/expand_op.h"
#include "storage/graph.h"

using std::vector;

// Expand from compress column to compress column
namespace AGE {
class ExpandCCOp : public ExpandOp {
   public:
    ExpandCCOp() : ExpandOp(OpType_EXPAND_CC) {}
    // Note that srcCol may be same as dstCol when variable in srcCol should be drop after this op
    ExpandCCOp(ColIndex src, ColIndex dst, ColIndex compress, LabelId eLabel, LabelId dstVLabel, DirectionType dir,
               u32 compressBeginIdx)
        : ExpandOp(OpType_EXPAND_CC, src, dst, compress, eLabel, dstVLabel, dir, compressBeginIdx) {
        assert(src == COLINDEX_COMPRESS && dst == COLINDEX_COMPRESS && compress != COLINDEX_COMPRESS);
    }

    bool process(Message &m, std::vector<Message> &output) override {
        Graph *g = m.plan->g;
        vector<Row> newData;

        if (compress == COLINDEX_NONE) {
            for (Row &r : m.data) {
                // LOG(INFO) << r.DebugString();
                vector<Item> nbs;
                size_t curr = 0;
                for (size_t i = compressBeginIdx; i < r.size(); i++) {
                    g->getNeighbor(r[i], eLabel, dir, nbs, dstVLabel);
                    for (size_t j = curr; j < nbs.size(); j++) {
                        nbs[j].cnt = r[i].cnt;
                    }
                    curr = nbs.size();
                }
                if (!nbs.size()) continue;
                newData.emplace_back(r, 0, compressBeginIdx, nbs.size());
                for (size_t i = 0; i < nbs.size(); i++) newData.back()[i + compressBeginIdx] = nbs[i];
                // LOG(INFO) << newData.back().DebugString();
            }
        } else {
            for (Row &r : m.data) {
                // LOG(INFO) << r.DebugString();
                for (size_t p = compressBeginIdx; p < r.size(); p++) {
                    vector<Item> nbs = g->getNeighbor(r[p], eLabel, dir, dstVLabel);
                    if (!nbs.size()) continue;
                    newData.emplace_back(r, 0, compressBeginIdx, nbs.size());
                    for (size_t i = 0; i < nbs.size(); i++) newData.back()[i + compressBeginIdx] = nbs[i];
                    newData.back()[compress] = r[p];
                    newData.back().setCount(newData.back().count() * r[p].cnt);
                    newData.back()[compress].cnt = 1;
                    // LOG(INFO) << newData.back().DebugString();
                }
            }
        }

        m.data.swap(newData);
        output.emplace_back(std::move(m));

        return true;
    }

   private:
};
}  // namespace AGE
