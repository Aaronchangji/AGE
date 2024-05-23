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

// Expand from normal column to compress column
namespace AGE {
class ExpandNCOp : public ExpandOp {
   public:
    ExpandNCOp() : ExpandOp(OpType_EXPAND_NC) {}
    // Note that srcCol may be same as dstCol when variable in srcCol should be drop after this op
    ExpandNCOp(ColIndex src, ColIndex dst, ColIndex compress, LabelId eLabel, LabelId dstVLabel, DirectionType dir,
               u32 compressBeginIdx)
        : ExpandOp(OpType_EXPAND_NC, src, dst, compress, eLabel, dstVLabel, dir, compressBeginIdx) {
        assert(src != COLINDEX_COMPRESS && dst == COLINDEX_COMPRESS && compress != COLINDEX_COMPRESS);
    }

    bool process(Message &m, std::vector<Message> &output) override {
        Graph *g = m.plan->g;
        vector<Row> newData;

        for (Row &r : m.data) {
            // LOG(INFO) << "input: " << r.DebugString();
            vector<Item> nbs = g->getNeighbor(r[src], eLabel, dir, dstVLabel);
            if (!nbs.size()) continue;

            if (compress == COLINDEX_NONE) {
                // Note that r.size() == compressBeginIdx means compress col not exist
                u64 compressCount = 0;
                for (size_t i = compressBeginIdx; i < r.size(); i++) {
                    compressCount += r[i].cnt;
                }
                if (compressCount == 0) compressCount = 1;
                u64 newCount = r.count() * compressCount;
                Row nr(std::move(r));
                nr.resize(nbs.size() + compressBeginIdx);
                for (size_t i = 0; i < nbs.size(); i++) nr[i + compressBeginIdx] = std::move(nbs[i]);
                nr.EmplaceWithCount(newData, newCount);
            } else {
                for (size_t i = compressBeginIdx; i < r.size(); i++) {
                    newData.emplace_back(r, 0, compressBeginIdx, nbs.size());
                    for (size_t j = 0; j < nbs.size(); j++) newData.back()[j + compressBeginIdx] = nbs[j];
                    newData.back()[compress] = r[i];
                }
            }
        }

        // for (Row &r : newData) LOG(INFO) << "output: " << r.DebugString();

        m.data.swap(newData);
        output.emplace_back(std::move(m));

        return true;
    }

   private:
};
}  // namespace AGE
