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

// Expand from normal column to normal column
namespace AGE {
class ExpandNNOp : public ExpandOp {
   public:
    ExpandNNOp() : ExpandOp(OpType_EXPAND_NN) {}
    // Note that srcCol may be same as dstCol when variable in srcCol should be drop after this op
    ExpandNNOp(ColIndex src, ColIndex dst, ColIndex compress, LabelId eLabel, LabelId dstVLabel, DirectionType dir,
               u32 compressBeginIdx)
        : ExpandOp(OpType_EXPAND_NN, src, dst, compress, eLabel, dstVLabel, dir, compressBeginIdx) {
        assert(src != COLINDEX_COMPRESS && dst != COLINDEX_COMPRESS);
    }

    bool process(Message &m, std::vector<Message> &output) override {
        Graph *g = m.plan->g;
        vector<Row> newData;

        for (Row &r : m.data) {
            vector<Item> nbs = g->getNeighbor(r[src], eLabel, dir, dstVLabel);
            for (Item &nb : nbs) {
                if (compress == COLINDEX_COMPRESS) {
                    newData.emplace_back(r);
                    newData.back()[dst] = std::move(nb);
                } else if (compress == COLINDEX_NONE) {
                    // Note that r.size() == compressBeginIdx means compress col not exist
                    u64 compressCount = 0;
                    for (size_t i = compressBeginIdx; i < r.size(); i++) {
                        compressCount += r[i].cnt;
                    }
                    if (compressCount == 0) compressCount = 1;
                    u64 newCount = r.count() * compressCount;
                    Row nr(r, 0, compressBeginIdx);
                    nr[dst] = std::move(nb);
                    nr.EmplaceWithCount(newData, newCount);
                } else {
                    for (size_t i = compressBeginIdx; i < r.size(); i++) {
                        // newData.emplace_back(r.begin(), r.begin() + compressBeginIdx);
                        newData.emplace_back(r, 0, compressBeginIdx);
                        newData.back()[compress] = r[i];
                        newData.back()[dst] = nb;
                    }
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
