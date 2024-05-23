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

// Note that srcCol may be same as dstCol, since some col is not supposed to used then,
// planner may use some col as temp col.

using std::vector;

namespace AGE {
class ExpandULOp : public ExpandOp {
   public:
    ExpandULOp() : ExpandOp(OpType_EXPAND_UL) {}
    // lid == 0 for all label.
    ExpandULOp(ColIndex src, ColIndex dst, ColIndex compress, LabelId eLabel, LabelId dstVLabel, DirectionType dir,
               u32 compressBeginIdx)
        : ExpandOp(OpType_EXPAND_UL, src, dst, compress, eLabel, dstVLabel, dir, compressBeginIdx) {
        assert(dst == COLINDEX_NONE);
        // assert(!(src == COLINDEX_COMPRESS && compress == COLINDEX_COMPRESS));
    }

    size_t getNeighborCnt(Graph *g, const Item &item, LabelId eLabel, LabelId dstVLabel, DirectionType dir) {
        if (dstVLabel == ALL_LABEL) return g->getNeighborCnt(item, eLabel, dir);
        vector<Item> nbs = g->getNeighbor(item, eLabel, dir, dstVLabel);
        return nbs.size();
    }

    bool process(Message &m, std::vector<Message> &output) override {
        Graph *g = m.plan->g;
        vector<Row> newData;

        if (src == COLINDEX_COMPRESS) {
            for (Row &r : m.data)
                for (size_t i = compressBeginIdx; i < r.size(); i++) {
                    size_t cnt = getNeighborCnt(g, r[i], eLabel, dstVLabel, dir);
                    if (cnt == 0) continue;
                    // vector<Item> nbs = g->getNeighbor(r[i], eLabel, dir);
                    if (compress == COLINDEX_COMPRESS) {
                        Row nr(r, 0, compressBeginIdx, 1);
                        nr[compressBeginIdx] = std::move(r[i]);
                        nr.EmplaceWithCount(newData, r.count() * cnt);
                    } else {
                        Row nr(r, 0, compressBeginIdx);
                        if (compress != COLINDEX_NONE) nr[compress] = std::move(r[i]);
                        nr.EmplaceWithCount(newData, r.count() * cnt);
                    }
                }
        } else {
            for (Row &r : m.data) {
                size_t cnt = getNeighborCnt(g, r[src], eLabel, dstVLabel, dir);
                if (cnt == 0) continue;
                if (compress == COLINDEX_COMPRESS) {
                    r.EmplaceWithCount(newData, r.count() * cnt);
                } else if (compress == COLINDEX_NONE) {
                    // Note that r.size() == compressBeginIdx means compress col not exist
                    u32 compressCnt = static_cast<i64>(r.size()) == compressBeginIdx ? 1 : r.size() - compressBeginIdx,
                        newCnt = r.count() * cnt * compressCnt;
                    r.resize(compressBeginIdx);
                    r.EmplaceWithCount(newData, newCnt);
                } else {
                    for (size_t j = compressBeginIdx; j < r.size(); j++) {
                        Row nr(r, 0, compressBeginIdx);
                        nr[compress] = std::move(r[j]);
                        nr.EmplaceWithCount(newData, r.count() * cnt);
                    }
                }
            }
        }

        // for (Row& r : newData) printf("%s\n", r.DebugString().c_str());

        m.data.swap(newData);
        output.emplace_back(std::move(m));

        return true;
    }

   private:
};
}  // namespace AGE
