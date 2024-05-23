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

#include "base/row.h"
#include "base/serializer.h"
#include "base/type.h"
#include "execution/physical_plan.h"
#include "operator/abstract_op.h"
#include "plan/op_params.h"
#include "storage/graph.h"

namespace AGE {

using std::string;

// Expand between two existing vertices
// Checking whether there is an eligible edge to connect that two vertices
class ExpandIntoOp : public AbstractOp {
   public:
    ExpandIntoOp() : AbstractOp(OpType_EXPAND_INTO) {}
    ExpandIntoOp(ColIndex src, ColIndex dst, LabelId lid, DirectionType dir, u32 compressBeginIdx)
        : AbstractOp(OpType_EXPAND_INTO, compressBeginIdx), src(src), dst(dst), lid(lid), dir(dir) {
        swapTwoEnd();
        SetStorageFetchCol(this->src);
    }

    void ToString(string *s) const override {
        AbstractOp::ToString(s);
        Serializer::appendI32(s, src);
        Serializer::appendI32(s, dst);
        Serializer::appendVar(s, dir);
        Serializer::appendVar(s, lid);
    }

    void FromString(const string &s, size_t &pos) override {
        AbstractOp::FromString(s, pos);
        src = Serializer::readI32(s, pos);
        dst = Serializer::readI32(s, pos);
        Serializer::readVar(s, pos, &dir);
        Serializer::readVar(s, pos, &lid);
        swapTwoEnd();
        SetStorageFetchCol(this->src);
    }

    void FromPhysicalOpParams(const PhysicalOpParams &params) override {
        AbstractOp::FromPhysicalOpParams(params);
        src = params.cols[0];
        dst = params.cols[1];
        size_t pos = 0;
        Serializer::readVar(params.params, pos, &dir);
        Serializer::readVar(params.params, pos, &lid);
        swapTwoEnd();
        SetStorageFetchCol(this->src);
    }

    std::string DebugString(int depth = 0) const override {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s [%d]%s[%d]", AbstractOp::DebugString(depth).c_str(), src,
                 Edge_DebugString(lid, dir).c_str(), dst);
        return buf;
    }

    void swapTwoEnd() {
        // Make sure that CUR COLUMN is src.
        if (dst == COLINDEX_COMPRESS) {
            std::swap(src, dst);
            dir = DirectionType_Reverse(dir);
        }
    }

    std::string DebugString() {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s([%d]%s[%d])", AbstractOp::DebugString().c_str(), src,
                 Edge_DebugString(lid, dir).c_str(), dst);
        return buf;
    }

    SingleProcStat::ColStat ExtractColStat(const Message &msg) const override {
        return std::make_tuple(src == COLINDEX_COMPRESS, dst == COLINDEX_COMPRESS, 0);
    }

    bool isConnect(Graph *g, const Item *u, const Item *v) {
        // TODO(ycli): move this into storage.
        DirectionType curDir = dir;
        u32 u_neighbor_cnt = g->getNeighborCnt(*u, lid, dir);
        // u32 v_neighbor_cnt = g->getNeighborCnt(*v, lid, DirectionType_Reverse(dir));
        if (u_neighbor_cnt == 0) return false;
        // if (g->getNeighborCnt(*u, lid, dir) > g->getNeighborCnt(*v, lid, dir)) {
        //     std::swap(u, v);
        //     curDir = DirectionType_Reverse(dir);
        // }
        // printf("curDir: %s\n", DirTypeStr[curDir]);
        vector<Item> nb = g->getNeighbor(*u, lid, curDir);
        if (lid == 0 || curDir == DirectionType_BOTH) {
            return std::find(nb.begin(), nb.end(), *v) != nb.end();
        } else {
            auto itr = std::lower_bound(nb.begin(), nb.end(), *v);
            return itr != nb.end() && *itr == *v;
        }
    }

    bool process(Message &m, std::vector<Message> &output) override {
        // TODO(ycli): optimize this op.
        // TODO(ycli): testing.
        Graph *g = m.plan->g;

        vector<Row> newData;
        for (Row &r : m.data) {
            // print(r);
            if (src == COLINDEX_COMPRESS) {
                vector<size_t> passed;
                for (size_t i = compressBeginIdx; i < r.size(); i++) {
                    Item &dv = dst == COLINDEX_COMPRESS ? r[i] : r[dst];
                    if (isConnect(g, &r[i], &dv)) passed.push_back(i);
                }
                for (size_t i = 0; i < passed.size(); i++) std::swap(r[i + compressBeginIdx], r[passed[i]]);
                if (passed.size() > 0) {
                    r.resize(compressBeginIdx + passed.size());
                    newData.emplace_back(std::move(r));
                }
            } else {
                if (isConnect(g, &r[src], &r[dst])) newData.emplace_back(std::move(r));
            }
        }

        m.data.swap(newData);
        output.emplace_back(std::move(m));
        return true;
    }

    ColIndex src, dst;
    LabelId lid;
    DirectionType dir;
};
}  // namespace AGE
