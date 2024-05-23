// Copyright 2021 HDL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,[]
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
#include <glog/logging.h>
#include <cassert>
#include <iostream>
#include <set>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include "base/expr_util.h"
#include "base/expression.h"
#include "base/serializer.h"
#include "base/type.h"
#include "execution/physical_plan.h"
#include "operator/abstract_op.h"

using std::vector;

namespace AGE {
#define _GRAPH_ITEM_ITSELF_ static_cast<PropId>(0)

// Operator to project variables so that
// it will be used in next query stage
// or return as result
class ProjectOp : public AbstractOp {
   public:
    explicit ProjectOp(u32 compressBeginIdx = COMPRESS_BEGIN_IDX_UNDECIDED)
        : AbstractOp(OpType_PROJECT, compressBeginIdx) {}

    // The column order of project op result is the same with the order of input Expression*.
    ProjectOp(const vector<pair<Expression, ColIndex>> &exps, u32 compressBeginIdx, u32 newCompressBeginIdx_)
        : ProjectOp(compressBeginIdx) {
        newCompressBeginIdx = newCompressBeginIdx_;
        for (auto &p : exps) AppendProject(p);
    }

    std::string DebugString(int depth = 0) const override {
        std::string ret = AbstractOp::DebugString(depth) + " dstCols: [";
        for (const auto &[expr, col] : exps) ret += to_string(col) + ", ";
        ret += "]";
        return ret;
    }

    void FromPhysicalOpParams(const PhysicalOpParams &params) override {
        AbstractOp::FromPhysicalOpParams(params);

        const string &s = params.params;
        size_t pos = 0;
        for (size_t i = 0; i < params.exprs.size(); i++) {
            AppendProject(std::make_pair(params.exprs[i], params.cols[i]));
        }
        u8 expectExpsSize = Serializer::readU8(s, pos);
        CHECK(expectExpsSize == exps.size());
        newCompressBeginIdx = Serializer::readU32(s, pos);
    }

    void ToString(string *s) const override {
        AbstractOp::ToString(s);
        Serializer::appendU8(s, exps.size());
        Serializer::appendU8(s, newCompressBeginIdx);
        for (auto &p : exps) {
            ExprUtil::ToString(s, &p.first);
            Serializer::appendI8(s, p.second);
        }
    }

    void FromString(const string &s, size_t &pos) override {
        AbstractOp::FromString(s, pos);
        u8 expsSize = Serializer::readU8(s, pos);
        newCompressBeginIdx = Serializer::readU8(s, pos);
        while (expsSize--) {
            Expression exp = ExprUtil::CreateFromString(s, pos);
            ColIndex col = Serializer::readI8(s, pos);
            AppendProject(std::make_pair(exp, col));
        }
    }

    SingleProcStat::ColStat ExtractColStat(const Message &msg) const override {
        return std::make_tuple(HasCompress(msg), true, 2);
    }

    bool process(Message &m, std::vector<Message> &output) override {
        // printf("ProjectOp::process(): %d\n", hasCompress);
        Graph *g = m.plan->g;

        // We assume that the layouts of rows in one message are consistent.
        bool hasCompress = HasCompress(m);

        vector<Row> newData;
        if (hasCompress) {
            for (Row &r : m.data) {
                // printf("%s\n", r.DebugString().c_str());
                for (u32 i = compressBeginIdx; i < r.size(); i++) {
                    newData.emplace_back(newCompressBeginIdx);
                    newData.back().setCount(r.count() * r[i].cnt);
                    for (size_t j = 0; j < exps.size(); j++) {
                        Item result = exps[j].first.Eval(&r, g, &r[i]);
                        if (exps[j].second == COLINDEX_COMPRESS) {
                            newData.back().emplace_back(std::move(result));
                        } else if (exps[j].second != COLINDEX_NONE) {
                            newData.back()[exps[j].second] = std::move(result);
                        }
                    }
                }
            }
        } else {
            for (Row &r : m.data) {
                newData.emplace_back(newCompressBeginIdx);
                newData.back().setCount(r.count());
                for (size_t i = 0; i < exps.size(); i++) {
                    Item result = exps[i].first.Eval(&r, g);
                    if (exps[i].second == COLINDEX_COMPRESS) {
                        newData.back().emplace_back(std::move(result));
                    } else if (exps[i].second != COLINDEX_NONE) {
                        newData.back()[exps[i].second] = std::move(result);
                    }
                }
            }
        }

        m.data.swap(newData);
        output.emplace_back(std::move(m));
        return true;
    }

    vector<pair<Expression, ColIndex>> exps;

    // private:
    u32 newCompressBeginIdx;

   private:
    void AppendProject(const pair<Expression, ColIndex> &p) { exps.emplace_back(p); }
};
}  // namespace AGE
