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
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include "base/expr_util.h"
#include "base/expression.h"
#include "execution/physical_plan.h"
#include "operator/abstract_op.h"
#include "plan/op_params.h"

namespace AGE {
// Filter operator to filter out rows who do not meet the requirements provided by query
class FilterOp : public AbstractOp {
   public:
    FilterOp() : AbstractOp(OpType_FILTER), filter() {}
    explicit FilterOp(const Expression &filter_, u32 compressBeginIdx) : AbstractOp(OpType_FILTER, compressBeginIdx) {
        filter = filter_;
    }

    ~FilterOp() {}

    void ToString(string *s) const override {
        AbstractOp::ToString(s);
        ExprUtil::ToString(s, &filter);
    }

    void FromString(const string &s, size_t &pos) override {
        AbstractOp::FromString(s, pos);
        filter = ExprUtil::CreateFromString(s, pos);
    }

    std::string DebugString(int depth = 0) const override {
        string ret = AbstractOp::DebugString(depth) + " filter:";
        ret += filter.DebugString(depth + 1);
        return ret;
    }

    void FromPhysicalOpParams(const PhysicalOpParams &params) override {
        AbstractOp::FromPhysicalOpParams(params);
        filter = params.exprs[0];
    }

    SingleProcStat::ColStat ExtractColStat(const Message &msg) const override {
        return std::make_tuple(HasCompress(msg), false, 1);
    }

    Expression getFilter() { return filter; }

    bool process(Message &m, std::vector<Message> &output) override {
        // Each process call should have a local copy of the filter expression (i.e., to avoid concurrent conflicts)
        // TBD: maybe a stateless version of expression
        Expression localFilter = getFilter();
        Graph *graph = m.plan->g;

        // We assume that the layouts of rows in one message are consistent.
        bool hasCompress = HasCompress(m);

        std::vector<Row> newData;
        if (hasCompress) {
            for (Row &r : m.data) {
                u32 passCnt = 0;
                for (u32 i = compressBeginIdx; i < r.size(); i++) {
                    Item res = localFilter.Eval(&r, graph, &r[i]);
                    // LOG(INFO) << r[i].DebugString() << " -> " << res.DebugString();
                    if (res) {
                        if (i > passCnt + compressBeginIdx) r[passCnt + compressBeginIdx] = std::move(r[i]);
                        passCnt++;
                    }
                }
                if (passCnt == 0) continue;

                r.resize(passCnt + compressBeginIdx);
                newData.emplace_back(std::move(r));
            }
        } else {
            for (Row &r : m.data) {
                Item res = localFilter.Eval(&r, graph);
                // LOG(INFO) << r.DebugString() << " -> " << res.DebugString();
                if (res) newData.emplace_back(std::move(r));
            }
        }

        // for (Row &r : newData) LOG(INFO) << r.DebugString();

        m.data.swap(newData);
        output.emplace_back(std::move(m));

        return true;
    }

   private:
    Expression filter;
};
}  // namespace AGE
