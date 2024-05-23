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
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "base/aggregation.h"
#include "base/expr_util.h"
#include "base/expression.h"
#include "base/math.h"
#include "base/proj_util.h"
#include "base/row.h"
#include "base/serializer.h"
#include "execution/graph_tool.h"
#include "execution/message.h"
#include "execution/physical_plan.h"
#include "operator/abstract_op.h"
#include "operator/aggregate_op.h"
#include "operator/base_barrier_op.h"
#include "plan/op_params.h"
#include "storage/graph.h"

using std::vector;

namespace AGE {

// A optimized version for a special case of AggregateOp
class CountOp : public AggregateOp {
   public:
    CountOp() : AggregateOp(OpType_COUNT) {}
    CountOp(const vector<pair<Expression, ColIndex>>& exps, u32 compressBeginIdx, u32 nKey, u32 newCompressBeginIdx,
            const vector<pair<Expression, isASC>>* orderbyExps = nullptr, int limitNum = LimitUtil::WITHOUT_LIMIT)
        : AggregateOp(OpType_COUNT, exps, compressBeginIdx, nKey, newCompressBeginIdx, orderbyExps, limitNum) {}

    // C++ will call destructor of AggregationOp by default
    ~CountOp() {}

    bool do_work(Message& m, std::vector<Message>& output, bool isReady) override {
        // printf("Count op:\n");
        Graph* g = m.plan->g;

        vector<int> order2exp(orderbyExps.size());
        if (hasOrder) {
            OrderUtil::OrderCheck(orderbyExps, orderHasAgg, order2exp, exps, nKey);
        }

        // We assume that the layouts of rows in one message are consistent.
        bool hasCompress = HasCompress(m);

        for (auto& row : m.data) {
            Row key(nKey);
            for (u32 j = 0; j < nKey; j++) key[j] = exps[j].first.Eval(&row, g);
            size_t cnt = 0;
            if (hasCompress) {
                for (size_t i = compressBeginIdx; i < row.size(); i++) cnt += row[i].cnt;
            } else {
                cnt = 1;
            }
            if (row.count() * cnt == 0) continue;

            auto [aggCtx, isNewCtx] = GetAggCtx(key);
            for (u32 aggFIndex = nKey; aggFIndex < exps.size(); aggFIndex++) {
                if (exps[aggFIndex].first.HasDistinct() && hasCompress) {
                    for (u32 i = compressBeginIdx; i < row.size(); i++)
                        Aggregation::UpdateCountOnlyAgg(exps[aggFIndex].first, &row, g, aggCtx, cnt * row.count(),
                                                        &row[i]);
                } else {
                    Aggregation::UpdateCountOnlyAgg(exps[aggFIndex].first, &row, g, aggCtx, cnt * row.count());
                }
            }

            if (hasOrder && isNewCtx) {
                OrderUtil::AddRawOrderbyMap(rawOrderbyMap, key, orderbyExps, order2exp, row, g, -1);
            }
        }

        if (isReady) {
            vector<Row> newData;
            LimitUtil limit(limitNum);
            if (hasOrder) {
                OrderUtil::BuildOrderbyMap(orderbyMap, rawOrderbyMap, aggMap, orderHasAgg, order2exp,
                                           orderbyExps.size(), exps);
                for (auto& [orderbyKey, key] : orderbyMap) {
                    if (!limit.checkCnt()) break;

                    newData.emplace_back(newCompressBeginIdx);
                    Aggregation::AppendResult(exps, newData, aggMap[key], key, nKey, nAggFunc);

                    if (!limit.updateCnt(newData.back())) break;
                }
            } else {
                for (auto [key, aggCtx] : aggMap) {
                    if (!limit.checkCnt()) break;

                    newData.emplace_back(newCompressBeginIdx);
                    Aggregation::AppendResult(exps, newData, aggCtx, key, nKey, nAggFunc);

                    if (!limit.updateCnt(newData.back())) break;
                }
            }

            m.data.swap(newData);
            output.emplace_back(std::move(m));
        }

        return isReady;
    }
};
}  // namespace AGE
