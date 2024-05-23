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

#include "base/aggregation.h"
#include <string>
#include "base/expression.h"
#include "base/row.h"
#include "base/type.h"

namespace AGE {
namespace Aggregation {
void UpdateCountOnlyAgg(Expression &exp, const Row *r, const Graph *g, AggCtx *ctxArr, size_t cnt,
                        const Item *curItem) {
    if (exp.type == EXP_AGGFUNC) {
        assert(exp.aggFunc.type == AGG_CNT);
        if (exp.aggFunc.NeedDistinct())
            UpdateAgg(exp, r, g, ctxArr, curItem);
        else
            exp.aggFunc.Update(Item(), ctxArr, cnt);
        return;
    }

    for (Expression &child : exp.GetChildren()) UpdateCountOnlyAgg(child, r, g, ctxArr, cnt, curItem);
}

bool UpdateDistinct(Expression &var, Expression &exp, AggCtx *ctxArr) {
    AggCtx &aggCtx = ctxArr[exp.aggFunc.ctx];
    if (aggCtx.distinctor == nullptr) aggCtx.distinctor = new LocalDistinctor();
    return aggCtx.distinctor->tryToInsert(var.GetValue());
}

void UpdateVal(Expression &var, Expression &root, const Row *r, AggCtx *ctxArr, bool isGlobalAggregation) {
    if (root.aggFunc.type == AGG_CNT && isGlobalAggregation) {
        // Need to update with the local aggregation result
        CHECK_EQ(r->at(var.colIdx).type, ItemType::T_INTEGER);
        int cnt = r->at(var.colIdx).integerVal;
        root.aggFunc.Update(var.GetValue(), ctxArr, cnt);
    } else if (root.aggFunc.NeedDistinct()) {
        root.aggFunc.Update(var.GetValue(), ctxArr, 1);
    } else {
        root.aggFunc.Update(var.GetValue(), ctxArr, r->count());
    }
}

void UpdateAgg(Expression &exp, const Row *r, const Graph *g, AggCtx *ctxArr, const Item *curItem,
               bool isGlobalAggregation) {
    for (Expression &child : exp.GetChildren()) {
        if (exp.type == EXP_AGGFUNC) {
            child.Eval(r, g, curItem);
            if (exp.aggFunc.NeedDistinct() && !UpdateDistinct(child, exp, ctxArr)) continue;
            UpdateVal(child, exp, r, ctxArr, isGlobalAggregation);
        } else {
            UpdateAgg(child, r, g, ctxArr, curItem, isGlobalAggregation);
        }
    }
}

Item GetResult(Expression &exp, AggCtx *ctxArr) {
    for (Expression &child : exp.GetChildren()) {
        GetResult(child, ctxArr);
    }

    if (exp.type == EXP_AGGFUNC)
        exp.SetValue(exp.aggFunc.GetResult(ctxArr));
    else
        exp.Calc();

    return exp.GetValue();
}

void AssignAggCtx(Expression &exp, u32 &nAggFunc) {
    if (exp.type == EXP_AGGFUNC) {
        exp.aggFunc.ctx = nAggFunc++;
    } else {
        for (Expression &child : exp.GetChildren()) {
            AssignAggCtx(child, nAggFunc);
        }
    }
}

void AppendResult(vector<pair<Expression, ColIndex>> &exps, vector<Row> &newData, AggCtx *AggCtx, const Row &key,
                  u32 nKey, u32 nAggFunc) {
    u32 nExpr = nKey + nAggFunc;
    CHECK(nExpr <= exps.size()) << "nKey + nAggFunc > exps.size() with nKey = " << nKey
                                << " and nAggFunc = " << nAggFunc << " and exps.size() = " << exps.size();
    for (u32 itr = 0; itr < nExpr; itr++) {
        Item result = itr >= nKey ? GetResult(exps[itr].first, AggCtx) : key[itr];
        if (exps[itr].second == COLINDEX_COMPRESS)
            newData.back().emplace_back(std::move(result));
        else if (exps[itr].second != COLINDEX_NONE)
            newData.back()[exps[itr].second] = std::move(result);
    }
}

}  // namespace Aggregation.
}  // namespace AGE
