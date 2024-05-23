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
#include <cassert>
#include <cstddef>
#include <iostream>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "operator/aggregate_op.h"

using std::pair;
using std::vector;

namespace AGE {

// Aggregation operator
class GlobalAggregateOp : public AggregateOp {
   public:
    // After agg op, every bunch only have one column.
    // The column order of agg op result is {group key 1, group key 2, ..., group key n, agg val, unsed columns}.
    explicit GlobalAggregateOp(u32 compressBeginIdx = COMPRESS_BEGIN_IDX_UNDECIDED)
        : AggregateOp(OpType_AGGREGATE_GLOBAL, compressBeginIdx) {}
    GlobalAggregateOp(const vector<pair<Expression, ColIndex>> &exps, u32 compressBeginIdx, u32 nKey,
                      u32 newCompressBeginIdx_, const vector<pair<Expression, isASC>> *orderbyExps = nullptr,
                      int limitNum_ = LimitUtil::WITHOUT_LIMIT)
        : AggregateOp(OpType_AGGREGATE_GLOBAL, exps, compressBeginIdx, nKey, newCompressBeginIdx_, orderbyExps,
                      limitNum_) {}

    void UpdateAggregation(Expression &expr, const Row *r, const Graph *g, AggCtx *ctxArr,
                           const Item *curItem = nullptr) override {
        Aggregation::UpdateAgg(expr, r, g, ctxArr, curItem, true);
    }
};

}  // namespace AGE
