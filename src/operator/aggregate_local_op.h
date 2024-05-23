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
class LocalAggregateOp : public AggregateOp {
   public:
    // After agg op, every bunch only have one column.
    // The column order of agg op result is {group key 1, group key 2, ..., group key n, agg val, unsed columns}.
    explicit LocalAggregateOp(u32 compressBeginIdx = COMPRESS_BEGIN_IDX_UNDECIDED)
        : AggregateOp(OpType_AGGREGATE_LOCAL, compressBeginIdx) {}
    LocalAggregateOp(const vector<pair<Expression, ColIndex>>& exps, u32 compressBeginIdx, u32 nKey,
                     u32 newCompressBeginIdx_, const vector<pair<Expression, isASC>>* orderbyExps = nullptr,
                     int limitNum_ = LimitUtil::WITHOUT_LIMIT)
        : AggregateOp(OpType_AGGREGATE_LOCAL, exps, compressBeginIdx, nKey, newCompressBeginIdx_, orderbyExps,
                      limitNum_) {}

    uint8_t get_end_history_size(Message& msg) override {
        uint8_t end_path_size = 0;
        end_path_size = msg.header.globalHistorySize;
        return end_path_size;
    }

    void AppendResult(vector<Row>& newData, AggCtx* AggCtx, const Row& key, const Row& orderbyKey = Row()) override {
        Aggregation::AppendResult(exps, newData, AggCtx, key, nKey, nAggFunc);
        // Local aggregation also need to process the order by keys
        uint32_t nOrderbyExpr = exps.size() - nKey - nAggFunc;
        if (!nOrderbyExpr) {
            return;
        }

        CHECK(orderbyKey.size() == nOrderbyExpr)
            << "orderbyKey size does not equal to expression size"
            << " orderbyKey size: " << orderbyKey.size() << " expression size: " << exps.size() - nKey - nAggFunc;
        for (size_t i = 0; i < orderbyKey.size(); i++) {
            if (exps[i + nKey + nAggFunc].second == COLINDEX_COMPRESS) {
                newData.back().emplace_back(orderbyKey[i]);
            } else if (exps[i + nKey + nAggFunc].second != COLINDEX_NONE) {
                newData.back()[exps[i + nKey + nAggFunc].second] = orderbyKey[i];
            }
        }
    }
};

}  // namespace AGE
