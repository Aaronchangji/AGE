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
#include <sstream>
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
#include "base/type.h"
#include "execution/graph_tool.h"
#include "execution/message.h"
#include "execution/physical_plan.h"
#include "operator/abstract_op.h"
#include "operator/base_barrier_op.h"
#include "operator/op_type.h"
#include "plan/op_params.h"
#include "storage/graph.h"

using std::pair;
using std::vector;

namespace AGE {

// Aggregation operator
class AggregateOp : public BaseBarrierOp {
   public:
    // After agg op, every bunch only have one column.
    // The column order of agg op result is {group key 1, group key 2, ..., group key n, agg val, unused columns}.
    explicit AggregateOp(OpType type, u32 compressBeginIdx = COMPRESS_BEGIN_IDX_UNDECIDED)
        : BaseBarrierOp(type, compressBeginIdx),
          nKey(0),
          nAggFunc(0),
          limitNum(LimitUtil::WITHOUT_LIMIT),
          hasOrder(false),
          init(false) {
        // Must call FromPhysicalOpParams to complete initialization
    }
    AggregateOp(OpType type, const vector<pair<Expression, ColIndex>>& exps, u32 compressBeginIdx, u32 nKey_,
                u32 newCompressBeginIdx_, const vector<pair<Expression, isASC>>* orderbyExps = nullptr,
                int limitNum_ = LimitUtil::WITHOUT_LIMIT)
        : AggregateOp(type, compressBeginIdx) {
        nKey = nKey_;
        newCompressBeginIdx = newCompressBeginIdx_;
        limitNum = limitNum_;
        Init(&exps, orderbyExps);
    }
    // construct from string
    void FromString(const string& s, size_t& pos) override {
        AbstractOp::FromString(s, pos);
        newCompressBeginIdx = Serializer::readU32(s, pos);
        limitNum = Serializer::readI32(s, pos);
        nKey = Serializer::readU32(s, pos);

        u8 expsLen = Serializer::readU8(s, pos);
        while (expsLen--) {
            Expression exp = ExprUtil::CreateFromString(s, pos);
            ColIndex col = Serializer::readI8(s, pos);
            AppendProject(std::make_pair(exp, col));
        }

        vector<pair<Expression, isASC>> orderbyExps;
        u8 orderExpsLen = Serializer::readU8(s, pos);
        while (orderExpsLen--) {
            Expression exp = ExprUtil::CreateFromString(s, pos);
            isASC asc = Serializer::readBool(s, pos);
            orderbyExps.emplace_back(std::make_pair(exp, asc));
        }
        Init(nullptr, &orderbyExps);
    }

    void FromPhysicalOpParams(const PhysicalOpParams& params) override {
        AbstractOp::FromPhysicalOpParams(params);

        const string& s = params.params;
        size_t pos = 0;

        u8 expsLen = Serializer::readU8(s, pos);
        nKey = Serializer::readU8(s, pos);
        u8 orderExpsLen = Serializer::readU8(s, pos);
        assert(expsLen && (expsLen + orderExpsLen == params.exprs.size()));

        vector<pair<Expression, ColIndex>> exps;
        vector<pair<Expression, isASC>> orderExps;
        for (u8 i = 0; i < expsLen; i++) exps.emplace_back(std::make_pair(params.exprs[i], params.cols[i]));
        for (u8 i = 0; i < orderExpsLen; i++) {
            isASC asc = Serializer::readBool(s, pos);
            orderExps.emplace_back(std::make_pair(params.exprs[i + expsLen], asc));
        }

        limitNum = Serializer::readI32(s, pos);
        Serializer::readBool(s, pos);  // Distinct(bool) is not used in this op
        newCompressBeginIdx = Serializer::readU32(s, pos);
        Init(&exps, &orderExps);
    }

    void ToString(string* s) const override {
        AbstractOp::ToString(s);
        Serializer::appendU32(s, newCompressBeginIdx);
        Serializer::appendI32(s, limitNum);
        Serializer::appendU32(s, nKey);
        Serializer::appendU8(s, exps.size());
        for (auto& p : exps) {
            ExprUtil::ToString(s, &p.first);
            Serializer::appendI8(s, p.second);
        }
        Serializer::appendU8(s, orderbyExps.size());
        for (auto& p : orderbyExps) {
            ExprUtil::ToString(s, &p.first);
            Serializer::appendBool(s, p.second);
        }
    }

    string DebugString(int depth = 0) const override {
        stringstream buf;
        buf << AbstractOp::DebugString(depth);
        buf << "limitNum: " << limitNum;
        for (u8 i = 0; i < exps.size(); i++) {
            buf << "\n";
            for (u8 j = 0; j <= depth; j++) buf << "\t";
            buf << (i < nKey ? "key:" : "val:") << exps[i].first.DebugString(depth + 2);
            for (u8 j = 0; j <= depth; j++) buf << "\t";
            buf << "-> " << exps[i].second;
        }
        for (const auto& exp : orderbyExps) {
            buf << "\n";
            for (u8 j = 0; j <= depth; j++) buf << "\t";
            buf << "orderby:" << exp.first.DebugString(depth + 2);
            for (u8 j = 0; j <= depth; j++) buf << "\t";
            buf << ", " << (exp.second ? "ASC" : "DSC");
        }
        return buf.str();
    }

    // agg op has the ownership of its Expression.
    ~AggregateOp() {
        for (auto p : aggMap) delete[] p.second;
    }

    // A non-existing key will be emplaced in aggMap and assigned a new aggCtx.
    // A new key signals a new row (that could be ordered) after aggregation
    pair<AggCtx*, bool> GetAggCtx(const Row& key) {
        auto itr = aggMap.find(key);
        if (itr == aggMap.end()) {
            AggCtx* ret = new AggCtx[nAggFunc];
            aggMap.emplace(key, ret);
            return std::make_pair(ret, true);
        }
        return std::make_pair(itr->second, false);
    }

    SingleProcStat::ColStat ExtractColStat(const Message& msg) const override {
        return std::make_tuple(HasCompress(msg), true, 2);
    }

    bool do_work(Message& m, std::vector<Message>& output, bool isReady) override {
        assert(init);
        Graph* g = m.plan->g;

        // We assume that the layouts of rows in one message are consistent
        bool hasCompress = HasCompress(m);

        // Get orderHasAgg and order2exp mapping
        vector<int> order2exp(orderbyExps.size());
        if (hasOrder) {
            OrderUtil::OrderCheck(orderbyExps, orderHasAgg, order2exp, exps, nKey);
        }

        if (hasCompress) {
            for (auto& row : m.data) {
                for (u32 i = compressBeginIdx; i < row.size(); i++) {
                    Row key(nKey);
                    for (u32 j = 0; j < nKey; j++) key[j] = exps[j].first.Eval(&row, g, &row[i]);

                    auto [aggCtx, isNewCtx] = GetAggCtx(key);
                    for (u32 j = nKey; j < exps.size(); j++) UpdateAggregation(exps[j].first, &row, g, aggCtx, &row[i]);

                    // if we get a new aggregated row that needs to be ordered...
                    if (hasOrder && isNewCtx) {
                        OrderUtil::AddRawOrderbyMap(rawOrderbyMap, key, orderbyExps, order2exp, row, g, i);
                    }
                }
            }
        } else {
            for (auto& row : m.data) {
                Row key(nKey);
                for (u32 j = 0; j < nKey; j++) key[j] = exps[j].first.Eval(&row, g);

                auto [aggCtx, isNewCtx] = GetAggCtx(key);
                for (u32 j = nKey; j < exps.size(); j++) UpdateAggregation(exps[j].first, &row, g, aggCtx);

                if (hasOrder && isNewCtx) {
                    OrderUtil::AddRawOrderbyMap(rawOrderbyMap, key, orderbyExps, order2exp, row, g, -1);
                }
            }
        }

        if (isReady) {
            vector<Row> newData;
            LimitUtil limit(limitNum);
            if (hasOrder) {
                OrderUtil::BuildOrderbyMap(orderbyMap, rawOrderbyMap, aggMap, orderHasAgg, order2exp,
                                           orderbyExps.size(), exps);
                for (auto& [orderbyKey, key] : orderbyMap) {
                    // could have WITHOUT_LIMIT or limitNum == 0 (hence must check before appending rows)
                    if (!limit.checkCnt()) break;

                    newData.emplace_back(newCompressBeginIdx);
                    AppendResult(newData, aggMap[key], key, orderbyKey);

                    if (!limit.updateCnt(newData.back())) break;
                }
            } else {
                for (auto [key, aggCtx] : aggMap) {
                    if (!limit.checkCnt()) break;

                    newData.emplace_back(newCompressBeginIdx);
                    AppendResult(newData, aggCtx, key);

                    if (!limit.updateCnt(newData.back())) break;
                }
            }

            m.data.swap(newData);
            output.emplace_back(std::move(m));
        }

        return isReady;
    }

    // private:
    vector<pair<Expression, ColIndex>> exps;
    vector<pair<Expression, isASC>> orderbyExps;
    u32 nKey, nAggFunc;
    u32 newCompressBeginIdx;
    AggMap aggMap;

    int limitNum = LimitUtil::WITHOUT_LIMIT;
    bool hasOrder = false, orderHasAgg = false;
    OrderUtil::RawOrderbyMap rawOrderbyMap;
    OrderUtil::OrderbyMap orderbyMap;

   protected:
    bool init;
    void AppendProject(const pair<Expression, ColIndex>& p) {
        exps.emplace_back(p);
        Expression& expr = exps.back().first;
        bool hasAgg = expr.HasAggregate();
        if (hasAgg) Aggregation::AssignAggCtx(expr, nAggFunc);
    }

    void Init(const vector<pair<Expression, ColIndex>>* exps_ = nullptr,
              const vector<pair<Expression, isASC>>* orderbyExps_ = nullptr) {
        assert(!init);

        if (exps_ != nullptr) {
            for (auto& p : *exps_) AppendProject(p);
        }
        if (orderbyExps_ != nullptr) {
            orderbyExps = *orderbyExps_;
            hasOrder = orderbyExps.size() > 0;
            orderbyMap = OrderUtil::OrderbyMap(OrderUtil::OrderSort(orderbyExps));
        }
        aggMap = AggMap();

        init = true;
    }

    virtual void UpdateAggregation(Expression& expr, const Row* r, const Graph* g, AggCtx* ctxArr,
                                   const Item* curItem = nullptr) {
        Aggregation::UpdateAgg(expr, r, g, ctxArr, curItem, false);
    }

    virtual void AppendResult(vector<Row>& newData, AggCtx* AggCtx, const Row& key, const Row& orderbyKey = Row()) {
        Aggregation::AppendResult(exps, newData, AggCtx, key, nKey, nAggFunc);
    }
};

}  // namespace AGE
