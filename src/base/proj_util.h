
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

#include <functional>
#include <map>
#include <utility>
#include <vector>
#include "base/aggregation.h"
#include "base/expr_util.h"
#include "base/expression.h"
#include "base/item.h"
#include "base/row.h"
#include "base/type.h"
#include "glog/logging.h"

using std::vector;
using isASC = bool;

namespace AGE {
class OrderUtil {
   public:
    static const int NONAGG = -1;
    static const bool ASC = true;
    static const bool DSC = false;

    /**
     * @brief function to compare two rows. Support custom orders specified by orderbyExp
     */
    struct RowCmp {
        RowCmp() : orderbyExp(0) {}
        explicit RowCmp(const vector<pair<Expression, isASC>> &orderbyExp_) : orderbyExp(orderbyExp_) {}
        int operator()(const Row &lhs, const Row &rhs) const {
            CHECK(lhs.size() == rhs.size() && lhs.size() >= orderbyExp.size());
            for (u32 i = 0; i < lhs.size(); i++) {
                isASC order = i >= orderbyExp.size() ? OrderUtil::ASC : orderbyExp[i].second;
                int res = Item::Compare(lhs[i], rhs[i]);
                if (res == 0) continue;
                return res * (order ? 1 : -1);
            }
            return 0;
        }

       private:
        vector<pair<Expression, isASC>> orderbyExp;
    };

    /**
     * @brief Function to order two rows. Rows not distinguishable by rowCmp are treated as the same rows
     * TODO: remove unused orderByExps.first.
     */
    struct OrderSort {
        OrderSort() : orderedCmp() {}
        explicit OrderSort(const vector<pair<Expression, isASC>> &orderbyExp_) : orderedCmp(orderbyExp_) {}
        bool operator()(const Row &lhs, const Row &rhs) const {
            int res = orderedCmp(lhs, rhs);
            return res != 0 ? res < 0 : false;
        }

       private:
        RowCmp orderedCmp;
    };
    /**
     * @brief Comparator used when we need both distinction and ordering. Comparing rule:
     * For <orderByKey, Row> lhs and <orderByKey, Row> rhs:
     *   if lhs.Row == rhs.Row : false (Distinction)
     *   else : order lhs and rhs according to their orderByKeys (Ordering)
     * Rows that are different but have the same ORDER BY keys are compared by themselves
     */
    struct DistinctSort {
        DistinctSort() : orderedCmp(), cmp() {}
        explicit DistinctSort(const vector<pair<Expression, isASC>> &orderByExp_) : orderedCmp(orderByExp_), cmp() {}
        bool operator()(const pair<Row, Row> &lhs, const pair<Row, Row> &rhs) const {
            int rowRes = cmp(lhs.second, rhs.second);
            if (rowRes == 0) return false;
            int keyRes = orderedCmp(lhs.first, rhs.first);
            return keyRes != 0 ? keyRes < 0 : rowRes < 0;
        }

       private:
        RowCmp orderedCmp, cmp;
    };

    // orderbyKeys -> aggKeys
    using OrderbyMap = std::multimap<Row, Row, OrderSort>;
    // orderbyKeys -> aggKeys without aggregated values; hence not actually sorted
    using RawOrderbyMap = vector<pair<Row, Row>>;

    /**
     * @brief check agg in ORDER BY, if valid build mapping from orderExps to exps
     * @param order2AggExp agg exp indices orderByExp -> exps
     * TODO: (jlan) maybe make this a part of planner
     */
    static void OrderCheck(const vector<pair<Expression, isASC>> &orderbyExp, bool &orderHasAgg,
                           vector<int> &order2AggExp, const vector<pair<Expression, ColIndex>> &exps, const int nKey) {
        for (u32 i = 0; i < orderbyExp.size(); i++) {
            order2AggExp[i] = NONAGG;
            if (orderbyExp[i].first.HasAggregate()) {
                orderHasAgg = true;
                bool valid = false;
                for (u32 j = nKey; j < exps.size(); j++) {
                    if (ExprUtil::ExpCmp(exps[j].first, orderbyExp[i].first)) {
                        valid = true;
                        order2AggExp[i] = j;
                        break;
                    }
                }
                LOG_IF(ERROR, !valid) << "Aggregations in ORDER BY must appear in exps";
            }
        }
    }

    /**
     * @brief evaluate non-aggregate order-by expressions and add them to RawOrderbyMap
     * @param rawOrderbyMap a simple vector storing evaluated pairs
     */
    static void AddRawOrderbyMap(RawOrderbyMap &rawOrderbyMap, Row &key, vector<pair<Expression, isASC>> &orderbyExp,
                                 const vector<int> &order2AggExp, Row &row, Graph *g, int compressItr) {
        Row orderbyKey(orderbyExp.size());
        for (u32 k = 0; k < orderbyExp.size(); k++) {
            if (order2AggExp[k] == NONAGG) {
                if (compressItr != -1) {
                    orderbyKey[k] = orderbyExp[k].first.Eval(&row, g, &row[compressItr]);
                } else {
                    orderbyKey[k] = orderbyExp[k].first.Eval(&row, g);
                }
            }
        }
        rawOrderbyMap.emplace_back(std::move(orderbyKey), std::move(key));
    }

    /**
     * @brief fetch aggregated results from aggMap, assemble them with the non-agg parts, and feed them to the real
     * orderbyMap
     * @param orderbyMap store orderby key and complete row with agg data filled in
     * @param rawOrderbyMap store orderby key and row with no agg data
     * @param orderbyExp order by expression built before
     * @return map<Item*, Item*, OrderSort>
     */
    static void BuildOrderbyMap(OrderbyMap &orderbyMap, RawOrderbyMap &rawOrderbyMap, AggMap &aggMap, bool orderHasAgg,
                                const vector<int> &order2exp, u32 orderbySize,
                                vector<pair<Expression, ColIndex>> &exps) {
        for (auto &[orderbyKey, key] : rawOrderbyMap) {
            if (orderHasAgg) {
                for (u32 i = 0; i < orderbySize; i++) {
                    if (order2exp[i] == NONAGG) continue;
                    orderbyKey[i] = Aggregation::GetResult(exps[order2exp[i]].first, aggMap[key]);
                }
            }
            orderbyMap.emplace(orderbyKey, key);
        }
        rawOrderbyMap.clear();
    }
};

class LimitUtil {
   public:
    static const int WITHOUT_LIMIT = -1;
    explicit LimitUtil(int limitNum_ = WITHOUT_LIMIT) : limitNum(limitNum_), limitCounter(0) {}
    bool updateCnt(Row &row) {
        if (limitNum == WITHOUT_LIMIT) return true;
        if (limitCounter + (int)row.count() < limitNum) {
            limitCounter += (int)row.count();
            return true;
        } else {
            row.setCount(limitNum - limitCounter);
            limitCounter = limitNum;
            return false;
        }
    }
    bool updateCnt(int cnt) {
        if (limitNum == WITHOUT_LIMIT) return true;
        if (limitCounter + cnt < limitNum) {
            limitCounter += cnt;
            return true;
        } else {
            limitCounter = limitNum;
            return false;
        }
    }
    bool checkCnt() const { return limitNum == WITHOUT_LIMIT || limitCounter < limitNum; }
    void setLimitNum(int limitNum_) { limitNum = limitNum_; }

   private:
    int limitNum, limitCounter;
};
}  // namespace AGE
