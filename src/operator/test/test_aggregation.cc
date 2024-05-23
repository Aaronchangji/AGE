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

#include <algorithm>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>
#include "./test_tool.h"
#include "base/aggregation.h"
#include "base/expression.h"
#include "base/graph_entity.h"
#include "base/item.h"
#include "base/proj_util.h"
#include "base/row.h"
#include "base/type.h"
#include "execution/message.h"
#include "execution/physical_plan.h"
#include "gtest/gtest.h"
#include "operator/aggregate_op.h"
#include "operator/barrier_projection_op.h"
#include "operator/distinct/global_distinctor.h"
#include "storage/graph.h"
#include "storage/graph_loader.h"

using std::map;
using std::pair;
using std::set;
using std::string;
using std::vector;

namespace AGE {
namespace testAggregation {
static const char aggregation_graphdir[] = AGE_TEST_DATA_DIR "/aggregation_data";
static const set<string> testProps = {"bool", "int", "string", "float", "randInt", "randStr", "null"};
static const vector<string> aggTypes = {"AGG_SUM", "AGG_AVG", "AGG_CNT", "AGG_MAX", "AGG_MIN"};
static const vector<bool> distinctList = {false, true};
static const vector<int> orderList = {-1, OrderUtil::ASC, OrderUtil::DSC}, limitList = {LimitUtil::WITHOUT_LIMIT, 10};

void PrintConfig(const vector<string>& fields, const vector<AggType>& aggFuncs,
                 const vector<pair<ColIndex, isASC>>& orderbyIdx, const bool& isDistinct, const bool& isOrder,
                 const int& limitNum) {
    u32 nKey = fields.size() - aggFuncs.size();
    std::cout << "Test configuration:" << std::endl;
    std::cout << "  isDistinct: " << isDistinct << std::endl;
    std::cout << "  isOrder:    " << isOrder << std::endl;
    std::cout << "  limitNum:   " << limitNum << std::endl;
    std::cout << "  Fields:"
              << "  { ";
    for (u32 i = 0; i < fields.size(); i++) {
        string output = i < nKey ? fields[i] : aggTypes[aggFuncs[i - nKey]] + "(" + fields[i] + ")";
        std::cout << output << ", ";
    }
    std::cout << "}" << std::endl;
    std::cout << "  order Exps: "
              << "  { ";
    for (u32 i = 0; i < orderbyIdx.size(); i++) {
        u32 idx = orderbyIdx[i].first;
        string output = idx < nKey ? fields[idx] : aggTypes[aggFuncs[idx - nKey]] + "(" + fields[idx] + ")";
        std::cout << output << ": " << (orderbyIdx[i].second ? "ASC" : "DSC") << ", ";
    }
    std::cout << "}" << std::endl;
}

void MakeOrdered(vector<Row>& rows, const vector<pair<ColIndex, isASC>>& orderbyIdx) {
    vector<pair<Expression, isASC>> orderExps;
    vector<Row> keys(rows.size());
    for (auto& [idx, asc] : orderbyIdx) {
        orderExps.emplace_back(Expression(), asc);
        for (u32 i = 0; i < rows.size(); i++) keys[i].emplace_back(rows[i][idx]);
    }

    OrderUtil::OrderbyMap orderMap = OrderUtil::OrderbyMap(OrderUtil::OrderSort(orderExps));
    for (u32 i = 0; i < rows.size(); i++) orderMap.emplace(keys[i], rows[i]);

    vector<Row> ret;
    for (auto& [key, row] : orderMap) ret.emplace_back(std::move(row));
    ret.swap(rows);
}

Expression GetPropExp(const PropId& pid) {
    Expression exp(EXP_PROP, pid);
    exp.EmplaceChild(EXP_VARIABLE, COLINDEX_COMPRESS);
    return exp;
}

// generate agg functions. Configure DISTINCT here
Expression GetAggExp(const PropId& pid, const AggType& aggType, const bool& isDistinct) {
    Expression exp(EXP_AGGFUNC, aggType, isDistinct);
    Expression operand(EXP_PROP, pid);
    operand.EmplaceChild(EXP_VARIABLE, COLINDEX_COMPRESS);
    exp.EmplaceChild(operand);
    return exp;
}

// translate field strings into field IDs
void PrepareId(const Graph* g, const vector<string>& fields, vector<PropId>& fieldsId) {
    for (auto field : fields) {
        ASSERT_TRUE(testProps.count(field));
        fieldsId.emplace_back(field == "null" ? INVALID_PROP : g->getPropId(field));
    }
}

// translate fields to 1) property expressions or 2) aggregation expressions
void PrepareExps(vector<pair<Expression, ColIndex>>& exps, vector<Expression>& propExps, const vector<PropId>& fieldsId,
                 const vector<AggType>& aggFuncs, const u32& nKey, const bool& isDistinct) {
    for (u32 i = 0; i < fieldsId.size(); i++) {
        propExps.emplace_back(GetPropExp(fieldsId[i]));
        if (i < nKey)
            exps.emplace_back(GetPropExp(fieldsId[i]), i);
        else
            exps.emplace_back(GetAggExp(fieldsId[i], aggFuncs[i - nKey], isDistinct), i);
    }
}

Message PrepareMessage(const Graph* g, const vector<pair<Expression, ColIndex>>& exps, const u32& nKey,
                       const vector<pair<ColIndex, isASC>>& orderbyIdx, const bool& isOrder, const int& limitNum) {
    vector<Item> vertices = g->getAllVtx();
    int colNum = 0, newColNum = exps.size();

    vector<pair<Expression, isASC>> orderbyExp;
    if (isOrder) {
        for (const auto& [colIdx, asc] : orderbyIdx) orderbyExp.emplace_back(exps[colIdx].first, asc);
    }

    Message m = Message::BuildEmptyMessage(0);
    m.plan->g = const_cast<Graph*>(g);
    m.plan->stages.emplace_back(new ExecutionStage());
    if (isOrder) {
        m.plan->stages.back()->appendOp(
            new AggregateOp(OpType_AGGREGATE, exps, colNum, nKey, newColNum, &orderbyExp, limitNum));
    } else {
        m.plan->stages.back()->appendOp(
            new AggregateOp(OpType_AGGREGATE, exps, colNum, nKey, newColNum, nullptr, limitNum));
    }
    m.data.emplace_back();
    for (auto vertex : vertices) m.data.back().emplace_back(vertex);
    m.data.back().setCount(1);

    return m;
}

vector<Row> PrepareResult(Message& m) {
    vector<Message> output;
    vector<Row> result;
    m.plan->stages.back()->processOp(m, output);
    for (Message& msg : output)
        for (Row& r : msg.data) result.emplace_back(std::move(r));

    return result;
}

// We'll mimic the aggregation function here
vector<Row> PrepareAnswer(const Graph* g, vector<Expression>& propExps, const vector<AggType>& aggFuncs,
                          const u32& nKey, const vector<pair<ColIndex, isASC>>& orderbyIdx, const bool& isDistinct,
                          const bool& isOrder) {
    // DISTINCT set, content, count (used in AVG)
    using AggCtx = std::tuple<set<Item>, Item, Item>;
    using AggMap = std::map<Row, vector<AggCtx>, OrderUtil::OrderSort>;
    AggMap amap;
    vector<Item> vertices = g->getAllVtx();
    vector<Row> propRows, answer;

    for (Item& v : vertices) {
        // generate key
        propRows.emplace_back(nKey);
        Row& row = propRows.back();
        Row* useless = &row;
        for (u32 i = 0; i < nKey; i++) row[i] = propExps[i].Eval(useless, g, &v);
        if (!amap.count(row)) amap[row] = vector<AggCtx>(aggFuncs.size());

        for (u32 i = 0; i < aggFuncs.size(); i++) {
            Item added = propExps[nKey + i].Eval(useless, g, &v);

            // DISTINCT here
            if (isDistinct) {
                set<Item>& distinctSet = std::get<0>(amap[row][i]);
                if (distinctSet.count(added)) continue;
                distinctSet.insert(added);
            }

            Item &content = std::get<1>(amap[row][i]), &count = std::get<2>(amap[row][i]);
            switch (aggFuncs[i]) {
            case AGG_SUM:
                content = (content.type == T_UNKNOWN) ? added : content + added;
                break;
            case AGG_CNT:
                content = (content.type == T_UNKNOWN) ? Item(T_INTEGER, 1) : content + Item(T_INTEGER, 1);
                break;
            case AGG_MAX:
                content = (content.type == T_UNKNOWN || content < added) ? added : content;
                break;
            case AGG_MIN:
                content = (content.type == T_UNKNOWN || content > added) ? added : content;
                break;
            case AGG_AVG:
                count = (count.type == T_UNKNOWN) ? Item(T_INTEGER, 1) : count + Item(T_INTEGER, 1);
                content = (content.type == T_UNKNOWN) ? added : (content * count + added) / count;
            }
        }
    }

    for (auto& [key, ctx] : amap) {
        answer.emplace_back(propExps.size());
        Row& row = answer.back();
        for (u32 i = 0; i < row.size(); i++) row[i] = i < nKey ? key[i] : std::get<1>(ctx[i - nKey]);
    }

    if (isOrder) MakeOrdered(answer, orderbyIdx);
    return answer;
}

void TestAggregation(const vector<string>& fields, const vector<AggType>& aggFuncs, const u32& nKey,
                     const vector<pair<ColIndex, isASC>>& orderbyIdx, const bool& isDistinct, const bool& isOrder,
                     const int& limitNum) {
    ASSERT_EQ(fields.size() - nKey, aggFuncs.size());
    // PrintConfig(fields, aggFuncs, orderbyIdx, isDistinct, isOrder, limitNum);

    Graph g(TestTool::getTestGraphDir(aggregation_graphdir));
    ASSERT_TRUE(g.load());

    vector<PropId> fieldsId;
    vector<Expression> propExps;              // property exps used by answer generator
    vector<pair<Expression, ColIndex>> exps;  // exps with agg, used by tested op

    PrepareId(&g, fields, fieldsId);
    PrepareExps(exps, propExps, fieldsId, aggFuncs, nKey, isDistinct);
    Message m = PrepareMessage(&g, exps, nKey, orderbyIdx, isOrder, limitNum);
    vector<Row> answer = PrepareAnswer(&g, propExps, aggFuncs, nKey, orderbyIdx, isDistinct, isOrder);
    vector<Row> result = PrepareResult(m);

    EXPECT_EQ(result.size(),
              limitNum != LimitUtil::WITHOUT_LIMIT ? std::min(limitNum, (int)answer.size()) : answer.size());
    if (!isOrder) {
        // for (u32 i = 0; i < result.size(); i++)
        //     std::cout << result[i].DebugString() << " -- " << answer[i].DebugString() << std::endl;

        std::unordered_set<Row, Row::HashFunc, Row::EqualFunc> answerSet;
        for (Row& row : answer) answerSet.emplace(std::move(row));
        for (Row& row : result) EXPECT_TRUE(answerSet.count(row));
    } else {
        for (u32 i = 0; i < result.size(); i++) {
            // std::cout << result[i].DebugString() << " -- " << answer[i].DebugString() << std::endl;
            // Only check those ORDER BY'ed positions. use Compare to handle NULLs
            for (const auto& [orderIdx, _] : orderbyIdx)
                EXPECT_EQ(Item::Compare(result[i][orderIdx], answer[i][orderIdx]), 0);
        }
    }
}

// Aggregation with no key (i.e., only one group)
TEST(test_aggregation, null_key) {
    TestTool::prepareGraph(aggregation_graphdir, 200, 100, 1);

    vector<string> candidateFields = {"float", "randInt", "randStr"}, fields(1);
    vector<AggType> candidateAggs = {AGG_CNT, AGG_MIN}, aggFuncs(1);

    vector<pair<ColIndex, isASC>> orderbyIdx;
    for (string field : candidateFields)
        for (AggType agg : candidateAggs)
            for (bool distinct : distinctList)
                for (int limit : limitList) {
                    fields[0] = field;
                    aggFuncs[0] = agg;
                    TestAggregation(fields, aggFuncs, 0, orderbyIdx, distinct, false, limit);
                }
}

TEST(test_aggregation, one_agg) {
    TestTool::prepareGraph(aggregation_graphdir, 200, 100, 1);

    vector<string> candidateFields = {"bool", "randStr", "randInt"}, fields(2);
    vector<AggType> candidateAggs = {AGG_CNT, AGG_MIN, AGG_SUM}, aggFuncs(1);

    // ORDER BY aggregated values
    vector<pair<ColIndex, isASC>> orderbyIdx(1);
    for (string field1 : candidateFields)
        for (string field2 : candidateFields) {
            if (field1 >= field2) continue;
            for (AggType agg : candidateAggs)
                for (bool distinct : distinctList)
                    for (int limit : limitList)
                        for (int order : orderList) {
                            bool isOrder = order >= 0 ? true : false;
                            fields[0] = field1, fields[1] = field2;
                            aggFuncs[0] = agg;
                            orderbyIdx[0] = std::make_pair(1, order);
                            TestAggregation(fields, aggFuncs, 1, orderbyIdx, distinct, isOrder, limit);
                        }
        }
}

TEST(test_aggregation, two_agg) {
    TestTool::prepareGraph(aggregation_graphdir, 200, 100, 1);

    vector<string> keyFields = {"bool", "randStr", "randInt"}, aggFields = {"float", "int"}, fields(3);
    vector<AggType> candidateAggs = {AGG_CNT, AGG_MIN, AGG_SUM}, aggFuncs(2);

    // ORDER BY aggregated values
    vector<pair<ColIndex, isASC>> orderbyIdx(2);
    for (string field : keyFields)
        for (AggType agg1 : candidateAggs)
            for (AggType agg2 : candidateAggs)
                for (bool distinct : distinctList)
                    for (int limit : limitList)
                        for (int order : orderList) {
                            bool isOrder = order >= 0 ? true : false;
                            fields[0] = field, fields[1] = aggFields[0], fields[2] = aggFields[1];
                            aggFuncs[0] = agg1, aggFuncs[1] = agg2;
                            orderbyIdx[0] = std::make_pair(1, order), orderbyIdx[1] = std::make_pair(2, !order);
                            TestAggregation(fields, aggFuncs, 1, orderbyIdx, distinct, isOrder, limit);
                        }
}

}  // namespace testAggregation
}  // namespace AGE
