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

#include <iostream>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>
#include "./test_tool.h"
#include "base/aggregation.h"
#include "base/expression.h"
#include "base/item.h"
#include "base/proj_util.h"
#include "base/row.h"
#include "base/type.h"
#include "execution/message.h"
#include "execution/physical_plan.h"
#include "gtest/gtest.h"
#include "operator/barrier_projection_op.h"
#include "operator/distinct/global_distinctor.h"
#include "storage/graph.h"
#include "storage/graph_loader.h"

using std::pair;
using std::string;
using std::vector;

namespace AGE {
namespace testProjection {
static const char projection_graphdir[] = AGE_TEST_DATA_DIR "/projection_data";
static const std::set<string> testProps = {"bool", "int", "string", "float", "randInt", "randStr", "null"};
static const vector<bool> distinctList = {false, true};
static const vector<int> orderList = {-1, OrderUtil::ASC, OrderUtil::DSC}, limitList = {LimitUtil::WITHOUT_LIMIT, 10};

void PrintConfig(const vector<string>& fields, const vector<pair<string, isASC>>& orderByExps, const bool& isDistinct,
                 const bool& isOrder, const int& limitNum) {
    std::cout << "Test configuration:" << std::endl;
    std::cout << "  isDistinct: " << isDistinct << std::endl;
    std::cout << "  isOrder:    " << isOrder << std::endl;
    std::cout << "  limitNum:   " << limitNum << std::endl;
    std::cout << "  Prop fields:"
              << "  { ";
    for (u32 i = 0; i < fields.size(); i++) std::cout << fields[i] << ", ";
    std::cout << "}" << std::endl;
    std::cout << "  order Exps: "
              << "  { ";
    for (u32 i = 0; i < orderByExps.size(); i++)
        std::cout << orderByExps[i].first << ": " << (orderByExps[i].second ? "ASC" : "DSC") << ", ";
    std::cout << "}" << std::endl;
}

// Remove duplicated rows from a vector
void MakeDistinct(vector<pair<Row, Row>>& rows) {
    vector<pair<Row, Row>> retRows;
    std::unordered_set<Row, Row::HashFunc, Row::EqualFunc> uniqueSet;
    for (u32 i = 0; i < rows.size(); i++) {
        if (!uniqueSet.count(rows[i].first)) {
            uniqueSet.insert(rows[i].first);
            retRows.emplace_back(rows[i]);
        }
    }
    rows.swap(retRows);
}

// Order rows by their keys (i.e., row.second)
void MakeOrdered(vector<pair<Row, Row>>& rows, const vector<pair<PropId, isASC>>& orderByProp) {
    ASSERT_EQ(orderByProp.size(), rows[0].second.size());

    // TODO(jlan): remove unused exps.first
    vector<pair<Expression, isASC>> exps;
    for (auto& [pid, asc] : orderByProp) exps.emplace_back(Expression(), asc);

    OrderUtil::OrderbyMap orderMap = OrderUtil::OrderbyMap(OrderUtil::OrderSort(exps));
    for (u32 i = 0; i < rows.size(); i++) orderMap.emplace(rows[i].second, rows[i].first);

    vector<pair<Row, Row>> ret;
    for (auto& [key, row] : orderMap) ret.emplace_back(std::move(row), std::move(key));
    ret.swap(rows);
}

// translate field strings into field IDs
void PrepareId(const Graph& g, const vector<string>& fields, vector<PropId>& fieldsId) {
    for (auto field : fields) {
        ASSERT_TRUE(testProps.count(field));
        fieldsId.emplace_back(field == "null" ? INVALID_PROP : g.getPropId(field));
    }
}

// translate orderKey strings into IDs; map orderKeys to their field positions
void PrepareOrderId(const Graph* g, const vector<pair<string, isASC>>& orderKeys, const vector<PropId>& fieldsId,
                    vector<pair<PropId, isASC>>& orderKeysId, vector<ColIndex>& orderPos) {
    for (auto& [key, asc] : orderKeys) {
        ASSERT_TRUE(testProps.count(key));
        orderKeysId.emplace_back(key == "null" ? INVALID_PROP : g->getPropId(key), asc);
        for (u32 i = 0; i < fieldsId.size(); i++) {
            if (orderKeysId.back().first == fieldsId[i]) {
                orderPos.emplace_back(i);
                break;
            }
        }
    }
    ASSERT_EQ(orderPos.size(), orderKeys.size());
}

Message PrepareMessage(const Graph* g, const vector<PropId>& fieldsId, const vector<pair<PropId, isASC>>& orderByProp,
                       bool isDistinct, bool isOrder, int limitNum = LimitUtil::WITHOUT_LIMIT) {
    vector<Item> vertices = g->getAllVtx();
    int colNum = 0, newColNum = fieldsId.size();

    vector<pair<Expression, ColIndex>> exps;
    for (u32 i = 0; i < fieldsId.size(); i++) {
        Expression exp = Expression(EXP_PROP, fieldsId[i]);
        exp.EmplaceChild(EXP_VARIABLE, COLINDEX_COMPRESS);
        exps.emplace_back(std::move(exp), i);
    }

    vector<pair<Expression, isASC>> orderByExp;
    if (isOrder) {
        for (auto& [key, asc] : orderByProp) {
            Expression exp(EXP_PROP, key);
            exp.EmplaceChild(EXP_VARIABLE, COLINDEX_COMPRESS);
            orderByExp.emplace_back(std::move(exp), asc);
        }
    }

    Message m = Message::BuildEmptyMessage(0);
    m.plan->g = const_cast<Graph*>(g);
    m.plan->stages.emplace_back(new ExecutionStage());
    m.plan->stages.back()->appendOp(
        new BarrierProjectOp(OpType_BARRIER_PROJECT, exps, colNum, newColNum, &orderByExp, isDistinct, limitNum));
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

vector<Row> PrepareAnswer(const Graph* g, const vector<PropId>& fieldsId,
                          const vector<pair<PropId, isASC>>& orderByProp, const bool& isDistinct, const bool& isOrder) {
    vector<Item> vertices = g->getAllVtx();
    vector<pair<Row, Row>> rows;  // row, orderKeys
    for (Item& v : vertices) {
        Row row;
        for (const PropId& id : fieldsId) {
            row.emplace_back(id == INVALID_PROP ? Item(T_NULL) : g->getProp(v, id));
        }
        rows.emplace_back(std::move(row), Row());

        if (isOrder) {
            Row key(orderByProp.size());
            for (u32 i = 0; i < orderByProp.size(); i++) {
                PropId orderKey = orderByProp[i].first;
                key[i] = orderByProp[i].first == INVALID_PROP ? Item(T_NULL) : g->getProp(v, orderKey);
            }
            rows.back().second = key;
        }
    }

    if (isDistinct) MakeDistinct(rows);
    if (isOrder) MakeOrdered(rows, orderByProp);

    vector<Row> result;
    for (auto& [row, key] : rows) result.emplace_back(std::move(row));
    return result;
}

void TestBarrierProjection(const vector<string>& fields, const vector<pair<string, isASC>>& orderByExps,
                           const bool& isDistinct, const bool& isOrder, const int& limitNum) {
    if (!(isDistinct || isOrder || limitNum != LimitUtil::WITHOUT_LIMIT)) return;
    // PrintConfig(fields, orderByExps, isDistinct, isOrder, limitNum);

    Graph g(TestTool::getTestGraphDir(projection_graphdir));
    ASSERT_TRUE(g.load());

    vector<PropId> fieldsId;
    vector<pair<PropId, isASC>> orderByProp;
    vector<ColIndex> orderPos;

    PrepareId(g, fields, fieldsId);
    if (isOrder) PrepareOrderId(&g, orderByExps, fieldsId, orderByProp, orderPos);
    Message m = PrepareMessage(&g, fieldsId, orderByProp, isDistinct, isOrder, limitNum);
    vector<Row> answer = PrepareAnswer(&g, fieldsId, orderByProp, isDistinct, isOrder);
    vector<Row> result = PrepareResult(m);

    // Global distinctor doesn't preserve the input order. We'll do LIMIT here
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
            for (ColIndex& orderIdx : orderPos) EXPECT_EQ(Item::Compare(result[i][orderIdx], answer[i][orderIdx]), 0);
        }
    }
}

TEST(test_barrier_projection, single_prop) {
    TestTool::prepareGraph(projection_graphdir, 200, 100, 10);

    vector<string> candidateFields = {"float", "randInt", "randStr"};

    vector<string> fields(1);
    vector<pair<string, isASC>> orderByExp(1);
    for (string field : candidateFields)
        for (bool distinct : distinctList)
            for (int limit : limitList)
                for (int order : orderList) {
                    bool isOrder = order >= 0 ? true : false;
                    fields[0] = field;
                    orderByExp[0] = std::make_pair(field, order >= 0 ? order : false);
                    TestBarrierProjection(fields, orderByExp, distinct, isOrder, limit);
                }
}

TEST(test_barrier_projection, multiple_prop) {
    TestTool::prepareGraph(projection_graphdir, 200, 100, 10);

    vector<string> candidateFields = {"float", "randInt", "randStr"};

    vector<string> fields(2);
    vector<pair<string, isASC>> orderByExp(1);
    for (string field1 : candidateFields)
        for (string field2 : candidateFields) {
            if (field1 >= field2) continue;
            for (bool distinct : distinctList)
                for (int limit : limitList)
                    for (int order : orderList) {
                        bool isOrder = order >= 0 ? true : false;
                        fields[0] = field1, fields[1] = field2;
                        orderByExp[0] = std::make_pair(field1, order >= 0 ? order : false);
                        TestBarrierProjection(fields, orderByExp, distinct, isOrder, limit);
                    }
        }
}

TEST(test_barrier_projection, null_prop) {
    TestTool::prepareGraph(projection_graphdir, 200, 100, 10);

    vector<string> candidateFields = {"null"};

    vector<string> fields(1);
    vector<pair<string, isASC>> orderByExp(1);
    for (string field : candidateFields)
        for (bool distinct : distinctList)
            for (int limit : limitList)
                for (int order : orderList) {
                    bool isOrder = order >= 0 ? true : false;
                    fields[0] = field;
                    orderByExp[0] = std::make_pair(field, order >= 0 ? order : false);
                    TestBarrierProjection(fields, orderByExp, distinct, isOrder, limit);
                }
}

TEST(test_barrier_projection, hybrid_prop) {
    TestTool::prepareGraph(projection_graphdir, 200, 100, 10);

    vector<string> candidateFields = {"null", "randInt", "randStr"};

    vector<string> fields(2);
    vector<pair<string, isASC>> orderByExp(1);
    for (string field1 : candidateFields)
        for (string field2 : candidateFields) {
            if (field1 >= field2) continue;
            for (bool distinct : distinctList)
                for (int limit : limitList)
                    for (int order : orderList) {
                        bool isOrder = order >= 0 ? true : false;
                        fields[0] = field1, fields[1] = field2;
                        orderByExp[0] = std::make_pair(field1, order >= 0 ? order : false);
                        TestBarrierProjection(fields, orderByExp, distinct, isOrder, limit);
                    }
        }
}

}  // namespace testProjection
}  // namespace AGE
