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

#include "execution/physical_plan.h"
#include "gtest/gtest.h"
#include "plan/planner.h"
#include "server/standalone.h"
#include "src/execution/graph_tool.h"
#include "test/test_tool.h"

namespace AGE {

const char test_index_graphdir[] = AGE_TEST_DATA_DIR "/index_data";

// TODO(ycli): currently we do not support multiple connected component in pattern graph,
//      so we only ensure the correct result for at most 1 scan in a query
// Return first index scan op
IndexScanOp *GetIndexScanOp(PhysicalPlan *plan) {
    for (ExecutionStage *stage : plan->stages) {
        for (AbstractOp *op : stage->ops_)
            if (op->type == OpType_INDEX_SCAN) return dynamic_cast<IndexScanOp *>(op);
    }
    return nullptr;
}

void ExpectNotUseIndex(const string &query, Planner *planner) {
    auto [_, plan] = planner->process(query);
    EXPECT_NE(plan, nullptr);
    IndexScanOp *scanOp = GetIndexScanOp(plan);
    EXPECT_EQ(scanOp, nullptr);
    delete plan;
}

void TestIndexScanOpIntervals(const string &query, Planner *planner, const Intervals &answer) {
    auto [_, plan] = planner->process(query);
    // LOG(INFO) << plan->DebugString();
    EXPECT_NE(plan, nullptr);
    IndexScanOp *scanOp = GetIndexScanOp(plan);
    EXPECT_NE(scanOp, nullptr);
    EXPECT_EQ(scanOp->intervals, answer);
    delete plan;
}

void TestNoneExprTranslateToIndex(Standalone &server) {
    Planner *planner = &server.GetPlanner();
    Intervals answer;

    EXPECT_EQ(server.RunQuery("drop index on :a(string)").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("create index on :a(bool)").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("create index on :a(float)").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("drop index on :a(int)").IsSuccess(), true);

    // Indexable expr under EXP_OR with other expr
    ExpectNotUseIndex("match (n: a)--(b: a) where n.float < 5 or b.float > 10 return n", planner);

    // Un-indexable expr
    ExpectNotUseIndex("match (n: a)--(b: a) where n.float < b.float return n", planner);

    // Disabled property
    ExpectNotUseIndex("match (n: a)--(b: a) where n.int = 10 and n.string <> \"xx\" return n, count(b)", planner);

    // Un-indexed label
    ExpectNotUseIndex("match (n: c)-[]-(b)--(c) where n.float < 5 or n.float > 10 return n, count(b)", planner);

    // All-label
    ExpectNotUseIndex("match (n) where n.bool = true return n", planner);
}

void TestPartialExprTranslateToIndex(Standalone &server) {
    Planner *planner = &server.GetPlanner();
    Intervals answer;

    EXPECT_EQ(server.RunQuery("create index on :a(string)").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("drop index on :a(bool)").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("drop index on :a(float)").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("create index on :a(int)").IsSuccess(), true);

    // Discard other varibable's expr
    TestIndexScanOpIntervals(
        "match (c {bool: false})--(b)-[:d]-(n: a) where b.float < 5 and n.int > 20 return n, c, count(b)", planner,
        TestTool::ExprToIntervals(EXP_GT, Item(T_INTEGER, 20)));

    // Discard disabled index expr
    TestIndexScanOpIntervals(
        "match (c)--(b)-[:d]-(n: a {bool : true}) where n.string = \"Sandrokottos\" return b.int, count(n)", planner,
        TestTool::ExprToIntervals(EXP_GE, Item(T_STRING, "Sandrokottos")));

    // Discard irrelevant expr under EXP_OR
    TestIndexScanOpIntervals(
        "match (c)--(b)--(n: a) where (b.float = 71 or n.int > 20) and (n.int = 42)"
        " and (c.bool <> false or c.int = 5) with n, b return n, b;",
        planner, TestTool::ExprToIntervals(EXP_EQ, Item(T_INTEGER, 42)));

    // Complex
    answer = TestTool::ExprToIntervals(EXP_GE, Item(T_STRING, "kk"));
    answer |= TestTool::ExprToIntervals(EXP_LT, Item(T_STRING, "hh"));
    answer &= TestTool::ExprToIntervals(EXP_GT, Item(T_STRING, "aa"));
    TestIndexScanOpIntervals(
        "match (c)--(b)--(n: a) where (b.float = 71 or n.int > 20) and (n.int = 42 or n.string = \"Hadrian\")"
        " and (c.bool <> false or c.int = 5) and n.string > \"aa\" and (n.string < \"hh\" or n.string >= \"kk\") "
        " with n, count(c) as cc return n, cc;",
        planner, answer);
}

void TestAllExprTranslateToIndex(Standalone &server) {
    Planner *planner = &server.GetPlanner();
    Intervals answer;

    EXPECT_EQ(server.RunQuery("create index on :a(string)").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("create index on :a(bool)").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("create index on :a(float)").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("create index on :a(int)").IsSuccess(), true);

    // Empty
    TestIndexScanOpIntervals("match (n: a {string: \"a\"})--(b)--(c) where n.string = \"b\" return count(c)", planner,
                             Intervals());

    // Union
    TestIndexScanOpIntervals("match (n: a)--(b) where n.int > 5 or n.int > 10  return count(n)", planner,
                             TestTool::ExprToIntervals(EXP_GT, Item(T_INTEGER, 5)));

    answer = TestTool::ExprToIntervals(EXP_NEQ, Item(T_FLOAT, 10));
    answer |= TestTool::ExprToIntervals(EXP_NEQ, Item(T_FLOAT, 50));
    // TODO(ycli): support float / int convert (if "n.float <> 10" this query will fail)
    TestIndexScanOpIntervals("match (n: a)--(b) where n.float <> 10.0 or n.float <> 50.0  return count(n)", planner,
                             answer);

    // And
    answer = TestTool::ExprToIntervals(EXP_GT, Item(T_STRING, "aa"));
    answer &= TestTool::ExprToIntervals(EXP_LE, Item(T_STRING, "Rurik"));
    TestIndexScanOpIntervals("match (n: a)--(b) where n.string > \"aa\" and n.string <= \"Rurik\"  return count(n)",
                             planner, answer);

    answer = TestTool::ExprToIntervals(EXP_EQ, Item(T_BOOL, true));
    TestIndexScanOpIntervals("match (n: a {bool : true})-[:b]-(b) where n.bool = true return b", planner, answer);

    // Complex
    answer = TestTool::ExprToIntervals(EXP_LT, Item(T_INTEGER, 0));
    answer |= TestTool::ExprToIntervals(EXP_GT, Item(T_INTEGER, 3));
    answer &= TestTool::ExprToIntervals(EXP_LE, Item(T_INTEGER, 10));
    answer |= TestTool::ExprToIntervals(EXP_EQ, Item(T_INTEGER, 19));
    answer |= TestTool::ExprToIntervals(EXP_GE, Item(T_INTEGER, 100));
    TestIndexScanOpIntervals(
        "match (b)-[:c]-(n: a)--(c:c) where (n.int > 3 and n.int <= 10)"
        "or (n.int = 19 or (n.int < 0 or n.int >= 100)) return n.bool, count(c)",
        planner, answer);
}

TEST(test_index, synthesized_test) {
    TestTool::prepareGraph(test_index_graphdir, 100, 200, 10);
    Standalone server(Config::GetInstance()->graph_name_, test_index_graphdir, 4, false);
    // Standalone server("/data/share/users/ycli/AGE/data/output", 8);

    TestAllExprTranslateToIndex(server);
    TestPartialExprTranslateToIndex(server);
    TestNoneExprTranslateToIndex(server);
}

}  // namespace AGE
