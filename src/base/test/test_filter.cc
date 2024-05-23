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

#include <base/expression.h>
#include <glog/logging.h>
#include <operator/filter_op.h>
#include <algorithm>
#include "base/type.h"
#include "gtest/gtest.h"
#include "storage/graph.h"
#include "storage/graph_loader.h"
#include "test/test_tool.h"

using std::vector;

namespace AGE {
const char test_filter_graphdir[] = AGE_TEST_DATA_DIR "/filter_data";

Expression propFilter(ColIndex colIdx, PropId pkey, ExpType cmp, Item item) {
    Expression op(cmp);
    Expression &prop = op.EmplaceChild(EXP_PROP, pkey);
    prop.EmplaceChild(EXP_VARIABLE, colIdx);
    op.EmplaceChild(EXP_CONSTANT, item);
    return op;
}

void testHisVPFilter(Graph *g) {
    // p.bool = false.
    PropId boo = g->getPropId("bool");
    ColIndex colIdx = 0;
    u32 compressBeginIdx = 2;
    Expression filter = propFilter(colIdx, boo, EXP_EQ, Item(T_BOOL, false));

    Row r(compressBeginIdx);
    for (const Item &v : g->getAllVtx()) {
        r[colIdx] = v;
        EXPECT_EQ(g->getProp(v, boo) == Item(T_BOOL, false), filter.Eval(&r, g));
    }
}

void testCompressVPFilter(Graph *g) {
    // p.bool = true.
    PropId boo = g->getPropId("bool");
    ColIndex colIdx = COLINDEX_COMPRESS;
    u32 compressBeginIdx = 2;
    Expression filter = propFilter(colIdx, boo, EXP_EQ, Item(T_BOOL, true));

    Row r(compressBeginIdx);
    for (const Item &v : g->getAllVtx()) {
        r.emplace_back(v);
    }
    for (u32 i = compressBeginIdx; i < r.size(); i++) {
        EXPECT_EQ(g->getProp(r[i], boo) == Item(T_BOOL, true), filter.Eval(&r, g, &r[i]));
    }
}

void testOperatorHis(Graph *g, const string &s) {
    // n.string < s.
    PropId str = g->getPropId("string");
    ColIndex colIdx = 0;
    u32 compressBeginIdx = 2;
    Expression filter = propFilter(colIdx, str, EXP_LT, Item(T_STRING, s));

    Message m = Message::BuildEmptyMessage(0);
    m.plan->g = g;
    vector<Item> answer;
    for (Item &v : g->getAllVtx()) {
        m.data.emplace_back(compressBeginIdx);
        m.data.back()[colIdx] = v;
        if (g->getProp(v, str) < Item(T_STRING, s)) answer.emplace_back(v);
    }

    m.plan->stages.emplace_back(new ExecutionStage());
    m.plan->stages[0]->appendOp(new FilterOp(filter, compressBeginIdx));

    vector<Message> output;
    vector<Item> result;
    m.plan->stages.back()->processOp(m, output);

    for (Message &m : output) {
        for (Row &r : m.data) {
            result.emplace_back(std::move(r[colIdx]));
        }
    }

    // Check answer.
    EXPECT_EQ(answer.size(), result.size());
    sort(answer.begin(), answer.end());
    sort(result.begin(), result.end());
    for (size_t i = 0; i < answer.size(); i++) {
        EXPECT_EQ(answer[i], result[i]);
    }
}

void testOperatorCompress(Graph *g, const string &s) {
    // n.string >= s.
    PropId str = g->getPropId("string");
    ColIndex colIdx = COLINDEX_COMPRESS;
    u32 compressBeginIdx = 2;
    Expression filter = propFilter(colIdx, str, EXP_GE, Item(T_STRING, s));
    // printf("filter: %s\n", filter->DebugString().c_str());

    Message m = Message::BuildEmptyMessage(0);
    m.plan->g = g;
    m.data.emplace_back(compressBeginIdx);
    vector<Item> answer;
    for (Item &v : g->getAllVtx()) {
        m.data.back().emplace_back(v);
        if (g->getProp(v, str) >= Item(T_STRING, s)) answer.emplace_back(v);
    }

    m.plan->stages.emplace_back(new ExecutionStage());
    m.plan->stages[0]->appendOp(new FilterOp(filter, compressBeginIdx));

    vector<Message> output;
    vector<Item> result;
    m.plan->stages[0]->processOp(m, output);

    EXPECT_EQ(output.size(), 1ull);
    // EXPECT_EQ(output[0].data.size(), 1ull);
    if (output[0].data.size() > 0) {
        Row &res = output[0].data[0];
        for (u32 i = compressBeginIdx; i < res.size(); i++) {
            result.emplace_back(std::move(res[i]));
        }
    }

    // Check answer.
    EXPECT_EQ(answer.size(), result.size());
    sort(answer.begin(), answer.end());
    sort(result.begin(), result.end());

    for (size_t i = 0; i < answer.size(); i++) {
        // printf("%lu: %s, %s\n", i, answer[i].DebugString().c_str(), result[i].DebugString().c_str());
        EXPECT_EQ(answer[i], result[i]);
    }
}

TEST(test_filter, all_test) {
    TestTool::prepareGraph(test_filter_graphdir, 50, 50, 10);
    Graph g(TestTool::getTestGraphDir(test_filter_graphdir));
    CHECK(g.load()) << "Graph Load Fails";
    testHisVPFilter(&g);
    testCompressVPFilter(&g);
    testOperatorHis(&g, "12");
    testOperatorHis(&g, "102");
    testOperatorCompress(&g, "977");
    testOperatorCompress(&g, "15");
}
}  // namespace AGE
