// Copyright 2022 HDL
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
// limitations under the License

#include <gtest/gtest.h>
#include <fstream>
#include <string>
#include <utility>
#include <vector>
#include "base/node.h"
#include "server/standalone.h"
#include "storage/id_mapper.h"
#include "storage/unique_namer.h"
#include "test/test_tool.h"

using std::make_pair;
using std::pair;
using std::string;
using std::vector;

namespace AGE {
const char query_graphdir[] = AGE_TEST_DATA_DIR "/test_query_data";

void loadExample(const string& graphdir) {
    TestTool::ExampleGraph g(query_graphdir);
    g.generate();
    GraphLoader loader(Nodes::CreateStandaloneNodes(), graphdir);
    loader.load({{g.vSchemaFile, g.vf}}, {{g.eSchemaFile, g.ef}});
}

void test_query() {
    // LOG(INFO) << Nodes::GetInstance()->DebugString();

    loadExample(query_graphdir);
    AGE::Standalone standalone(Config::GetInstance()->graph_name_, query_graphdir, 4, false);
    Graph* g = &standalone.getGraph();
    LabelId person = g->getLabelId("person");
    // LabelId comment = g->getLabelId("comment");
    PropId age = g->getPropId("age");

    // Github issue #122
    // Get result.
    Result res = standalone.RunQuery("match (n:person)-[:knows]->(m:person) return n, m, n.age, m.age");

    sort(res.data.begin(), res.data.end(), [](const Row& l, const Row& r) {
        if (l[0].vertex.id != r[0].vertex.id) return l[0].vertex.id < r[0].vertex.id;
        return l[1].vertex.id < r[1].vertex.id;
    });
    // LOG(INFO) << res.DebugString();

    // Check answer.
    EXPECT_EQ(res.data.size(), 4ull);
    int answer[4][2] = {{1, 2}, {1, 3}, {2, 1}, {3, 2}};
    for (size_t i = 0; i < res.data.size(); i++) {
        Item l = Item(T_VERTEX, Vertex(answer[i][0], person)), r = Item(T_VERTEX, Vertex(answer[i][1], person));
        // LOG(INFO) << res.data[i][0].DebugString() << ", " << l.DebugString();
        EXPECT_EQ(res.data[i][0], l);
        // LOG(INFO) << res.data[i][1].DebugString() << ", " << l.DebugString();
        EXPECT_EQ(res.data[i][1], r);
        // LOG(INFO) << res.data[i][2].DebugString() << ", " << g->getProp(l, age).DebugString();
        EXPECT_EQ(res.data[i][2], g->getProp(l, age));
        // LOG(INFO) << res.data[i][3].DebugString() << ", " << g->getProp(r, age).DebugString();
        EXPECT_EQ(res.data[i][3], g->getProp(r, age));
    }

    // Github issue #123
    // Get result.
    res = standalone.RunQuery("match (n:person)-[:knows]->(m:person) with m,n return m.gender, count(m)");
    sort(res.data.begin(), res.data.end(), [](const Row& l, const Row& r) { return l[0].boolVal < r[0].boolVal; });

    // Check answer.
    EXPECT_EQ(res.data.size(), 2ull);
    EXPECT_EQ(res.data[0][0], Item(T_BOOL, false));
    EXPECT_EQ(res.data[0][1], Item(T_INTEGER, 1));
    EXPECT_EQ(res.data[1][0], Item(T_BOOL, true));
    EXPECT_EQ(res.data[1][1], Item(T_INTEGER, 3));

    // Github issue #128
    // Get result.
    res = standalone.RunQuery("match (n:person)-[:knows]->(m:person) return m.gender, count(m.gender)");
    sort(res.data.begin(), res.data.end(), [](const Row& l, const Row& r) { return l[0].boolVal < r[0].boolVal; });

    // Check answer.
    EXPECT_EQ(res.data.size(), 2ull);
    EXPECT_EQ(res.data[0][0], Item(T_BOOL, false));
    EXPECT_EQ(res.data[0][1], Item(T_INTEGER, 1));
    EXPECT_EQ(res.data[1][0], Item(T_BOOL, true));
    EXPECT_EQ(res.data[1][1], Item(T_INTEGER, 3));

    // Github issue #229
    auto [returnStrs, plan] = standalone.Explain("match (n)-->(m) match (m)-->(n) return n");
    u32 expandIntoCnt = 0;
    for (ExecutionStage* stage : plan->stages) {
        for (AbstractOp* op : stage->ops_) {
            expandIntoCnt += op->type == OpType_EXPAND_INTO;
        }
    }
    EXPECT_LE(expandIntoCnt, 1ul);
    delete plan;
}

TEST(test_query, manual_test) { test_query(); }
}  // namespace AGE
