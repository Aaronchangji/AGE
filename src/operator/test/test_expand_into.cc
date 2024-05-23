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

#include <string>
#include <utility>
#include <vector>
#include "gtest/gtest.h"

#include "base/expression.h"
#include "base/type.h"
#include "execution/message.h"
#include "operator/ops.h"
#include "storage/graph.h"
#include "storage/graph_loader.h"
#include "test/test_tool.h"

using std::pair;
using std::string;
using std::swap;
using std::vector;

namespace AGE {
const char expand_into_graphdir[] = AGE_TEST_DATA_DIR "/expand_into_data";

void testCouple() {
    Graph g(TestTool::getTestGraphDir(expand_into_graphdir));
    g.load();

    // Prepare PhysicalPlan manually.
    u32 compressBeginIdx = 2, srcCol = 0, dstCol = 1;
    Message m = Message::BuildEmptyMessage(0);
    m.plan->g = &g;
    m.plan->stages.emplace_back(new ExecutionStage());
    m.plan->stages.back()->appendOp(new ExpandIntoOp(srcCol, dstCol, 0, DirectionType_IN, compressBeginIdx));

    // Prepare data.
    vector<Item> src = g.getAllVtx();
    vector<pair<Item, Item>> answer;
    vector<Row>& input = m.data;
    for (Item& v : src) {
        for (Item& u : g.getNeighbor(v, 0, DirectionType_OUT)) {
            // printf("%s --> %s\n", v.DebugString().c_str(), u.DebugString().c_str());
            input.emplace_back(compressBeginIdx);
            input.back()[srcCol] = v;
            input.back()[dstCol] = u;

            // get answer.
            bool ok = false;
            for (const Item& w : g.getNeighbor(u, 0, DirectionType_OUT)) {
                if (w == v) ok = true;
            }
            if (!ok) continue;
            if (u.vertex.id > v.vertex.id) swap(u, v);
            answer.emplace_back(u, v);
        }
    }

    // Process operator.
    vector<Message> output;
    m.plan->stages.back()->processOp(m, output);
    vector<pair<Item, Item>> result;
    for (auto& msg : output)
        for (Row& r : msg.data) {
            Item &u = r[srcCol], &v = r[dstCol];
            if (u.vertex.id > v.vertex.id) swap(u, v);
            result.emplace_back(u, v);
        }

    sort(answer.begin(), answer.end());
    sort(result.begin(), result.end());

    EXPECT_EQ(answer.size(), result.size());
    for (size_t i = 0; i < std::min(answer.size(), result.size()); i++) {
        EXPECT_EQ(answer[i].first, result[i].first);
        EXPECT_EQ(answer[i].second, result[i].second);
    }
}

TEST(test_expand_into, test) {
    TestTool::prepareGraph(expand_into_graphdir, 0, 0, 0);
    testCouple();
    TestTool::prepareGraph(expand_into_graphdir, 10, 20, 1);
    testCouple();
    TestTool::prepareGraph(expand_into_graphdir, 100, 200, 2);
    testCouple();
}
}  // namespace AGE
