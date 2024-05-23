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

#include "base/type.h"
#include "execution/message.h"
#include "execution/physical_plan.h"
#include "operator/vertex_scan_op.h"
#include "storage/graph.h"
#include "storage/graph_loader.h"
#include "test/test_tool.h"

using std::pair;
using std::string;
using std::vector;

namespace AGE {
const char vertex_scan_graphdir[] = AGE_TEST_DATA_DIR "/vertex_scan_data";

struct {
    bool operator()(Row a, Row b) const {
        if (a.size() != b.size()) return a.size() < b.size();
        for (size_t index = 0; index < a.size(); index++) {
            if (a.at(index) == b.at(index)) continue;
            return a.at(index) < b.at(index);
        }
        return false;
    }
} ItemVecGreater;

void testVertexScanOp(int label, ColIndex dstCol, u32 compressBeginIdx) {
    Graph g(TestTool::getTestGraphDir(vertex_scan_graphdir));
    g.load();

    // Prepare Physical plan manually.
    Message m = Message::BuildEmptyMessage(0);
    m.plan->g = &g;
    m.plan->stages.emplace_back(new ExecutionStage());
    m.plan->stages.back()->appendOp(new VertexScanOp(label, dstCol, compressBeginIdx));

    // Process operator
    vector<Message> output;
    m.plan->stages.back()->processOp(m, output);

    // Get expected answer.
    vector<Item> answer = label == 0 ? g.getAllVtx() : g.getLabelVtx(label);
    sort(answer.begin(), answer.end());

    // Get result.
    vector<Item> result;
    for (Message& msg : output) {
        for (Row& r : msg.data) {
            if (dstCol < 0) {
                // compress column.
                for (size_t i = compressBeginIdx; i < r.size(); i++) result.emplace_back(std::move(r[i]));
            } else {
                // History column.
                result.emplace_back(std::move(r[dstCol]));
            }
        }
    }
    sort(result.begin(), result.end());

    // Check result.
    EXPECT_EQ(answer.size(), result.size());
    for (size_t i = 0; i < answer.size() && i < result.size(); i++) {
        EXPECT_EQ(answer[i], result[i]);
    }
}

// Check case:
// Already matched vertex A, new introduce vertex with label B
void testCartisanProduct(int label, ColIndex dstCol, int colNum, ColIndex compressDst) {
    // Only accept test one vertex now
    ASSERT_TRUE(compressDst < colNum && dstCol < colNum && dstCol != compressDst);

    Graph g(TestTool::getTestGraphDir(vertex_scan_graphdir));
    g.load();

    // Prepare Physical plan manually.
    Message m = Message::BuildEmptyMessage(0);
    m.plan->g = &g;
    m.plan->stages.emplace_back(new ExecutionStage());
    m.plan->stages.back()->appendOp(new VertexScanOp(label, dstCol, colNum, compressDst, true));

    // Prepare input data manually
    vector<Row> newData;
    if (compressDst != COLINDEX_NONE) {
        Row r(colNum);
        g.getAllVtx(r);
        r.setCount(1);
        newData.emplace_back(std::move(r));
    } else {
        vector<Item> vec;
        g.getAllVtx(vec);
        for (Item& v : vec) {
            Row r(colNum);
            r[0] = std::move(v);
            newData.emplace_back(std::move(r));
        }
    }
    m.data.swap(newData);

    // Process operator
    vector<Message> output;
    m.plan->stages.back()->processOp(m, output);

    // Get expected answer.
    vector<Row> answer;
    vector<Item> baseVertex;
    g.getAllVtx(baseVertex);
    vector<Item> newVertex;
    if (label == 0)
        g.getAllVtx(newVertex);
    else
        g.getLabelVtx(label, newVertex);
    // Do cartisan product:
    for (Item& base : baseVertex) {
        for (Item& newV : newVertex) {
            Row r(2);
            r[0] = base;
            r[1] = newV;
            answer.emplace_back(r);
        }
    }
    sort(answer.begin(), answer.end(), ItemVecGreater);

    // Get result:
    vector<Row> result;
    for (Message& msg : output) {
        for (Row& r : msg.data) {
            Row newRow;
            // Retreive base vertex:
            if (compressDst == COLINDEX_NONE) {
                // Store base vertex
                newRow.emplace_back(r[0]);
                // Store new vertex
                if (dstCol == COLINDEX_COMPRESS) {
                    for (size_t index = colNum; index < r.size(); index++) {
                        result.emplace_back(newRow);
                        result.back().emplace_back(r[index]);
                    }
                } else {
                    newRow.emplace_back(r[dstCol]);
                    result.emplace_back(newRow);
                }
            } else if (compressDst == COLINDEX_COMPRESS) {
                Row newRow(2);
                newRow[1] = r[dstCol];
                for (size_t index = colNum; index < r.size(); index++) {
                    newRow[0] = r[index];
                    result.emplace_back(newRow);
                }
            } else {
                // Store base vertex
                newRow.emplace_back(r[compressDst]);
                if (dstCol == COLINDEX_COMPRESS) {
                    for (size_t index = colNum; index < r.size(); index++) {
                        result.emplace_back(newRow);
                        result.back().emplace_back(r[index]);
                    }
                } else {
                    newRow.emplace_back(r[dstCol]);
                    result.emplace_back(newRow);
                }
            }
        }
    }
    sort(result.begin(), result.end(), ItemVecGreater);

    // Check result.
    EXPECT_EQ(answer.size(), result.size());
    for (size_t i = 0; i < answer.size(); i++) {
        EXPECT_EQ(answer[i][0], result[i][0]);
        EXPECT_EQ(answer[i][1], result[i][1]);
    }
}

/**
 * @brief Test the processing performance for cartisan product
 */
void testCPPerformance(int scale, ColIndex dstCol, ColIndex compressDst, int colNum) {
    // Only accept test one vertex now
    ASSERT_TRUE(compressDst < colNum && dstCol < colNum && dstCol != compressDst);
    TestTool::prepareGraph(vertex_scan_graphdir, scale, scale, 1);
    Graph g(TestTool::getTestGraphDir(vertex_scan_graphdir));
    g.load();

    // Prepare Physical plan manually.
    Message m = Message::BuildEmptyMessage(0);
    m.plan->g = &g;
    m.plan->stages.emplace_back(new ExecutionStage());
    m.plan->stages.back()->appendOp(new VertexScanOp(ALL_LABEL, dstCol, colNum, compressDst, true));

    // Prepare input data manually
    vector<Row> newData;
    if (compressDst != COLINDEX_NONE) {
        Row r(colNum);
        g.getAllVtx(r);
        r.setCount(1);
        newData.emplace_back(std::move(r));
    } else {
        vector<Item> vec;
        g.getAllVtx(vec);
        for (Item& v : vec) {
            Row r(colNum);
            r[0] = std::move(v);
            newData.emplace_back(std::move(r));
        }
    }
    m.data.swap(newData);

    // Process operator
    vector<Message> output;
    // u64 startTime = TestTool::getTimeNs();
    m.plan->stages.back()->processOp(m, output);
    // u64 processTime = TestTool::getTimeNs() - startTime;

    // printf("Testing performance on scale %d*%d, compressDst:%d, dstCol:%d, colNum: %d\n Time cost: %.6f ms\n",
    // scale,
    //      scale, compressDst, dstCol, colNum, processTime / 1e6);
}
TEST(test_vertex_scan, test_label_vertex_scan) {
    TestTool::prepareGraph(vertex_scan_graphdir, 0, 0, 0);
    testVertexScanOp(ALL_LABEL, 0, 1);
    testVertexScanOp(ALL_LABEL, COLINDEX_COMPRESS, 1);
    testVertexScanOp(ALL_LABEL, 0, 2);
    testVertexScanOp(ALL_LABEL, COLINDEX_COMPRESS, 2);
    TestTool::prepareGraph(vertex_scan_graphdir, 20, 0, 5);
    testVertexScanOp(ALL_LABEL, 0, 2);
    testVertexScanOp(1, COLINDEX_COMPRESS, 2);
    TestTool::prepareGraph(vertex_scan_graphdir, 3000, 1000, 10);
    testVertexScanOp(ALL_LABEL, 0, 1);
    testVertexScanOp(1, 1, 2);
}

TEST(test_vertex_scan, test_cartisan_product) {
    TestTool::prepareGraph(vertex_scan_graphdir, 20, 0, 5);
    testCartisanProduct(ALL_LABEL, 2, 3, COLINDEX_COMPRESS);
    testCartisanProduct(ALL_LABEL, 2, 3, COLINDEX_NONE);
    testCartisanProduct(ALL_LABEL, 2, 3, 1);
    testCartisanProduct(ALL_LABEL, COLINDEX_COMPRESS, 3, 1);
    testCartisanProduct(ALL_LABEL, COLINDEX_COMPRESS, 3, COLINDEX_NONE);
    TestTool::prepareGraph(vertex_scan_graphdir, 30, 100, 10);
    testCartisanProduct(ALL_LABEL, 2, 3, COLINDEX_COMPRESS);
    testCartisanProduct(ALL_LABEL, 2, 3, COLINDEX_NONE);
    testCartisanProduct(ALL_LABEL, 2, 3, 1);
    testCartisanProduct(ALL_LABEL, COLINDEX_COMPRESS, 3, 1);
    testCartisanProduct(ALL_LABEL, COLINDEX_COMPRESS, 3, COLINDEX_NONE);
}

TEST(test_vertex_scan, test_cartisan_performance) {
    int scale = 50, colNum = 3;
    // Paras:       scale | dstCol | compressDst | colNum
    testCPPerformance(scale, 2, COLINDEX_COMPRESS, colNum);
    testCPPerformance(scale, 2, COLINDEX_NONE, colNum);
    testCPPerformance(scale, 2, 1, colNum);
    testCPPerformance(scale, COLINDEX_COMPRESS, 1, colNum);
    testCPPerformance(scale, COLINDEX_COMPRESS, COLINDEX_NONE, colNum);
    scale = 300;
    testCPPerformance(scale, 2, COLINDEX_COMPRESS, colNum);
    testCPPerformance(scale, 2, COLINDEX_NONE, colNum);
    testCPPerformance(scale, 2, 1, colNum);
    testCPPerformance(scale, COLINDEX_COMPRESS, 1, colNum);
    testCPPerformance(scale, COLINDEX_COMPRESS, COLINDEX_NONE, colNum);
}
}  // namespace AGE
