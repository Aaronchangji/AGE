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

using std::cout;
using std::endl;
using std::pair;
using std::string;
using std::to_string;
using std::tuple;
using std::vector;

namespace AGE {
const char prop_op_graphdir[] = AGE_TEST_DATA_DIR "/propOp_data";

void testCompress(ColIndex src, ColIndex dst, ColIndex compress, string pKey, u32 compressBeginIdx) {
    Graph g(TestTool::getTestGraphDir(prop_op_graphdir));
    g.load();
    assert(src != COLINDEX_COMPRESS);
    assert(src != dst);
    assert(src != compress);

    PropId pid = g.getPropId(pKey);

    // Prepare plan manually.
    Message m = Message::BuildEmptyMessage(0);
    m.plan->g = &g;
    m.plan->stages.emplace_back(new ExecutionStage());
    m.plan->stages.back()->appendOp(new PropertyOp(src, dst, pid, compress, compressBeginIdx));

    vector<Item> vtx = g.getAllVtx();

    for (Item& v : vtx) {
        m.data.emplace_back(compressBeginIdx);
        m.data.back()[src] = v;
        int vid = v.vertex.id;
        m.data.back().emplace_back(T_INTEGER, vid);
        m.data.back().emplace_back(T_INTEGER, vid + 1);
        m.data.back().emplace_back(T_INTEGER, vid + 2);
    }

    vector<Message> output;
    m.plan->stages.back()->processOp(m, output);

    // Get expected answer
    vector<pair<Item, Item>> answer;
    for (const Item& u : vtx) {
        Item prop = g.getProp(u, pid);
        answer.emplace_back(u, prop);
    }

    // Collect result.
    map<Item, vector<Item>> result;
    for (Message& msg : output) {
        for (Row& row : msg.data) {
            if (compress == COLINDEX_COMPRESS) {
                for (size_t i = compressBeginIdx; i < row.size(); i++) {
                    result[row[src]].emplace_back(row[i]);
                }
            } else if (compress == COLINDEX_NONE) {
                if (dst == COLINDEX_COMPRESS) {
                    EXPECT_EQ(row.size(), static_cast<u64>(compressBeginIdx + 1));
                    EXPECT_EQ(row[compressBeginIdx], g.getProp(row[src], pid));
                } else {
                    EXPECT_EQ(row.size(), static_cast<u64>(compressBeginIdx));
                }
            } else {
                result[row[src]].emplace_back(row[compress]);
            }
        }
    }

    // Check result.
    for (auto& [vtx, vec] : result) {
        std::sort(vec.begin(), vec.end());
        int vid = vtx.vertex.id;
        EXPECT_EQ(vec.size(), 3ull);
        EXPECT_EQ(vec[0], Item(T_INTEGER, vid));
        EXPECT_EQ(vec[1], Item(T_INTEGER, vid + 1));
        EXPECT_EQ(vec[2], Item(T_INTEGER, vid + 2));
    }
}

void testPropOp(ColIndex src, ColIndex dst, ColIndex compress, string pKey, u32 compressBeginIdx, u32 rowCnt = 1,
                u32 itemCnt = 1) {
    // printf("testPropOp(%d-->%d, compress-->%d, compressBeginIdx:%d\n", src, dst, compress, compressBeginIdx);
    Graph g(TestTool::getTestGraphDir(prop_op_graphdir));
    g.load();

    PropId pid = g.getPropId(pKey);

    // Prepare plan manually.
    Message m = Message::BuildEmptyMessage(0);
    m.plan->g = &g;
    m.plan->stages.emplace_back(new ExecutionStage());
    m.plan->stages.back()->appendOp(new PropertyOp(src, dst, pid, compress, compressBeginIdx));

    vector<Item> vtx = g.getAllVtx();
    size_t CompressItemCnt = 2;

    if (src == COLINDEX_COMPRESS) {
        m.data.emplace_back(compressBeginIdx + vtx.size());
        if (m.data.back().size()) m.data.back().setCount(rowCnt);
        for (size_t i = compressBeginIdx; i < m.data.back().size(); i++) {
            m.data.back()[i] = vtx[i - compressBeginIdx];
            m.data.back()[i].cnt = itemCnt;
        }
    } else {
        for (Item& v : vtx) {
            // ({Vertex|1,v|1}, {Unknown|1,}, {Integer|itemCnt,1}
            // Vertex;         Empty Normal;  Compress Column.
            m.data.emplace_back(compressBeginIdx + CompressItemCnt);
            m.data.back().setCount(rowCnt);
            m.data.back()[src] = v;
            m.data.back()[src].cnt = 1;
            // here is the compresse column items.
            for (size_t i = compressBeginIdx; i < m.data.back().size(); i++) {
                m.data.back()[i] = Item(T_INTEGER, 1);
                m.data.back()[i].cnt = itemCnt;
            }
        }
    }

    vector<Message> output;
    m.plan->stages.back()->processOp(m, output);

    // Check Item Count and Row Count.
    for (Message& msg : output) {
        for (Row& row : msg.data) {
            // All item in history column has cnt = 0.
            for (size_t i = 0; i < compressBeginIdx; i++) {
                if (row[i].type != T_UNKNOWN) {
                    EXPECT_EQ(1ull, row[i].cnt);
                }
            }

            // 1. src could be: COLINDEX_COMPRESS / inNormalColumn.
            // 2. dst could be: COLINDEX_COMPRESS / inNormalColumn
            // 3. compress could be: COLINDEX_COMPRESS / inNormalColumn / COLINDEX_NONE
            // 4. dst/src and compress could not be COLINDEX_COMPRESS at the same time.
            // Totally we have 2*2*3 - 2 - 1 = 9 cases.
            if (row.size() == 0) {
                EXPECT_EQ(0ull, row.count());
                continue;
            }
            if (src == COLINDEX_COMPRESS && dst == COLINDEX_COMPRESS && compress == COLINDEX_NONE) {
                // Nbs replace the original compress column, adopt the original itemCnt.
                EXPECT_EQ(rowCnt, row.count());
                for (size_t i = compressBeginIdx; i < row.size(); i++) {
                    EXPECT_EQ(itemCnt, row[i].cnt);
                }
            }
            if (src == COLINDEX_COMPRESS && dst == COLINDEX_COMPRESS && inNormalColumn(compress)) {
                // Move compress column to history column, multiply it to row count.
                // New items in compress column become 1.
                EXPECT_EQ(rowCnt * itemCnt, row.count());
                for (size_t i = compressBeginIdx; i < row.size(); i++) {
                    EXPECT_EQ(1ull, row[i].cnt);
                }
            }
            if (src == COLINDEX_COMPRESS && inNormalColumn(dst) && compress == COLINDEX_NONE) {
                // Move compress column to history column, multiply it to row count.
                // Do not generate new compress column, don't need to expect EQ.
                EXPECT_EQ(rowCnt * itemCnt, row.count());
            }
            if (src == COLINDEX_COMPRESS && inNormalColumn(dst) && inNormalColumn(compress)) {
                // From compress to normal column, multiply the item cnt to row count.
                EXPECT_EQ(rowCnt * itemCnt, row.count());
            }
            if (inNormalColumn(src) && dst == COLINDEX_COMPRESS && compress == COLINDEX_NONE) {
                // Remove the compress column, multiply the sum of item count to row count.
                EXPECT_EQ(rowCnt * itemCnt * CompressItemCnt, row.count());
                for (size_t i = compressBeginIdx; i < row.size(); i++) {
                    EXPECT_EQ(1ull, row[i].cnt);
                }
            }
            if (inNormalColumn(src) && dst == COLINDEX_COMPRESS && inNormalColumn(compress)) {
                // Src from history column, new items' cnt is 1.
                EXPECT_EQ(rowCnt * itemCnt, row.count());
                for (size_t i = compressBeginIdx; i < row.size(); i++) {
                    EXPECT_EQ(1ull, row[i].cnt);
                }
            }
            if (inNormalColumn(src) && inNormalColumn(dst) && compress == COLINDEX_NONE) {
                // Remove the compress column, multiply the sum of item count to row count.
                EXPECT_EQ(rowCnt * itemCnt * CompressItemCnt, row.count());
            }
            if (inNormalColumn(src) && inNormalColumn(dst) && inNormalColumn(compress)) {
                // Move the compress column to normal column, each row multiply one item count.
                EXPECT_EQ(rowCnt * itemCnt, row.count());
            }
            if (inNormalColumn(src) && inNormalColumn(dst) && compress == COLINDEX_COMPRESS) {
                // Compress column not change.
                EXPECT_EQ(rowCnt, row.count());
                for (size_t i = compressBeginIdx; i < row.size(); i++) {
                    EXPECT_EQ(itemCnt, row[i].cnt);
                }
            }
        }
    }

    // Get expected answer
    vector<pair<Item, Item>> answer;
    for (const Item& u : vtx) {
        Item prop = g.getProp(u, pid);
        if (src != COLINDEX_COMPRESS && inNormalColumn(compress)) {
            for (size_t i = 0; i < CompressItemCnt; i++) answer.emplace_back(u, prop);
        } else {
            answer.emplace_back(u, prop);
        }
    }

    // Get result
    vector<pair<Item, Item>> result;
    for (Message& msg : output) {
        for (Row& row : msg.data) {
            if (dst == COLINDEX_COMPRESS) {
                for (size_t i = compressBeginIdx; i < row.size(); i++) {
                    if (src == COLINDEX_COMPRESS) {
                        if (compress == COLINDEX_NONE) {
                            result.emplace_back(Item(T_INTEGER, 0), row[i]);
                        } else {
                            result.emplace_back(row[compress], row[i]);
                        }
                    } else {
                        result.emplace_back(row[src], row[i]);
                    }
                }
            } else {
                if (src == COLINDEX_COMPRESS) {
                    if (compress == COLINDEX_NONE) {
                        result.emplace_back(Item(T_INTEGER, 0), row[dst]);
                    } else {
                        result.emplace_back(row[compress], row[dst]);
                    }
                } else {
                    result.emplace_back(row[src], row[dst]);
                }
            }
        }
    }

    // If result will drop source vtx, drop it from answer to keep same order
    if (src == COLINDEX_COMPRESS && compress == COLINDEX_NONE)
        for (auto& [x, y] : answer) x = Item(T_INTEGER, 0);

    // Check result
    sort(answer.begin(), answer.end());
    sort(result.begin(), result.end());

    /*
    printf("answer: %lu, result: %lu\n", answer.size(), result.size());
    printf("answer:\n");
    for (size_t i = 0; i < answer.size(); i++) {
        printf("%s, %s\n", answer[i].first.DebugString().c_str(), answer[i].second.DebugString().c_str());
    }

    printf("result:\n");
    for (size_t i = 0; i < result.size(); i++) {
        printf("%s, %s\n", result[i].first.DebugString().c_str(), result[i].second.DebugString().c_str());
    }
    */

    EXPECT_EQ(answer.size(), result.size());
    for (size_t i = 0; i < answer.size() && i < result.size(); i++) {
        // if (answer[i].first != result[i].first) printf("i:%lu, %s, %s\n", i, answer[i].first.DebugString().c_str(),
        // result[i].first.DebugString().c_str());
        // if (answer[i].second != result[i].second) printf("i:%lu, %s, %s\n", i,
        // answer[i].second.DebugString().c_str(), result[i].second.DebugString().c_str());
        // if (answer[i].second.type != T_NULL || result[i].second.type != T_NULL) {
        //     EXPECT_EQ(answer[i].second, result[i].second);
        // }
        if (src == COLINDEX_COMPRESS && compress == COLINDEX_NONE) continue;
        EXPECT_EQ(answer[i].first, result[i].first);
    }
}

void testPropOpOverall(int rc = 1, int ic = 1) {
    ColIndex compress = COLINDEX_COMPRESS, ul = COLINDEX_NONE;

    // normal, normal, normal
    testPropOp(0, 1, 2, "bool", 3, rc, ic);
    testPropOp(0, 1, 2, "xxx", 4, rc, ic);
    testCompress(0, 1, 2, "int", 3);

    // normal, normal, ul
    testPropOp(0, 1, ul, "bool", 3, rc, ic);
    testPropOp(0, 1, ul, "string", 2, rc, ic);
    testPropOp(0, 1, ul, "xxx", 2, rc, ic);
    testCompress(0, 1, ul, "int", 2);

    // normal, normal, compress
    testPropOp(0, 1, compress, "string", 2, rc, ic);
    testCompress(0, 1, compress, "int", 3);

    // normal, compress, normal
    testPropOp(0, compress, 2, "int", 4, rc, ic);
    testPropOp(0, compress, 2, "xxx", 3, rc, ic);
    testCompress(0, compress, 2, "int", 4);

    // normal, compress, ul
    testPropOp(0, compress, ul, "int", 4, rc, ic);
    testCompress(0, compress, ul, "string", 3);

    // compress, normal, ul
    testPropOp(compress, 0, ul, "bool", 1, rc, ic);
    testPropOp(compress, 1, ul, "bool", 2);

    // compress, normal, normal
    testPropOp(compress, 0, 1, "bool", 4);

    // compress, compress, ul
    testPropOp(compress, compress, ul, "float", 0);

    // compress, compress, normal
    testPropOp(compress, compress, 0, "float", 1);
}

void testPropOpOverallWithCnt() {
    testPropOpOverall(1, 9999);
    testPropOpOverall(10055, 1);
    testPropOpOverall(12345, 6789);
}

TEST(test_propOp, synthetic_test) {
    TestTool::prepareGraph(prop_op_graphdir, 0, 0, 0);
    testPropOpOverall();
    TestTool::prepareGraph(prop_op_graphdir, 5, 0, 1);
    testPropOpOverall();
    TestTool::prepareGraph(prop_op_graphdir, 10, 20, 5);
    testPropOpOverall();
    testPropOpOverallWithCnt();
}
}  // namespace AGE
