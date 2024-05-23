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

#include <glog/logging.h>
#include <string>
#include <utility>
#include <vector>
#include "base/item.h"
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
const char expand_graphdir[] = AGE_TEST_DATA_DIR "/expand_data";

AbstractOp* createExpandOp(ColIndex srcCol, ColIndex dstCol, ColIndex compressCol, LabelId eLabel, LabelId dstVLabel,
                           DirectionType dir, u32 compressBeginIdx) {
    if (dstCol == COLINDEX_NONE)
        return new ExpandULOp(srcCol, dstCol, compressCol, eLabel, dstVLabel, dir, compressBeginIdx);
    if (srcCol == COLINDEX_COMPRESS && dstCol == COLINDEX_COMPRESS)
        return new ExpandCCOp(srcCol, dstCol, compressCol, eLabel, dstVLabel, dir, compressBeginIdx);
    else if (srcCol == COLINDEX_COMPRESS)
        return new ExpandCNOp(srcCol, dstCol, compressCol, eLabel, dstVLabel, dir, compressBeginIdx);
    else if (dstCol == COLINDEX_COMPRESS)
        return new ExpandNCOp(srcCol, dstCol, compressCol, eLabel, dstVLabel, dir, compressBeginIdx);
    else
        return new ExpandNNOp(srcCol, dstCol, compressCol, eLabel, dstVLabel, dir, compressBeginIdx);
}

bool checkLabel(const Item& vtx, LabelId label) {
    assert(vtx.type == T_VERTEX);
    if (label == ALL_LABEL) return true;
    if (label == INVALID_LABEL) return false;
    return vtx.vertex.label == label;
}

void testExpand(int edge_label, DirectionType direction, ColIndex src, ColIndex dst, ColIndex compress,
                LabelId dstLabel, u32 compressBeginIdx, size_t rowCnt = 1, size_t itemCnt = 1) {
    Graph g(TestTool::getTestGraphDir(expand_graphdir));
    g.load();

    // Rely on that Graph::getLabelId return 0 if not exist.
    LabelId lid = g.getLabelId({static_cast<char>(edge_label + 'a')});

    // Prepare Physical plan manually.
    Message m = Message::BuildEmptyMessage(0);
    m.plan->g = &g;
    m.plan->stages.emplace_back(new ExecutionStage());
    m.plan->stages.back()->appendOp(createExpandOp(src, dst, compress, lid, dstLabel, direction, compressBeginIdx));
    // printf("test %s\n", m.plan->ops.back()->DebugString().c_str());

    // Prepare operator input message data(Rows) manually.
    vector<Item> vtx = g.getAllVtx();
    size_t CompressItemCnt = 2;

    if (src == COLINDEX_COMPRESS) {
        // From compress column.
        m.data.emplace_back(compressBeginIdx + vtx.size());
        if (m.data.back().size()) m.data.back().setCount(rowCnt);
        for (size_t i = compressBeginIdx; i < m.data.back().size(); i++) {
            m.data.back()[i] = vtx[i - compressBeginIdx];
            m.data.back()[i].cnt = itemCnt;
        }
    } else {
        // From history column.
        for (Item& v : vtx) {
            // here we use two more item to represent compress column (test count).
            m.data.emplace_back(compressBeginIdx + CompressItemCnt);
            m.data.back().setCount(rowCnt);
            m.data.back()[src] = v;
            m.data.back()[src].cnt = 1;
            // here is the compresse column items.
            for (size_t i = compressBeginIdx; i < m.data.back().size(); i++) {
                m.data.back()[i] = Item(T_INTEGER, 0);
                m.data.back()[i].cnt = itemCnt;
            }
        }
    }

    // Process operator.
    vector<Message> output;
    m.plan->stages.back()->processOp(m, output);

    // Check Item Count and Row Count.
    for (Message& msg : output) {
        for (Row& row : msg.data) {
            // All item in history column has cnt = 1.
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
                // Move compress column to normal column, multiply it to rowCnt.
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

    // Get expected answer.
    vector<pair<Item, Item>> answer;
    for (const Item& u : vtx) {
        vector<Item> nb = g.getNeighbor(u, lid, direction);
        for (const Item& v : nb)
            if (checkLabel(v, dstLabel)) {
                // printf("%s --> %s\n", u.DebugString().c_str(), v.DebugString().c_str());
                answer.emplace_back(v, u);
            }
    }

    // Get result.
    vector<pair<Item, Item>> result;
    for (Message& msg : output)
        for (Row& row : msg.data) {
            if (dst == COLINDEX_COMPRESS) {
                // To compress column.
                for (size_t i = compressBeginIdx; i < row.size(); i++) {
                    if (src == COLINDEX_COMPRESS) {
                        if (compress == COLINDEX_NONE) {
                            result.emplace_back(row[i], Item(T_INTEGER, 0));
                        } else {
                            result.emplace_back(row[i], row[compress]);
                        }
                    } else {
                        result.emplace_back(row[i], row[src]);
                    }
                }
            } else {
                // To history column.
                if (src == COLINDEX_COMPRESS) {
                    if (compress == COLINDEX_NONE) {
                        result.emplace_back(row[dst], Item(T_INTEGER, 0));
                    } else {
                        result.emplace_back(row[dst], row[compress]);
                    }
                } else {
                    result.emplace_back(row[dst], row[src]);
                }
            }
        }

    // Check result.
    sort(answer.begin(), answer.end());
    sort(result.begin(), result.end());

    // printf("answer: %lu, result: %lu\n", answer.size(), result.size());
    EXPECT_EQ(answer.size(), result.size());
    for (size_t i = 0; i < answer.size() && i < result.size(); i++) {
        // if (answer[i].first != result[i].first) printf("i:%lu, %s, %s\n", i, answer[i].first.DebugString().c_str(),
        // result[i].first.DebugString().c_str());
        EXPECT_EQ(answer[i].first, result[i].first);
        if (src == COLINDEX_COMPRESS && compress == COLINDEX_NONE) continue;
        // if (answer[i].second != result[i].second) printf("%s, %s\n", answer[i].second.DebugString().c_str(),
        // result[i].second.DebugString().c_str());
        EXPECT_EQ(answer[i].second, result[i].second);
    }
}

void testExpandOverall(int edge_label, DirectionType dir, size_t rc = 1, size_t ic = 1) {
    LabelId nol = ALL_LABEL;
    ColIndex compress = COLINDEX_COMPRESS, ul = COLINDEX_NONE;
    testExpand(edge_label, dir, 0, 1, ul, nol, 2, rc, ic);
    testExpand(edge_label, dir, 0, 1, ul, 1, 2, rc, ic);
    testExpand(edge_label, dir, 0, compress, ul, nol, 2, rc, ic);
    testExpand(edge_label, dir, 0, compress, ul, 2, 2, rc, ic);
    testExpand(edge_label, dir, compress, 1, ul, nol, 2, rc, ic);
    testExpand(edge_label, dir, compress, 1, ul, 3, 2, rc, ic);
    testExpand(edge_label, dir, compress, compress, 0, nol, 2, rc, ic);
    testExpand(edge_label, dir, compress, compress, ul, nol, 2, rc, ic);
    testExpand(edge_label, dir, compress, compress, ul, 4, 2, rc, ic);

    testExpand(edge_label, dir, 0, 1, ul, nol, 4, rc, ic);
    testExpand(edge_label, dir, 0, 1, ul, 1, 4, rc, ic);
    testExpand(edge_label, dir, 0, compress, ul, nol, 4, rc, ic);
    testExpand(edge_label, dir, 0, compress, ul, 2, 4, rc, ic);
    testExpand(edge_label, dir, compress, 1, ul, nol, 4, rc, ic);
    testExpand(edge_label, dir, compress, 1, ul, 3, 4, rc, ic);
    testExpand(edge_label, dir, compress, compress, ul, nol, 4, rc, ic);
    testExpand(edge_label, dir, compress, compress, 0, 4, 4, rc, ic);
}

void testExpandOverallWithCnt(int edge_label, DirectionType dir) {
    testExpandOverall(edge_label, dir, 1, 9999);
    testExpandOverall(edge_label, dir, 10055, 1);
    testExpandOverall(edge_label, dir, 12345, 6789);
}

TEST(test_expand, synthetic_test) {
    TestTool::prepareGraph(expand_graphdir, 0, 0, 0);
    testExpandOverall(0, DirectionType_IN);
    TestTool::prepareGraph(expand_graphdir, 5, 10, 1);
    testExpandOverall(0, DirectionType_BOTH);
    TestTool::prepareGraph(expand_graphdir, 1000, 2000, 5);
    testExpandOverall(0, DirectionType_BOTH);
    testExpandOverall(4, DirectionType_OUT);
    testExpandOverall(4, DirectionType_IN);
    testExpandOverallWithCnt(0, DirectionType_BOTH);
}
}  // namespace AGE
