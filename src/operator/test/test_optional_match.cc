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
#include <algorithm>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>
#include "base/graph_entity.h"
#include "base/item.h"
#include "base/row.h"
#include "execution/mailbox.h"
#include "execution/message_header.h"
#include "gtest/gtest.h"

#include "base/expression.h"
#include "base/type.h"
#include "execution/message.h"
#include "execution/result_collector.h"
#include "execution/standalone_executor.h"
#include "operator/abstract_op.h"
#include "operator/expand_cc_op.h"
#include "operator/filter_op.h"
#include "operator/ops.h"
#include "operator/optional_match_op.h"
#include "operator/property_op.h"
#include "operator/subquery_util.h"
#include "operator/vertex_scan_op.h"
#include "storage/graph.h"
#include "storage/graph_loader.h"
#include "test/test_tool.h"
#include "util/tool.h"

namespace AGE {
namespace testOptionalMatch {
const char optional_graphdir[] = AGE_TEST_DATA_DIR "/optional_match_op_data";
const u32 NUM_VERTICES = 10000;
const u32 NUM_EDGES = 20000;
const u8 NUM_PROPERTY = 4;
const u8 MAX_NUM_COMPRESS_ITEMS = 5;
const Item CONST_INT_ITEM = Item(T_INTEGER, 5000);
const Item CONST_UNKNOWN_ITEM = Item(T_UNKNOWN);
const Item CONST_NULL_ITEM = Item(T_NULL);

Result RunSinglePlan(StandaloneExecutor* executor, Graph* g, Mailbox* mb, ResultCollector* rc,
                     shared_ptr<PhysicalPlan>&& plan, Message& m) {
    QueryId qid = m.header.qid;
    executor->insertPlan(std::move(plan));
    executor->insertQueryMeta(qid, QueryMeta("", Tool::getTimeNs() / 1000, "", {}));
    mb->send_local(std::move(m));

    Result r = rc->pop();
    executor->erasePlan(qid);

    return r;
}

AbstractOp* CreateSubqueryEntryOp(const vector<int>& subOps, const vector<ColIndex>& inputColumns,
                                  const SubqueryUtil::ColumnMapT& outputColumns, ColIndex compressDst,
                                  u32 compressBeginIdx, u32 innerCompressBeginIdx) {
    return new OptionalMatchOp(compressBeginIdx, subOps.size(), inputColumns, compressDst, outputColumns,
                               innerCompressBeginIdx);
}

vector<AbstractOp*> CreateSubqueryOps(Graph& g, u32 compressBeginIdx, vector<int>& subops) {
    // To place C and C.int;
    CHECK_GE(compressBeginIdx, static_cast<u32>(3));
    const int srcColIdx = 0;
    const int dstColIdx = compressBeginIdx - 2;
    const int propColIdx = compressBeginIdx - 1;

    vector<AbstractOp*> ret;
    ret.emplace_back(
        new ExpandNNOp(srcColIdx, dstColIdx, COLINDEX_NONE, ALL_LABEL, ALL_LABEL, DirectionType_OUT, compressBeginIdx));
    ret.emplace_back(new PropertyOp(dstColIdx, propColIdx, g.getPropId("int"), COLINDEX_NONE, compressBeginIdx));

    Expression filter(EXP_GT);
    filter.EmplaceChild(EXP_VARIABLE, propColIdx);
    filter.EmplaceChild(EXP_CONSTANT, CONST_INT_ITEM);
    ret.emplace_back(new FilterOp(filter, compressBeginIdx));

    subops.emplace_back(0);
    // for (size_t i = 0; i < ret.size(); i++) {
    //     ret[i]->nextOp.emplace_back((i == ret.size() - 1) ? 0 : i + 2);
    // }
    return ret;
}

void GetAnswerForOptionalMatch(const vector<Row>& input, vector<Row>& answer, Graph* g, bool hasCompressInput,
                               int numColumns, ColIndex compressDst) {
    vector<Row> realInput, tmpAnswer;
    if (hasCompressInput) {
        for (const Row& row : input) {
            row.decompress(realInput, numColumns, COLINDEX_COMPRESS);
        }
    } else {
        realInput = input;
    }

    for (const Row& row : realInput) {
        const Item& src = hasCompressInput ? row[numColumns] : row[0];
        vector<Item> nxt;
        g->getNeighbor(src, ALL_LABEL, DirectionType_OUT, nxt, ALL_LABEL);
        nxt.erase(
            std::remove_if(nxt.begin(), nxt.end(),
                           [g](const Item& dst) { return g->getProp(dst, g->getPropId("int")) <= CONST_INT_ITEM; }),
            nxt.end());
        if (!nxt.size()) {
            tmpAnswer.emplace_back(row);
            tmpAnswer.back()[1] = Item(T_NULL);
        } else {
            for (const Item& dst : nxt) {
                tmpAnswer.emplace_back(row);
                tmpAnswer.back()[1] = dst;
            }
        }
    }

    if (!hasCompressInput && compressDst == COLINDEX_COMPRESS) {
        answer = tmpAnswer;
    } else {
        for (const Row& r : tmpAnswer) r.decompress(answer, numColumns, compressDst);
    }
}

/*
    Test query template:
    (input: [a, a.int, a.string, ..., [compressed vertices]])
    OPTIONAL match (a)-[]->(b)
    where b.int > CONST
*/
void TestOptionalMatch(Graph& g, StandaloneExecutor* exe, Mailbox* mb, ResultCollector* rc, int numColumns,
                       int numInputColumns, bool hasCompress, bool hasCompressInput, ColIndex compressDst,
                       int numInputs) {
    CHECK_GT(numColumns, 0);
    CHECK_GT(numInputColumns, 0);
    CHECK_GE(numColumns, numInputColumns);

    Message m = Message::BuildEmptyMessage(0);

    // prepare input data
    vector<Item> allVtx = g.getAllVtx();
    CHECK(numInputs <= static_cast<int>(allVtx.size()));
    vector<Item> inputs(allVtx.begin(), allVtx.begin() + numInputs);

    // insert columns
    for (auto& v : inputs) {
        if (hasCompress) {
            int compressLen = TestTool::Rand() % MAX_NUM_COMPRESS_ITEMS + 1;
            m.data.emplace_back(numColumns + compressLen);
            m.data.back()[0] = v;

            for (size_t i = 1; i < static_cast<size_t>(numColumns); ++i) {
                m.data.back()[i] = g.getProp(v, i % NUM_PROPERTY + 1);
            }

            for (size_t i = static_cast<size_t>(numColumns); i < m.data.back().size(); ++i) {
                m.data.back()[i] = allVtx[i - numColumns];
            }
        } else {
            m.data.emplace_back(numColumns);
            m.data.back()[0] = v;

            for (size_t i = 1; i < static_cast<size_t>(numColumns); ++i) {
                m.data.back()[i] = g.getProp(v, i % NUM_PROPERTY + 1);
            }
        }
        // LOG(INFO) << m.data.back().DebugString();
    }

    SubqueryUtil::ColumnMapT outputColumns;
    vector<int> subops;
    u32 compressBeginIdx = numColumns;
    u32 innerCompressBeginIdx = numInputColumns + 2;

    vector<ColIndex> inputColumns;
    for (int i = 0; i < numInputColumns - 1; ++i) {
        inputColumns.emplace_back(i);
    }
    if (hasCompressInput) {
        inputColumns.emplace_back(COLINDEX_COMPRESS);
    } else {
        inputColumns.emplace_back(numInputColumns - 1);
    }
    outputColumns.emplace_back(std::make_pair(1, 1));

    shared_ptr<PhysicalPlan> plan = std::make_shared<PhysicalPlan>(PhysicalPlan());
    plan->qid = m.header.qid;
    plan->g = &g;
    vector<AbstractOp*> subquery = CreateSubqueryOps(g, innerCompressBeginIdx, subops);
    AbstractOp* subqueryEntryOp = CreateSubqueryEntryOp(subops, inputColumns, outputColumns, compressDst,
                                                        compressBeginIdx, innerCompressBeginIdx);

    plan->AppendStage(new ExecutionStage());
    plan->stages.back()->appendOp(subqueryEntryOp);

    for (size_t idx = 0; idx < subops.size(); idx++) {
        plan->AppendStage(new ExecutionStage(), 0);
        if (idx + 1 < subops.size()) {
            plan->stages.back()->ops_.insert(plan->stages.back()->ops_.end(), subquery.begin() + subops[idx],
                                             subquery.begin() + subops[idx + 1]);
        } else {
            plan->stages.back()->ops_.insert(plan->stages.back()->ops_.end(), subquery.begin() + subops[idx],
                                             subquery.end());
        }
        plan->stages.back()->next_stages_.emplace_back(0);
    }
    plan->AppendStage(new ExecutionStage(), 0);
    plan->stages.back()->appendOp(new EndOp());

    m.plan = plan;
    // LOG(INFO) << "\n" << m.plan->DebugString();

    vector<Row> answer;
    GetAnswerForOptionalMatch(m.data, answer, &g, hasCompressInput, numColumns, compressDst);
    sort(answer.begin(), answer.end(), [](const Row& lhs, const Row& rhs) { return lhs[0] < rhs[0]; });

    Result result = RunSinglePlan(exe, &g, mb, rc, std::move(plan), m);
    sort(result.data.begin(), result.data.end(), [](const Row& lhs, const Row& rhs) { return lhs[0] < rhs[0]; });

    EXPECT_EQ(answer.size(), result.data.size());
    for (size_t i = 0; i < answer.size(); i++) {
        EXPECT_EQ(answer[i][0], result.data[i][0]);
        if (answer[i][1] == Item(T_NULL)) {
            EXPECT_EQ(answer[i][1], result.data[i][1]);
        }
    }
}

TEST(test_optional_match, input_no_compress) {
    TestTool::prepareGraph(optional_graphdir, NUM_VERTICES, NUM_EDGES, 3);
    Graph g(TestTool::getTestGraphDir(optional_graphdir));
    CHECK(g.load()) << "Graph load failed!";

    Config* config = Config::GetInstance();
    Mailbox mb(config->num_threads_, config->num_mailbox_threads_);
    ResultCollector rc;
    Nodes nodes = Nodes::CreateStandaloneNodes();
    StandaloneExecutor* exe = new StandaloneExecutor(nodes, g, mb, rc, config->num_threads_);
    exe->start();

    // Input contains no compressCol.
    TestOptionalMatch(g, exe, &mb, &rc, 3, 1, false, false, COLINDEX_NONE, NUM_VERTICES);

    delete exe;
}

TEST(test_optional_match, input_has_compress) {
    TestTool::prepareGraph(optional_graphdir, NUM_VERTICES, NUM_EDGES, 3);
    Graph g(TestTool::getTestGraphDir(optional_graphdir));
    CHECK(g.load()) << "Graph load failed!";

    Config* config = Config::GetInstance();
    Mailbox mb(config->num_threads_, config->num_mailbox_threads_);
    ResultCollector rc;
    Nodes nodes = Nodes::CreateStandaloneNodes();
    StandaloneExecutor* exe = new StandaloneExecutor(nodes, g, mb, rc, config->num_threads_);
    exe->start();

    // Input contains compressCol, but do not have compressInput
    TestOptionalMatch(g, exe, &mb, &rc, 3, 1, true, false, COLINDEX_COMPRESS, NUM_VERTICES);
    TestOptionalMatch(g, exe, &mb, &rc, 3, 1, true, false, COLINDEX_NONE, NUM_VERTICES);
    TestOptionalMatch(g, exe, &mb, &rc, 3, 1, true, false, 2, NUM_VERTICES);

    delete exe;
}

TEST(test_optional_match, has_compress_input) {
    TestTool::prepareGraph(optional_graphdir, NUM_VERTICES, NUM_EDGES, 3);
    Graph g(TestTool::getTestGraphDir(optional_graphdir));
    CHECK(g.load()) << "Graph load failed!";

    Config* config = Config::GetInstance();
    Mailbox mb(config->num_threads_, config->num_mailbox_threads_);
    ResultCollector rc;
    Nodes nodes = Nodes::CreateStandaloneNodes();
    StandaloneExecutor* exe = new StandaloneExecutor(nodes, g, mb, rc, config->num_threads_);
    exe->start();

    // Input contains compressCol and will have compressInput
    // i.e., use vertices inside compressed column as the starting node a
    TestOptionalMatch(g, exe, &mb, &rc, 3, 1, true, true, COLINDEX_COMPRESS, NUM_VERTICES);
    TestOptionalMatch(g, exe, &mb, &rc, 3, 1, true, true, COLINDEX_NONE, NUM_VERTICES);
    TestOptionalMatch(g, exe, &mb, &rc, 3, 1, true, true, 2, NUM_VERTICES);

    delete exe;
}
}  // namespace testOptionalMatch
}  // namespace AGE
