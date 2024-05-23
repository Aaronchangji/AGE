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
#include "base/row.h"
#include "gtest/gtest.h"

#include "base/expression.h"
#include "base/type.h"
#include "execution/message.h"
#include "execution/result_collector.h"
#include "execution/standalone_executor.h"
#include "operator/op_type.h"
#include "operator/ops.h"
#include "operator/subquery_util.h"
#include "storage/graph.h"
#include "storage/graph_loader.h"
#include "test/test_tool.h"
#include "util/tool.h"

using std::string;
using std::vector;

namespace AGE {
namespace testSubquery {
const char subquery_graphdir[] = AGE_TEST_DATA_DIR "/subquery_op_data";
const u32 NUM_VERTICES = 10000;
const u32 NUM_EDGES = 20000;
const u8 NUM_PROPERTY = 4;
const u8 FILTER_NUM_PROPERTY = 3;
const u8 MAX_NUM_COMPRESS_ITEMS = 5;
constexpr char* filterPropNameList[] = {(char*)"bool", (char*)"int", (char*)"float"};
const Item CONST_BOOL_ITEM = Item(T_BOOL, true);
const Item CONST_INT_ITEM = Item(T_INTEGER, 10);
const Item CONST_FLOAT_ITEM = Item(T_FLOAT, 100.0);

Result RunSinglePlan(StandaloneExecutor* executor, Graph* g, Mailbox* mb, ResultCollector* rc,
                     shared_ptr<PhysicalPlan>&& plan, Message& m) {
    QueryId qid = m.header.qid;
    // size_t inputSize = m.data.size();
    executor->insertPlan(std::move(plan));
    executor->insertQueryMeta(qid, QueryMeta("", Tool::getTimeNs() / 1000, "", {}));

    // u64 ts = Tool::getTimeMs();
    mb->send_local(std::move(m));
    Result r = rc->pop();
    executor->erasePlan(qid);
    // u64 te = Tool::getTimeMs();
    // LOG(INFO) << "Execution time for input size " << inputSize << ": " << te - ts << " ms";

    return r;
}

AbstractOp* CreateSubqueryEntryOp(const vector<int>& subOps, const vector<ColIndex>& inputColumns,
                                  const SubqueryUtil::ColumnMapT& outputColumns, ColIndex compressDst, OpType type,
                                  u32 compressBeginIdx, u32 innerCompressBeginIdx, int passLoopNum = 0,
                                  int endLoopNum = 0) {
    switch (type) {
    case OpType_BRANCH_AND:
        return new BranchFilterEntryOp(type, compressBeginIdx, subOps.size(), inputColumns, compressDst, outputColumns,
                                       innerCompressBeginIdx);
    case OpType_BRANCH_NOT:
        return new BranchFilterEntryOp(type, compressBeginIdx, subOps.size(), inputColumns, compressDst, outputColumns,
                                       innerCompressBeginIdx);
    case OpType_BRANCH_OR:
        return new BranchFilterEntryOp(type, compressBeginIdx, subOps.size(), inputColumns, compressDst, outputColumns,
                                       innerCompressBeginIdx);
    case OpType_LOOP:
        return new LoopOp(compressBeginIdx, inputColumns, compressDst, outputColumns, passLoopNum, endLoopNum,
                          innerCompressBeginIdx);
    default:
        CHECK(false);
    }
}

int InsertSubquery(Graph& g, string propName, int propColIdx, u32 compressBeginIdx, vector<AbstractOp*>& ret) {
    int op_cnt = 0;
    ret.emplace_back(new PropertyOp(0, propColIdx, g.getPropId(propName), -1, compressBeginIdx));
    op_cnt++;

    if (propName == "int") {
        Expression filter(EXP_GT);
        filter.EmplaceChild(EXP_VARIABLE, propColIdx);
        filter.EmplaceChild(EXP_CONSTANT, CONST_INT_ITEM);
        ret.emplace_back(new FilterOp(filter, compressBeginIdx));
    } else if (propName == "float") {
        Expression filter(EXP_LT);
        filter.EmplaceChild(EXP_VARIABLE, propColIdx);
        filter.EmplaceChild(EXP_CONSTANT, CONST_FLOAT_ITEM);
        ret.emplace_back(new FilterOp(filter, compressBeginIdx));
    } else if (propName == "bool") {
        Expression filter(EXP_EQ);
        filter.EmplaceChild(EXP_VARIABLE, propColIdx);
        filter.EmplaceChild(EXP_CONSTANT, CONST_BOOL_ITEM);
        ret.emplace_back(new FilterOp(filter, compressBeginIdx));
    } else {
        LOG(ERROR) << "TEST_SUBQUERY_OP only supports int, float, and bool now";
    }
    op_cnt++;

    return op_cnt;
}

vector<AbstractOp*> CreateSubqueryOps(Graph& g, int numBranches, OpType type, int propColIdx, u32 compressBeginIdx,
                                      vector<int>& subops) {
    vector<AbstractOp*> ret;
    if (OpType_IsBranchFilter(type)) {
        for (int i = 0; i < numBranches; ++i) {
            int num_ops =
                InsertSubquery(g, filterPropNameList[i % FILTER_NUM_PROPERTY], propColIdx, compressBeginIdx, ret);
            if (subops.empty())
                subops.emplace_back(0);
            else
                subops.emplace_back(subops.back() + num_ops);
        }
    } else {
        // For loop
        subops.emplace_back(0);
        ret.emplace_back(new ExpandNNOp(propColIdx, propColIdx, COLINDEX_NONE, ALL_LABEL, ALL_LABEL, DirectionType_BOTH,
                                        compressBeginIdx));
    }
    return ret;
}

bool PassFilter(Graph& g, const Item& src, string propName) {
    Item prop = g.getProp(src, g.getPropId(propName));
    if (propName == "bool") {
        if (prop == CONST_BOOL_ITEM) return true;
    } else if (propName == "int") {
        if (prop > CONST_INT_ITEM) return true;
    } else if (propName == "float") {
        if (prop < CONST_FLOAT_ITEM) return true;
    }

    return false;
}

bool Evaluate(bool lhs, bool rhs, OpType type) {
    switch (type) {
    case OpType_BRANCH_AND:
        return lhs && rhs;
    case OpType_BRANCH_OR:
        return lhs || rhs;
    default:
        return false;
    }
}

bool CheckPass(Graph& g, int numBranches, const Item& itm, OpType type) {
    bool pass = type == OpType_BRANCH_OR ? false : true;
    for (int i = 0; i < numBranches; ++i) {
        if (i % FILTER_NUM_PROPERTY == 0) {
            if (i == 0) {
                pass = (type == OpType_BRANCH_NOT ? !PassFilter(g, itm, "bool") : PassFilter(g, itm, "bool"));
            } else {
                pass = Evaluate(pass, PassFilter(g, itm, "bool"), type);
            }
        } else if (i % FILTER_NUM_PROPERTY == 1) {
            if (i == 0) {
                pass = (type == OpType_BRANCH_NOT ? !PassFilter(g, itm, "int") : PassFilter(g, itm, "int"));
            } else {
                pass = Evaluate(pass, PassFilter(g, itm, "int"), type);
            }
        } else if (i % FILTER_NUM_PROPERTY == 2) {
            if (i == 0) {
                pass = (type == OpType_BRANCH_NOT ? !PassFilter(g, itm, "float") : PassFilter(g, itm, "float"));
            } else {
                pass = Evaluate(pass, PassFilter(g, itm, "float"), type);
            }
        } else {
            continue;
        }
    }
    return pass;
}

void GetAnswerForBranchFilterTest(const vector<Row>& input, vector<Row>& answer, Graph& g, int numBranches,
                                  bool hasCompressInput, int numColumns, ColIndex compressDst, OpType type) {
    vector<Row> tmpAnswer;
    for (auto& r : input) {
        if (!CheckPass(g, numBranches, r[0], type)) continue;
        tmpAnswer.emplace_back(std::move(r));
    }

    // At the end of a subqueryEntryOp compressed column will be moved to compressDst
    // Except when the compressed structure can be preserved
    if (!hasCompressInput && compressDst == COLINDEX_COMPRESS) {
        answer = tmpAnswer;
    } else {
        for (const auto& r : tmpAnswer) r.decompress(answer, numColumns, compressDst);
    }
}

// numColumns: number of normal columns in input data
void TestBranchFilterSubquery(Graph& g, StandaloneExecutor* exe, Mailbox* mb, ResultCollector* rc, OpType type,
                              int numColumns, int numInputColumns, int numBranches, bool hasCompress,
                              bool hasCompressInput, ColIndex compressDst, int numInputs) {
    CHECK_GT(numColumns, 0);
    CHECK_GT(numInputColumns, 0);
    CHECK_GE(numColumns, numInputColumns);
    CHECK(OpType_IsBranchFilter(type));

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
    }

    // prepare subq op parameters
    SubqueryUtil::ColumnMapT outputColumns;
    vector<int> subops;
    u32 compressBeginIdx = numColumns;
    u32 innerCompressBeginIdx = numInputColumns + 1;

    vector<ColIndex> inputColumns;
    for (int i = 0; i < numInputColumns - 1; ++i) {
        inputColumns.emplace_back(i);
    }
    if (hasCompressInput) {
        inputColumns.emplace_back(COLINDEX_COMPRESS);
    } else {
        inputColumns.emplace_back(numInputColumns - 1);
    }

    shared_ptr<PhysicalPlan> plan = std::make_shared<PhysicalPlan>(PhysicalPlan());
    plan->qid = m.header.qid;
    plan->g = &g;
    vector<AbstractOp*> subquery =
        CreateSubqueryOps(g, numBranches, type, numInputColumns, innerCompressBeginIdx, subops);
    AbstractOp* subqueryEntryOp = CreateSubqueryEntryOp(subops, inputColumns, outputColumns, compressDst, type,
                                                        compressBeginIdx, innerCompressBeginIdx);
    plan->stages.emplace_back(new ExecutionStage());
    plan->stages.back()->appendOp(subqueryEntryOp);
    // plan->AppendOp(subqueryEntryOp);

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

    // Get the answer
    vector<Row> answer;
    GetAnswerForBranchFilterTest(m.data, answer, g, numBranches, hasCompressInput, numColumns, compressDst, type);
    sort(answer.begin(), answer.end(), [](const Row& lhs, const Row& rhs) { return lhs[0] < rhs[0]; });

    // Run the query
    Result result = RunSinglePlan(exe, &g, mb, rc, std::move(plan), m);
    sort(result.data.begin(), result.data.end(), [](const Row& lhs, const Row& rhs) { return lhs[0] < rhs[0]; });

    // Check results
    EXPECT_EQ(answer.size(), result.data.size());
    for (size_t i = 0; i < answer.size(); i++) {
        EXPECT_EQ(answer[i][0], result.data[i][0]);
    }
}

TEST(test_subquery, branch_or_test) {
    TestTool::prepareGraph(subquery_graphdir, NUM_VERTICES, NUM_EDGES, 2);
    Graph g(TestTool::getTestGraphDir(subquery_graphdir));
    CHECK(g.load()) << "Graph load failed!";

    Config* config = Config::GetInstance();
    Mailbox mb(config->num_threads_, config->num_mailbox_threads_);
    ResultCollector rc;
    Nodes nodes = Nodes::CreateStandaloneNodes();
    StandaloneExecutor* exe = new StandaloneExecutor(nodes, g, mb, rc, config->num_threads_);
    exe->start();

    TestBranchFilterSubquery(g, exe, &mb, &rc, OpType_BRANCH_OR, 3, 2, 2, false, false, 0, NUM_VERTICES);
    TestBranchFilterSubquery(g, exe, &mb, &rc, OpType_BRANCH_OR, 3, 2, 2, true, false, 0, NUM_VERTICES);
    TestBranchFilterSubquery(g, exe, &mb, &rc, OpType_BRANCH_OR, 3, 2, 2, true, true, 2, NUM_VERTICES);
    TestBranchFilterSubquery(g, exe, &mb, &rc, OpType_BRANCH_OR, 3, 2, 2, true, true, COLINDEX_COMPRESS, NUM_VERTICES);
    TestBranchFilterSubquery(g, exe, &mb, &rc, OpType_BRANCH_OR, 3, 2, 2, true, false, COLINDEX_NONE, NUM_VERTICES);

    delete exe;
}

TEST(test_subquery, branch_and_test) {
    TestTool::prepareGraph(subquery_graphdir, NUM_VERTICES, NUM_EDGES, 2);
    Graph g(TestTool::getTestGraphDir(subquery_graphdir));
    CHECK(g.load()) << "Graph load failed";

    Config* config = Config::GetInstance();
    Mailbox mb(config->num_threads_, config->num_mailbox_threads_);
    ResultCollector rc;
    Nodes nodes = Nodes::CreateStandaloneNodes();
    StandaloneExecutor* exe = new StandaloneExecutor(nodes, g, mb, rc, config->num_threads_);
    exe->start();

    TestBranchFilterSubquery(g, exe, &mb, &rc, OpType_BRANCH_AND, 3, 2, 2, false, false, 0, NUM_VERTICES);
    TestBranchFilterSubquery(g, exe, &mb, &rc, OpType_BRANCH_AND, 3, 2, 2, true, false, 0, NUM_VERTICES);
    TestBranchFilterSubquery(g, exe, &mb, &rc, OpType_BRANCH_AND, 3, 2, 2, true, true, 2, NUM_VERTICES);
    TestBranchFilterSubquery(g, exe, &mb, &rc, OpType_BRANCH_AND, 3, 2, 2, true, true, COLINDEX_COMPRESS, NUM_VERTICES);
    TestBranchFilterSubquery(g, exe, &mb, &rc, OpType_BRANCH_AND, 3, 2, 2, true, false, COLINDEX_NONE, NUM_VERTICES);

    delete exe;
}

TEST(test_subquery, branch_not_test) {
    TestTool::prepareGraph(subquery_graphdir, NUM_VERTICES, NUM_EDGES, 2);
    Graph g(TestTool::getTestGraphDir(subquery_graphdir));
    CHECK(g.load()) << "Graph load failed";

    Config* config = Config::GetInstance();
    Mailbox mb(config->num_threads_, config->num_mailbox_threads_);
    ResultCollector rc;
    Nodes nodes = Nodes::CreateStandaloneNodes();
    StandaloneExecutor* exe = new StandaloneExecutor(nodes, g, mb, rc, config->num_threads_);
    exe->start();

    TestBranchFilterSubquery(g, exe, &mb, &rc, OpType_BRANCH_NOT, 3, 2, 1, false, false, 0, NUM_VERTICES);
    TestBranchFilterSubquery(g, exe, &mb, &rc, OpType_BRANCH_NOT, 3, 2, 1, true, false, 0, NUM_VERTICES);
    TestBranchFilterSubquery(g, exe, &mb, &rc, OpType_BRANCH_NOT, 3, 2, 1, true, true, 2, NUM_VERTICES);
    TestBranchFilterSubquery(g, exe, &mb, &rc, OpType_BRANCH_NOT, 3, 2, 1, true, true, COLINDEX_COMPRESS, NUM_VERTICES);
    TestBranchFilterSubquery(g, exe, &mb, &rc, OpType_BRANCH_NOT, 3, 2, 1, true, false, COLINDEX_NONE, NUM_VERTICES);

    delete exe;
}
}  // namespace testSubquery
}  // namespace AGE
