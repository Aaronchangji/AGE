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

#include "base/expression.h"
#include "execution/result_collector.h"
#include "execution/standalone_executor.h"
#include "gtest/gtest.h"
#include "operator/aggregate_op.h"
#include "operator/init_op.h"
#include "operator/ops.h"
#include "operator/vertex_scan_op.h"
#include "test/test_tool.h"
#include "util/tool.h"

using std::string;
using std::vector;

namespace AGE {
const char op_exe_graphdir[] = AGE_TEST_DATA_DIR "/op_exe_data";

Result run_single_plan(StandaloneExecutor* executor, Graph* g, shared_ptr<PhysicalPlan>&& plan, Mailbox* mb,
                       ResultCollector* rc, QueryId qid) {
    Message m = Message::BuildEmptyMessage(qid);
    m.plan = nullptr;
    executor->insertPlan(std::move(plan));
    executor->insertQueryMeta(qid, QueryMeta("", Tool::getTimeNs() / 1000, "", {}));
    mb->send_local(std::move(m));

    Result r = rc->pop();
    executor->erasePlan(qid);
    // LOG(INFO) << r.DebugString();

    return r;
}

void test_all_2_hop_count(StandaloneExecutor* executor, Graph& g, uint32_t maxDataSize, DirectionType dir, Mailbox* mb,
                          ResultCollector* rc, QueryId qid) {
    vector<Item> src = g.getAllVtx(), dst;
    for (Item& v : src) g.getNeighbor(v, 0, dir, dst);
    src.clear();
    for (Item& v : dst) g.getNeighbor(v, 0, dir, src);

    u32 compressBeginIdx = 2, resultCompressBeginIdx = 1;
    shared_ptr<PhysicalPlan> plan = std::make_shared<PhysicalPlan>(PhysicalPlan());
    plan->qid = qid;
    plan->g = &g;

    plan->AppendStage(new ExecutionStage());
    plan->stages.back()->appendOp(new VertexScanOp(0, COLINDEX_COMPRESS, compressBeginIdx));

    plan->stages.back()->appendOp(
        new ExpandCCOp(COLINDEX_COMPRESS, COLINDEX_COMPRESS, 0, ALL_LABEL, ALL_LABEL, dir, compressBeginIdx));

    plan->AppendStage(new ExecutionStage());
    plan->stages.back()->appendOp(
        new ExpandCCOp(COLINDEX_COMPRESS, COLINDEX_COMPRESS, 1, ALL_LABEL, ALL_LABEL, dir, compressBeginIdx));

    vector<pair<Expression, ColIndex>> exps = {make_pair(Expression(EXP_AGGFUNC, AGG_CNT), 0)};
    exps.back().first.EmplaceChild(EXP_VARIABLE, COLINDEX_COMPRESS);

    plan->AppendStage(new ExecutionStage());
    plan->stages.back()->appendOp(new CountOp(exps, compressBeginIdx, 0, resultCompressBeginIdx));

    plan->AppendStage(new ExecutionStage());
    plan->stages.back()->appendOp(new EndOp(1));

    // Get and check result.
    Result r = run_single_plan(executor, &g, std::move(plan), mb, rc, qid);
    EXPECT_EQ(r.data.size(), 1ull);
    EXPECT_EQ(r.data[0].size(), static_cast<size_t>(resultCompressBeginIdx));
    EXPECT_EQ(r.data[0][0], Item(T_INTEGER, static_cast<int64_t>(src.size())));
}

TEST(test_standalone_executor, test_single_plan) {
    TestTool::prepareGraph(op_exe_graphdir, 5, 5, 4);
    Config* config = Config::GetInstance();
    Graph g(TestTool::getTestGraphDir(op_exe_graphdir));
    g.load();

    Nodes nodes = Nodes::CreateStandaloneNodes();

    int num_threads = 2;
    int num_mailbox_threads = 2;
    Mailbox mb(num_threads, num_mailbox_threads);
    config->num_threads_ = num_threads;
    config->num_mailbox_threads_ = num_mailbox_threads;
    ResultCollector rc;
    StandaloneExecutor* exe = new StandaloneExecutor(nodes, g, mb, rc, num_threads);
    exe->start();
    test_all_2_hop_count(exe, g, 4096, DirectionType_IN, &mb, &rc, 1);
    test_all_2_hop_count(exe, g, 4096, DirectionType_IN, &mb, &rc, 2);
    delete exe;
}
}  // namespace AGE
