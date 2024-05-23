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
// limitations under the License.

#pragma once

#include <glog/logging.h>
#include <algorithm>
#include <stack>
#include <utility>
#include "base/expr_util.h"
#include "base/row.h"
#include "base/serializer.h"
#include "base/type.h"
#include "execution/physical_plan.h"
#include "operator/abstract_op.h"
#include "operator/op_type.h"
#include "operator/ops.h"
#include "plan/ast_helper.h"
#include "plan/col_assigner.h"
#include "plan/logical_plan.h"
#include "plan/op_params.h"
#include "plan/type.h"
#include "storage/str_map.h"

using std::stack;

namespace AGE {
class PhysicPlanBuilder {
   public:
    PhysicPlanBuilder(const VarMap &varMap, const vector<string> &returnStrs, QueryId qid)
        : qid(qid), varMap(varMap), returnStrs(returnStrs) {}

    PhysicalPlan *Build(LogicalPlan &logicalPlan) {
        // Build Segments(i.e. Stages): One segment is a stage
        globalData.headSeg = BuildSegment(logicalPlan.ops.front(), nullptr, InputMap(), globalData);

        PhysicalPlan *physicalPlan = new PhysicalPlan(qid);
        BuildStages(physicalPlan, globalData.headSeg, globalData);

        // LOG(INFO) << physicalPlan->DebugString() << std::flush;

        delete globalData.headSeg;
        return physicalPlan;
    }

   private:
    // InputMap handles the variable transfer between two segments
    using InputMap = map<VarId, pair<ColIndex, bool>>;
    // Divide whole plan into Segments by PROJECT / BRANCH / LOOP ...
    class Segment {
       public:
        ColAssigner *colAssigner;
        LogicalOp *beg, *end;
        map<pair<const LogicalOp *, const LogicalOp *>, Segment *> out;
        vector<LogicalOp *> skeleton;

        Segment(LogicalOp *beg, LogicalOp *end, const InputMap &input, const VarMap &varMap) : beg(beg), end(end) {
            colAssigner = new ColAssigner(beg, end, input);
            // LOG(INFO) << "CompressBeginIdx: " << colAssigner->GetCompressBeginIdx();
            // LOG(INFO) << colAssigner->GenerateDiscardString();
        }
        ~Segment() {
            delete colAssigner;
            // Assume segment graph structure is a tree
            for (auto [p, outSeg] : out) delete outSeg;
        }
        Segment *GetNextSegment(const LogicalOp *l, const LogicalOp *r) const {
            return out.find(std::make_pair(l, r))->second;
        }
    };

    struct GlobalData {
        // Used in FindSegmentRange. Check the end of a subquery
        set<LogicalOp *> vis;
        // The root of the op segment tree
        Segment *headSeg = nullptr;
        // Used for building subqueries. Similar to logicalPlan's subOpStk
        stack<int> subStageStack;
        stack<AbstractOp *> subEntryOps;
        int prevStageIdx = -1;

        int GetPrevOpPos() const { return prevStageIdx; }
        int GetCurrentSubStageIdx() const {
            if (!subStageStack.size()) return -1;
            return subStageStack.top();
        }
        int ResetToSubStage(PhysicalPlan *plan) {
            CHECK(subStageStack.size());
            plan->ConnectStages(prevStageIdx, subStageStack.top());
            return prevStageIdx = subStageStack.top();
        }
        bool StackEmpty() const { return subStageStack.empty(); }
        void PushStage(int idx) { subStageStack.push(idx); }
        void PopStage() {
            CHECK(subStageStack.size() && prevStageIdx == subStageStack.top());
            subStageStack.pop();
        }
        void PushSubEntryOp(AbstractOp *op) { subEntryOps.push(op); }
        AbstractOp *GetSubEntryOp() const { return subEntryOps.top(); }
        void PopSubEntryOp() { subEntryOps.pop(); }
    };

    // nxt: identify the subquery branches so that we can extract input vars
    InputMap BuildInputMap(const LogicalOp *pre, const LogicalOp *nxt);

    // Scan until: 1. cur is a projection (then end is beg of the next segment)
    //             2. cur is endOp
    //             3. end is a visited subqueryEntry (that must have been visited)
    LogicalOp *FindSegmentRange(LogicalOp *beg, const GlobalData &globalData);
    Segment *BuildSegment(LogicalOp *beg, LogicalOp *end, const InputMap &input, GlobalData &globalData);

    AbstractOp *CreatePhysicalOp(const LogicalOp &logicalOp, ColAssigner &colAssigner);
    AbstractOp *BuildPhysicalProject(LogicalOp &curOp, const Segment *curSeg, const vector<string> &returnStrs);
    void CompletePhysicalSubquery(AbstractOp *subEntryOp, const LogicalOp &curOp, const Segment *curSeg);

    void BuildExecutionStage(PhysicalPlan *physicalPlan, LogicalOp *beg, LogicalOp *end, Segment *curSeg,
                             GlobalData &globalData);
    void BuildStages(PhysicalPlan *physicalPlan, Segment *curSeg, GlobalData &globalData);
    void FindStageSkeleton(Segment *seg, GlobalData &globalData);
    pair<bool, VarId> CheckConnectivity(LogicalOp *op, VarId cur_stage_key);

    QueryId qid;
    GlobalData globalData;
    // For building RETURN statement
    const VarMap &varMap;
    const vector<string> &returnStrs;
};
}  // namespace AGE
