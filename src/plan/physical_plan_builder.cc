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

#include "plan/physical_plan_builder.h"

namespace AGE {

typename PhysicPlanBuilder::InputMap PhysicPlanBuilder::BuildInputMap(const LogicalOp *pre, const LogicalOp *nxt) {
    InputMap ret;
    if (OpType_IsProject(pre->type)) {
        for (VarId var : pre->params.postVars) {
            if (var == COLINDEX_NONE) continue;
            ret.emplace(var, std::make_pair(static_cast<ColIndex>(ret.size()), false));
        }
    } else if (OpType_IsSubquery(pre->type)) {
        // column schema for subquery:
        // | columns from outside (including LoopExpandSrc) | LoopExpandDst (if any) | columns generated inside |
        for (VarId var : pre->params.preVars) {
            if (var == COLINDEX_NONE) continue;
            CHECK(pre->params.preservedVarSet.count(var) || var == pre->params.LoopExpandSrc);
            ret.emplace(var, std::make_pair(static_cast<ColIndex>(ret.size()), true));
        }
        if (pre->params.LoopExpandDst != kVarIdNone) {
            CHECK(pre->params.preservedVarSet.count(pre->params.LoopExpandDst));
            ret.emplace(pre->params.LoopExpandDst, std::make_pair(static_cast<ColIndex>(ret.size()), true));
        }
        for (VarId var : pre->params.preservedVarSet) {
            if (var == COLINDEX_NONE || ret.count(var)) continue;
            ret.emplace(var, std::make_pair(static_cast<ColIndex>(ret.size()), true));
        }
    }
    // LOG(INFO) << "Input map for Op " << pre->DebugString() << ":";
    // for (const auto &[var, col] : ret) LOG(INFO) << var << " -> " << col.first;
    return ret;
}

LogicalOp *PhysicPlanBuilder::FindSegmentRange(LogicalOp *beg, const PhysicPlanBuilder::GlobalData &globalData) {
    CHECK(beg != nullptr);
    LogicalOp *cur = beg, *end = beg->nextOp[0];
    while (end != nullptr && !globalData.vis.count(end) && !OpType_IsProject(cur->type)) {
        cur = cur->nextOp[0];
        end = end->nextOp[0];
    }
    return end;
}

PhysicPlanBuilder::Segment *PhysicPlanBuilder::BuildSegment(LogicalOp *beg, LogicalOp *end,
                                                            const typename PhysicPlanBuilder::InputMap &input,
                                                            PhysicPlanBuilder::GlobalData &globalData) {
    if (end == nullptr) end = FindSegmentRange(beg, globalData);
    // LOG(INFO) << "BuildSegment(" << beg->DebugString() << ", " << (end == nullptr ? "END" : end->DebugString()) <<
    // ")";

    Segment *curSeg = new Segment(beg, end, input, varMap);
    for (LogicalOp *cur = beg; cur != end; cur = cur->nextOp[0]) {
        // LOG(INFO) << "Scan logical op: " << OpType_DebugString(cur->type);
        globalData.vis.insert(cur);

        // Assume the structure of subquery in LogicalOp like this:
        //  BranchOp X -[subq]-> { some Op A -[next]-> some Op B} -[next]-> BranchOp X
        //                       |<------Subquery Segment------>|
        if (OpType_IsSubquery(cur->type)) {
            CHECK(cur->subqueries.size());
            for (LogicalOp *subBeg : cur->subqueries) {
                InputMap inputMap = BuildInputMap(cur, subBeg);
                Segment *subSeg = BuildSegment(subBeg, nullptr, inputMap, globalData);
                curSeg->out.emplace(std::make_pair(cur, subBeg), subSeg);
            }
        }

        // Projection marks the end of a segment
        if (OpType_IsProject(cur->type)) {
            InputMap inputMap = BuildInputMap(cur, end);
            Segment *subSeg = BuildSegment(end, nullptr, inputMap, globalData);
            curSeg->out.emplace(std::make_pair(cur, end), subSeg);
        }
    }

    return curSeg;
}

AbstractOp *PhysicPlanBuilder::CreatePhysicalOp(const LogicalOp &logicalOp, ColAssigner &colAssigner) {
    CHECK(!OpType_IsProject(logicalOp.type) && !OpType_IsSubquery(logicalOp.type));
    AbstractOp *op = nullptr;
    PhysicalOpParams params = logicalOp.params.BuildPhysicalOpParams(colAssigner);

    // TODO(ycli): Merege all expand_xx_op into one to eliminate this if
    if (logicalOp.type == OpType_EXPAND) {
        op = ExpandOp::FromPhysicalParams(params);
    } else {
        op = Ops::CreateOp(logicalOp.type);
        op->FromPhysicalOpParams(params);
    }
    return op;
}

AbstractOp *PhysicPlanBuilder::BuildPhysicalProject(LogicalOp &curOp, const Segment *curSeg,
                                                    const vector<string> &returnStrs) {
    PhysicalOpParams params;
    Segment *nxtSeg = curSeg->GetNextSegment(&curOp, curOp.nextOp[0]);

    params.compressBeginIdx = curSeg->colAssigner->GetCompressBeginIdx();
    params.params = curOp.params.params;
    // LOG(INFO) << params.compressBeginIdx;

    // Process input project expressions
    for (const Expression &preExpr : curOp.params.preExprs) {
        // LOG(INFO) << preExpr.DebugString();
        for (Expression *const var : ExprUtil::GetTypedExpr(EXP_VARIABLE, preExpr)) {
            VarId varId = var->colIdx;
            ColIndex colIdx = curSeg->colAssigner->GetCol(varId);
            var->colIdx = colIdx;
            CHECK(colIdx != COLINDEX_NONE);
        }
        params.exprs.emplace_back(preExpr);
    }

    // Process output columns for: RETURN or WITH
    vector<VarId> &postVars = curOp.params.postVars;
    u32 newCompressBeginIdx = nxtSeg->colAssigner->GetCompressBeginIdx();
    if (nxtSeg->beg->type == OpType_END) {
        CHECK(newCompressBeginIdx == postVars.size() || newCompressBeginIdx + 1ull == postVars.size());
        bool useCompress = newCompressBeginIdx + 1ull == postVars.size();
        // Find out the column position of the return variables
        for (VarId varId : postVars) {
            u32 idx = 0;
            while (idx < returnStrs.size() && varId != varMap.at(returnStrs[idx])) idx++;
            CHECK(idx < returnStrs.size()) << "Input vairable with vid: " << varId << " not found in return strings";

            if (useCompress && idx + 1ull == returnStrs.size()) {
                params.cols.emplace_back(COLINDEX_COMPRESS);
            } else {
                params.cols.emplace_back(idx);
            }
            // LOG(INFO) << varId << " -> " << params.cols.back();
        }
    } else {
        for (VarId var : postVars) {
            ColIndex col = nxtSeg->colAssigner->GetCol(var);
            params.cols.emplace_back(col);
        }
    }
    Serializer::appendU32(&params.params, newCompressBeginIdx);

    AbstractOp *op = Ops::CreateOp(curOp.type);
    op->FromPhysicalOpParams(params);
    return op;
}

void PhysicPlanBuilder::CompletePhysicalSubquery(AbstractOp *subEntryOp, const LogicalOp &curOp,
                                                 const Segment *curSeg) {
    // CHECK(OpType_IsSubquery(subEntryOp->type) && subEntryOp->nextOp.size() == curOp.subqueries.size()) <<
    // OpTypeStr[subEntryOp->type] << " " << subEntryOp->nextOp.size() << " " << curOp.subqueries.size();
    CHECK(OpType_IsSubquery(subEntryOp->type)) << OpTypeStr[subEntryOp->type];

    // Insert the nextStages into subEntryOp

    PhysicalOpParams params;
    params.compressBeginIdx = curSeg->colAssigner->GetCompressBeginIdx();

    // In the current implementation all subquery branches share the same column schema
    LogicalOp *subOp = curOp.subqueries[0];
    Segment *subSeg = curSeg->GetNextSegment(&curOp, subOp);
    const InputMap &inputMap = subSeg->colAssigner->GetInputMap();

    // inner_compress_begin_index
    Serializer::appendU32(&params.params, inputMap.size());

    // sub_ops
    // Serializer::appendU32(&params.params, subEntryOp->nextOp.size());
    // for (int subOpIdx : subEntryOp->nextOp) Serializer::appendI32(&params.params, subOpIdx);

    // num_branches
    Serializer::appendU32(&params.params, curOp.subqueries.size());

    // input_columns: variables taken from outside the subquery. Get their column index for extraction
    // For loop subquery, mapping from LoopExpandSrc to LoopExpandDst will be placed at the end of input_columns
    Serializer::appendU32(&params.params, curOp.params.preVars.size() + (curOp.type == OpType_LOOP));
    for (VarId var : curOp.params.preVars) {
        CHECK(inputMap.count(var) && curSeg->colAssigner->GetCol(var) != COLINDEX_NONE);
        Serializer::appendI32(&params.params, curSeg->colAssigner->GetCol(var));
    }
    if (curOp.type == OpType_LOOP)
        Serializer::appendI32(&params.params, curSeg->colAssigner->GetCol(curOp.params.LoopExpandSrc));

    // Switch to column schema after processing this subquery op
    // ColIndex preCompress = curSeg->colAssigner->GetCompress(),
    //          compressBeginIdx = curSeg->colAssigner->GetCompressBeginIdx();
    ColIndex compressDst = curSeg->colAssigner->ProcessNextOp();

    // compress_dst
    // LOG(INFO) << preCompress << " in " << compressBeginIdx << " -> " << compressDst;
    Serializer::appendI32(&params.params, compressDst);

    // output_colmuns
    set<VarId> outputSet;
    for (VarId var : curOp.output) {
        if (curSeg->colAssigner->GetCol(var) == COLINDEX_NONE) continue;
        outputSet.insert(var);
    }
    Serializer::appendU16(&params.params, outputSet.size());
    for (VarId var : outputSet) {
        Serializer::appendI32(&params.params, inputMap.at(var).first);
        Serializer::appendI32(&params.params, curSeg->colAssigner->GetCol(var));
    }

    if (subEntryOp->type == OpType_LOOP) {
        size_t pos = 0;
        u8 minLen = Serializer::readU8(curOp.params.params, pos), maxLen = Serializer::readU8(curOp.params.params, pos);
        Serializer::appendU8(&params.params, minLen);
        Serializer::appendU8(&params.params, maxLen);
    } else {
        // Nothing to be done
    }

    subEntryOp->FromPhysicalOpParams(params);
}

void PhysicPlanBuilder::BuildStages(PhysicalPlan *physicalPlan, Segment *curSeg, GlobalData &globalData) {
    if (curSeg->beg->type == OpType_END) {
        // LOG(INFO) << "Build END op" << std::flush;
        ExecutionStage *stage = new ExecutionStage();
        stage->appendOp(new EndOp(curSeg->colAssigner->GetCompressBeginIdx()));
        stage->setExecutionSide();
        physicalPlan->AppendStage(stage, globalData.GetPrevOpPos());
        return;
    }

    LOG_IF(INFO, Config::GetInstance()->verbose_ >= 2) << "Build Stages for Segment " << curSeg->beg->DebugString()
                                                       << " to " << curSeg->end->DebugString() << std::flush;

    // Find the range for each segment first
    // LOG(INFO) << "Find the range for each segment first" << std::flush;
    FindStageSkeleton(curSeg, globalData);

    // Build Stages
    for (size_t i = 0; i < curSeg->skeleton.size(); i++) {
        // LOG(INFO) << "Build Stage from SkeletonOp " << OpTypeStr[curSeg->skeleton[i]->type] << std::flush;
        LogicalOp *cur_op = curSeg->skeleton[i];
        if (i == curSeg->skeleton.size() - 1) {
            BuildExecutionStage(physicalPlan, cur_op, curSeg->end, curSeg, globalData);
        } else {
            BuildExecutionStage(physicalPlan, cur_op, curSeg->skeleton[i + 1], curSeg, globalData);
        }

        if (OpType_IsSubquery(cur_op->type)) {
            // LOG(INFO) << "Build subquery" << std::flush;
            // recursively build stages of subqueries
            globalData.PushStage(globalData.GetPrevOpPos());
            for (LogicalOp *subquery_beg_op : cur_op->subqueries) {
                Segment *nextSeg = curSeg->GetNextSegment(cur_op, subquery_beg_op);
                BuildStages(physicalPlan, nextSeg,
                            globalData);  // TODO: HERE, append first ops of subqueries to SubEntryOp
            }
            globalData.PopStage();

            // Need to fill in subquery infos after all subqueries are generated
            CompletePhysicalSubquery(globalData.GetSubEntryOp(), *cur_op, curSeg);
            globalData.PopSubEntryOp();
            continue;
        }

        if (i == curSeg->skeleton.size() - 1) {
            // last stage, check whether there is next segment
            LogicalOp *last_op = cur_op;
            while (last_op->nextOp[0] != curSeg->end) {
                last_op = last_op->nextOp[0];
            }
            if (curSeg->out.size()) {
                Segment *nextSeg = curSeg->GetNextSegment(last_op, curSeg->end);
                BuildStages(physicalPlan, nextSeg, globalData);
            }
        }
    }

    // Detect the end of a subquery segment, and go back to the entry op
    if (OpType_IsSubquery(curSeg->end->type) && !globalData.StackEmpty()) {
        globalData.ResetToSubStage(physicalPlan);
    }
}

void PhysicPlanBuilder::FindStageSkeleton(Segment *seg, GlobalData &globalData) {
    LogicalOp *cur_op = seg->beg;
    size_t skeleton_insert_location = 0;

    // first op
    ClusterRole stage_role = OpType_GetExecutionSide(seg->beg->type);
    pair<bool, VarId> stage_checker{false, kVarIdNone};
    stage_checker = CheckConnectivity(seg->beg, stage_checker.second);
    seg->skeleton.insert(seg->skeleton.begin() + skeleton_insert_location++, cur_op);
    cur_op = cur_op->nextOp[0];

    bool last_op_is_subentry = false;

    /**
     * Each of following situation results in a new stage
     * 1. Whether the op is right after a subquery
     * 2. Whether the op is the entry of a subquery
     * 3. Whether the op changed the execution side
     * 4. Whether the op has the different stage_key with the previous op
     *
     * These four situations are sequentially evaluated.
     */
    while (cur_op != seg->end) {
        if (last_op_is_subentry) {
            // new stage
            // LOG(INFO) << "After SubqueryEntryOp: " << cur_op->DebugString() << std::flush;
            seg->skeleton.insert(seg->skeleton.begin() + skeleton_insert_location++, cur_op);
            if (OpType_IsSubquery(cur_op->type)) {
                // two subqueries are conjuntive
                last_op_is_subentry = true;
                cur_op = cur_op->nextOp[0];
                continue;
            }
            stage_role = OpType_GetExecutionSide(cur_op->type);
            last_op_is_subentry = false;
            // Update stage key, the pair.first is meaningless in this iteration
            stage_checker = CheckConnectivity(cur_op, stage_checker.second);
            cur_op = cur_op->nextOp[0];
            continue;
        }

        if (OpType_IsSubquery(cur_op->type)) {
            // Subquery Entry must be a new stage
            seg->skeleton.insert(seg->skeleton.begin() + skeleton_insert_location++, cur_op);
            last_op_is_subentry = true;
            cur_op = cur_op->nextOp[0];
            continue;
        }

        ClusterRole role = OpType_GetExecutionSide(cur_op->type);
        if (role != stage_role) {
            // Execution side changed, a new stage
            seg->skeleton.insert(seg->skeleton.begin() + skeleton_insert_location++, cur_op);
            cur_op = cur_op->nextOp[0];
            stage_role = role;
            continue;
        }

        stage_checker = CheckConnectivity(cur_op, stage_checker.second);
        if (!stage_checker.first) {
            // Stage key chagned, a new stage
            seg->skeleton.insert(seg->skeleton.begin() + skeleton_insert_location++, cur_op);
            cur_op = cur_op->nextOp[0];
            continue;
        }

        cur_op = cur_op->nextOp[0];
    }

    LOG_IF(INFO, Config::GetInstance()->verbose_ >= 2) << "Stage Skeleton:" << std::flush;
    for (auto &op : seg->skeleton) {
        LOG_IF(INFO, Config::GetInstance()->verbose_ >= 2) << op->DebugString() << std::flush;
    }
}

// bool hasConnectivity, VarId next_stage_key
pair<bool, VarId> PhysicPlanBuilder::CheckConnectivity(LogicalOp *op, VarId cur_stage_key) {
    pair<bool, VarId> ret;
    set<VarId> *search_set;
    switch (op->type) {
    case OpType_VERTEX_SCAN:
        search_set = &(op->output);
        break;
    case OpType_EXPAND:
    case OpType_EXPAND_NN:
    case OpType_EXPAND_NC:
    case OpType_EXPAND_CN:
    case OpType_EXPAND_CC:
    case OpType_EXPAND_UL:
    case OpType_EXPAND_INTO:
    case OpType_PROPERTY:
        search_set = &(op->input);
        break;
    case OpType_FILTER:
        // need to extract the key
        search_set = new set<VarId>();
        for (auto &ipt : op->input) {
            search_set->insert(varMap.ParsePropVarName(varMap.at(ipt)).first);
        }
        break;
        // no stage key
    case OpType_INIT:
    case OpType_PROJECT:
    case OpType_AGGREGATE:
    case OpType_AGGREGATE_LOCAL:
    case OpType_AGGREGATE_GLOBAL:
    case OpType_END:
    case OpType_COUNT:
    case OpType_BRANCH_AND:
    case OpType_BRANCH_OR:
    case OpType_BRANCH_NOT:
    case OpType_LOOP:
    case OpType_OPTIONAL_MATCH:
    case OpType_BARRIER_PROJECT:
    case OpType_BARRIER_PROJECT_LOCAL:
    case OpType_BARRIER_PROJECT_GLOBAL:
    case OpType_INDEX:
    case OpType_INDEX_SCAN:
        ret = pair<bool, VarId>(true, cur_stage_key);
        return ret;
    default:
        LOG(ERROR) << "Unexpected op type: " << OpTypeStr[op->type];
        CHECK(false);
    }

    // init
    if (cur_stage_key == kVarIdNone) {
        ret = pair<bool, VarId>(true, *(search_set->begin()));
        return ret;
    }

    auto itr = search_set->find(cur_stage_key);
    if (itr != search_set->end()) {
        ret = pair<bool, VarId>(true, cur_stage_key);
    } else {
        ret = pair<bool, VarId>(false, *(search_set->begin()));
    }

    if (op->type == OpType_FILTER) {
        delete search_set;
    }
    return ret;
}

// [beg, end)
void PhysicPlanBuilder::BuildExecutionStage(PhysicalPlan *physicalPlan, LogicalOp *beg, LogicalOp *end, Segment *curSeg,
                                            GlobalData &globalData) {
    // LOG(INFO) << "BuildExecutionStage: " << beg->DebugString() << " to " << end->DebugString() << std::flush;
    ExecutionStage *new_stage = new ExecutionStage();
    LogicalOp *cur_op = beg;
    while (cur_op != end) {
        AbstractOp *phy_op = nullptr;
        if (OpType_IsProject(cur_op->type)) {
            phy_op = BuildPhysicalProject(*cur_op, curSeg, returnStrs);
        } else if (OpType_IsSubquery(cur_op->type)) {
            phy_op = Ops::CreateOp(cur_op->type);
            globalData.PushSubEntryOp(phy_op);
        } else {
            phy_op = CreatePhysicalOp(*cur_op, *curSeg->colAssigner);
        }

        new_stage->appendOp(phy_op);
        cur_op = cur_op->nextOp[0];
    }

    new_stage->setExecutionSide();
    // LOG(INFO) << "new stage: " << new_stage->DebugString(physicalPlan->size()) << std::flush;
    physicalPlan->AppendStage(new_stage, globalData.GetPrevOpPos());
    globalData.prevStageIdx = physicalPlan->size() - 1;
}

}  // namespace AGE
