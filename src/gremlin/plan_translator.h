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

#pragma once

#include <common.pb.h>
#include <google/protobuf/util/json_util.h>
#include <gremlin.pb.h>
#include <grpcpp/grpcpp.h>
#include <job_service.grpc.pb.h>
#include <cstdio>
#include <iostream>
#include <map>
#include <queue>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>
#include "base/expression.h"
#include "execution/physical_plan.h"
#include "gremlin/filter_translator.h"
#include "gremlin/op_translator.h"
#include "gremlin/plan_checker.h"
#include "gremlin/step_translator.h"
#include "operator/ops.h"
#include "operator/vertex_scan_op.h"
#include "plan/logical_plan.h"
#include "plan/planner.h"
#include "storage/layout.h"

using std::pair;
using std::priority_queue;
using std::unordered_set;
using std::vector;

namespace AGE {

class PlanTranslator {
   public:
    PlanTranslator(const StrMap *strMap, Graph *g)
        : g(g), strMap(strMap), opTranslator(strMap), filterTranslator(strMap) {}

    PhysicalPlan *translate(const protocol::JobRequest *jobReq) {
        int qid = jobReq->conf().job_id();
        const protocol::Source &source = jobReq->source();
        const protocol::TaskPlan &taskPlan = jobReq->plan();
        const protocol::Sink &sink = jobReq->sink();
        printf("building logical plan\n");
        LogicalPlan *logicalPlan = buildLogicalPlan(source, taskPlan, sink, qid);
        printf("LogicalPlan: %s\n", logicalPlan->DebugString().c_str());

        if (!planChecker.check(*logicalPlan)) return nullptr;

        printf("building execution plan\n");
        PhysicalPlan *physicalPlan = BuildPhysicalPlan(logicalPlan, qid);
        printf("PhysicalPlan: %s\n", physicalPlan->DebugString().c_str());
        return physicalPlan;
    }

   private:
    // Replace current column with alias.
    // If alias in current column is still useful then, assgin a col to it.
    void assignCol(int p, int alias, int &cur, int &maxColNum, const map<int, int> &lastUsed,
                   priority_queue<int> idleCols, map<int, int> &tag2col) {
        // printf("assign(%d, %d %d)\n", p, alias, cur);
        int prevCur = cur;
        cur = alias;
        if (prevCur != Planner::CUR_COL_NOT_EXIST)
            printf("prevCur: %d, lastUsed: %d, p: %d\n", prevCur, lastUsed.at(prevCur), p);
        if (prevCur == Planner::CUR_COL_NOT_EXIST || p >= lastUsed.at(prevCur)) return;
        if (idleCols.empty()) idleCols.push(maxColNum++);
        // assert(!alias2col.count(prevCur));
        tag2col[prevCur] = idleCols.top();
        idleCols.pop();
    }

    // Return maxColNum.
    int buildTagColMap(const LogicalPlan &logicalPlan, map<int, int> &tag2col, vector<int> &curTags) {
        map<int, int> lastUsed;  // alias --> Last used position for this alias.
        priority_queue<int> idleCols;
        int cur = Planner::CUR_COL_NOT_EXIST;
        curTags.emplace_back(cur);

        // Get alias last used position.
        // int lastOutput = 0;
        for (u32 i = 0; i < logicalPlan.size(); i++)
            for (int alias : logicalPlan[i].input) {
                lastUsed[alias] = i;
            }
        // for (auto[x, y] : lastUsed) printf("lastUsed[%d]: %d\n", x, y);

        // Assign column.
        int maxColNum = 0;

        // Firstly consider output from last phase.
        printf("maxColNum: %d\n", maxColNum);

        for (int i = 0; i < static_cast<i64>(logicalPlan.size()) - 1l; i++) {
            /*
            printf("maxColNum: %d\n", maxColNum);
            printf("LogicalPlan[%u]: %s\n", i, plan[i].DebugString(aliasMap).c_str());

            printf("bfore alias2col: ");
            printAlias2col();
            */

            for (int alias : logicalPlan[i].input)
                if (i == lastUsed[alias] && alias != cur) {
                    idleCols.push(tag2col.at(alias));
                }

            for (int alias : logicalPlan[i].output)
                if (lastUsed.count(alias)) {
                    // Assign a column for useful alias.
                    assignCol(i, alias, cur, maxColNum, lastUsed, idleCols, tag2col);
                }

            curTags.emplace_back(cur);

            /*
            printf("after alias2col: ");
            printAlias2col();

            printf("curAlias: %d\n", cur);
            */
        }

        curTags.emplace_back(cur);
        printf("maxColNum: %d\n", maxColNum);
        return maxColNum;
    }

    PhysicalPlan *BuildPhysicalPlan(LogicalPlan *logicalPlan, int qid) {
        map<int, int> tag2col;
        vector<int> curTags;
        printf("before buildTagColMap\n");
        int colNum = buildTagColMap(*logicalPlan, tag2col, curTags);

        printf("tag2col: %lu, colNum: %d\n", tag2col.size(), colNum);
        for (auto [k, v] : tag2col) {
            printf("%d, %d\n", k, v);
        }

        printf("curTags: ");
        for (int x : curTags) printf("%d ", x);
        printf("\n");

        printf("before materialize\n");
        PhysicalPlan *plan = materializePlan(logicalPlan, tag2col, curTags, qid, colNum);
        return plan;
    }

    PhysicalPlan *materializePlan(LogicalPlan *logicalPlan, map<int, int> &tag2col, vector<int> &curTags, int qid,
                                  int colNum) {
        PhysicalPlan *plan = new PhysicalPlan(-qid);
        printf("plan size: %lu\n", logicalPlan->size());
        for (u32 i = 0; i < logicalPlan->size(); i++) {
            printf("materialize: %u\n", i);
            AbstractOp *op =
                materializeOp(logicalPlan->ops[i], logicalPlan->params[i], tag2col, curTags[i], curTags[i + 1], colNum);
            printf("op: %p\n", op);
            plan->appendOp(op);
        }
        plan->appendOp(new EndOp(0));
        return plan;
    }

    AbstractOp *materializeOp(const LogicalOp &logicalOp, void *param, map<int, int> &tag2col, int inCur, int outCur,
                              int colNum) {
#define _COL(CUR, col) ((col) == CUR ? COLINDEX_COMPRESS : tag2col.at(col))
        OpType type = logicalOp.type;
        printf("materializeOp(%s)\n", OpTypeStr[type]);
        AbstractOp *op = nullptr;
        if (type == OpType_INIT) {
            op = new InitOp();
        } else if (type == OpType_EXPAND) {
            printf("mat expand\n");
            ExpandOp::Params *par = reinterpret_cast<ExpandOp::Params *>(param);
            printf("inCur: %d, outCur: %d, src: %d, dst: %d\n", inCur, outCur, par->src, par->dst);
            ColIndex src = _COL(inCur, par->src), dst = _COL(outCur, par->dst), cur = COLINDEX_COMPRESS;
            printf("p2\n");
            if (inCur != outCur) {
                cur = tag2col.count(inCur) ? tag2col.at(inCur) : COLINDEX_NONE;
            }

            op = createExpandOp(src, dst, cur, par->eLabel, par->dstVLabel, par->dir, colNum);
            delete par;
            printf("mat expand done\n");
        } else if (type == OpType_VERTEX_SCAN) {
            printf("vertex scan\n");
            VertexScanOp::Params *par = reinterpret_cast<VertexScanOp::Params *>(param);
            printf("par: %p\n", par);
            printf("should constructor: %d, %d, %d\n", par->dstColIdx, inCur, outCur);
            op = new VertexScanOp(par->label, _COL(outCur, par->dstColIdx), colNum);
            printf("vertex scan op: %p\n", op);
            delete par;
        } else if (type == OpType_FILTER) {
            Expression *exp = reinterpret_cast<Expression *>(param);
            // TODO(ycli): apply ColAssigner in gremlin
            // ExprUtil::mapVar(exp, tag2col, inCur);
            op = new FilterOp(exp, colNum);
        } else if (type == OpType_PROJECT) {
        } else if (type == OpType_COUNT) {
            vector<pair<Expression *, ColIndex>> exps;
            exps.emplace_back(new Expression(EXP_AGGFUNC, AGG_CNT), 0);
            int newColNum = 1;
            op = new CountOp(exps, 0, colNum, 0, newColNum, inCur != Planner::CUR_COL_NOT_EXIST);
        }
        return op;
    }

    AbstractOp *createExpandOp(ColIndex srcCol, ColIndex dstCol, ColIndex curCol, LabelId eLabel, LabelId dstVLabel,
                               DirectionType dir, int colNum) {
        if (dstCol == COLINDEX_NONE) return new ExpandULOp(srcCol, dstCol, curCol, eLabel, dstVLabel, dir, colNum);
        if (srcCol == COLINDEX_COMPRESS && dstCol == COLINDEX_COMPRESS)
            return new ExpandCCOp(srcCol, dstCol, curCol, eLabel, dstVLabel, dir, colNum);
        else if (srcCol == COLINDEX_COMPRESS)
            return new ExpandCNOp(srcCol, dstCol, curCol, eLabel, dstVLabel, dir, colNum);
        else if (dstCol == COLINDEX_COMPRESS)
            return new ExpandNCOp(srcCol, dstCol, curCol, eLabel, dstVLabel, dir, colNum);
        else
            return new ExpandNNOp(srcCol, dstCol, curCol, eLabel, dstVLabel, dir, colNum);
    }

    LogicalPlan *buildLogicalPlan(const protocol::Source &source, const protocol::TaskPlan &taskPlan,
                                  const protocol::Sink &sink, int qid) {
        LogicalPlan *logicalPlan = new LogicalPlan();
        logicalPlan->appendOp(OpType_INIT);

        printf("translating source\n");
        // Translate source.
        int curTag = translateSource(logicalPlan, source);

        printf("translating task plan\n");
        // Translate taskPlan.
        curTag = translateTaskPlan(logicalPlan, curTag, taskPlan);

        // Translate sink.
        translateSink(logicalPlan, curTag, sink);
        return logicalPlan;
    }

    void translateSink(LogicalPlan *plan, int curTag, const protocol::Sink &sink) {
        protocol::OperatorDef op;
        if (sink.has_fold()) {
            op.mutable_fold()->CopyFrom(sink.fold());
        } else if (sink.has_group()) {
            op.mutable_group()->CopyFrom(sink.group());
        }
        opTranslator.translateOpDef(plan, curTag, op);

        if (!endWithProj(plan)) {
            plan->appendOp(OpType_PROJECT, reinterpret_cast<void *>(curTag));
            plan->ops.back().input.insert(curTag);
        }
    }

    int translateSource(LogicalPlan *plan, const protocol::Source &source) {
        gremlin::GremlinStep gremlinStep;
        gremlinStep.ParseFromString(source.resource());

        // tags.
        // Currently don't find the meaning of multiple tags.
        // Only use first tag.
        int outTag = gremlinStep.tags_size() == 0 ? -(plan->size() + 1) : gremlinStep.tags(0).tag();

        assert(gremlinStep.has_graph_step());
        const gremlin::GraphStep graphStep = gremlinStep.graph_step();

        printf("p1\n");
        // TODO(ycli): support ids.
        if (graphStep.ids_size() > 0) printf("[Not support] begin by ids ignored\n");

        // TODO(ycli): support g.E().
        if (graphStep.return_type() == gremlin::EDGE) printf("[Not support] begin by edge\n");

        // TODO(ycli): support traverse_requirements.
        graphStep.traverser_requirements_size();

        printf("p2\n");
        const gremlin::QueryParams &qParams = graphStep.query_params();

        // Label.
        int label = ALL_LABEL;
        printf("qParams.has_labels(): %d\n", qParams.has_labels());
        if (qParams.has_labels() && qParams.labels().labels_size() > 0) {
            // TODO(ycli): support multi-label.
            printf("size: %d\n", qParams.labels().labels_size());
            if (qParams.labels().labels_size() > 1) printf("[Not support] multi-label graphStep, pick first label");
            label = qParams.labels().labels(0);
        } else {
            label = ALL_LABEL;
        }

        printf("p3\n");
        // Append op.
        VertexScanOp::Params *par = new VertexScanOp::Params(label, outTag);
        plan->appendOp(OpType_VERTEX_SCAN, reinterpret_cast<void *>(par));
        plan->ops.back().output.insert(outTag);

        // Ignored.
        if (qParams.has_required_properties()) {
            const gremlin::PropKeys &pKeys = qParams.required_properties();
        }

        // TODO(ycli): support limit.
        if (qParams.has_limit()) {
            int limit = qParams.limit().limit();
        }

        // predicates.
        if (qParams.has_predicates()) {
            const gremlin::FilterChain filterChain = qParams.predicates();
            Expression *filter = filterTranslator.translateFilterChain(filterChain, outTag);
            printf("filter: %p\n", filter);
            if (filter != nullptr) {
                plan->appendOp(OpType_FILTER, reinterpret_cast<void *>(filter));
                plan->ops.back().input.insert(outTag);
            }
        }

        // Ignored.
        // if (qParams.has_extra_params()) {
        // }

        return outTag;
    }

    bool endWithProj(LogicalPlan *plan) {
        if (!plan->ops.size()) return false;
        OpType type = plan->ops.back().type;

        return type == OpType_AGGREGATE || type == OpType_COUNT || type == OpType_PROJECT;
    }

    int translateTaskPlan(LogicalPlan *plan, int curTag, const protocol::TaskPlan &taskPlan) {
        int size = taskPlan.plan_size();
        printf("size: %d\n", size);
        for (int i = 0; i < size; i++) {
            printf("operatorDef %d\n", i);
            const protocol::OperatorDef &opDef = taskPlan.plan(i);
            curTag = opTranslator.translateOpDef(plan, curTag, opDef);
        }

        return curTag;
        /*
        if (!endWithProj(plan)) {
            plan->appendOp(OpType_PROJECT, reinterpret_cast<void*>(curTag));
            plan->ops.back().input.insert(curTag);
        }
        */
    }

    Graph *g;
    PlanChecker planChecker;
    const StrMap *strMap;
    OpTranslator opTranslator;
    FilterTranslator filterTranslator;
};

}  // namespace AGE
