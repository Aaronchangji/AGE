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
#include <map>
#include <queue>
#include <utility>
#include <vector>
#include "base/expr_util.h"
#include "base/expression.h"
#include "base/type.h"
#include "plan/ast_helper.h"
#include "plan/plan_graph.h"
#include "storage/graph.h"

namespace AGE {

using std::map;
using std::pair;
using std::queue;
using std::vector;

class PlanGraphOptimizer {
   public:
    PlanGraphOptimizer() {}

    static void Optimize(PlanGraph &planGraph) {
        EliminateBranchAND(planGraph);
        ConvertPatternFilter(planGraph);
        DivideFilters(planGraph);
    }

    // Divide filters for every PlanGraph::Vertex by eliminating continuous AND from EXP tree root
    //  e.g.:
    //    Original filters:
    //      a.age = b.age AND c.firstName = "Ivan"
    //      c.length < 5 AND (a.length > 2 OR b.age > 20)
    //    Optimized filters:
    //      a.age = b.age
    //      c.firstName = "Jean"
    //      c.length < 5
    //      a.length > 2 OR b.age > 20
    static void DivideFilters(PlanGraph &planGraph) {
        for (PlanGraph::Vertex *curVtx : planGraph.vtxList) {
            vector<Expression> newFilters;
            for (Expression &filter : curVtx->filters) ExprUtil::DivideFilter(filter, newFilters);
            curVtx->filters.swap(newFilters);
        }
    }

    // Convert direct Pattern Filter into PatternGraph
    //  e.g.:
    //    Original PlanVertex: {pattern: {(a)-->(b)-->(c)}, filter: {(a)-->(c), c.age > 5}}
    //    Optimized PlanVertex: {pattern: {(a)-->(b)-->(c), (a)-->(c)}, filter: {c.age > 5}}
    static void ConvertPatternFilter(PlanGraph &planGraph) {
        for (PlanGraph::Vertex *curVtx : planGraph.vtxList) {
            vector<Expression> newFilters;
            for (Expression &filter : curVtx->filters) {
                if (filter.type != EXP_PATTERN_PATH) {
                    newFilters.push_back(filter);
                    continue;
                }

                curVtx->patternGraph.AddPath(reinterpret_cast<const ASTNode *>(filter.GetValue().integerVal),
                                             curVtx->varMap, curVtx->strMap);
                AstHelper::CollectPatternPathInlinedPropFilter(
                    reinterpret_cast<const ASTNode *>(filter.GetValue().integerVal), curVtx->varMap, curVtx->strMap,
                    newFilters);
            }
            curVtx->filters.swap(newFilters);
        }
    }

    // Transform PE_BRANCH_AND to direct filter
    //  e.g.
    //      Original PlanGraph: {
    //          PlanVertex1--[PE_BRANCH_AND]-->PlanVertex2--[PE_SUBQUERY]-->{PlanVertex3, PlanVertex4}},
    //          PlanVertex1 : {pattern: {(a)-->(b)-->(c)-->(d)}, filter: {}},
    //          PlanVertex3 : {pattern: {}, filter: {(a)-->(d)}},
    //          PlanVertex4 : {pattern: {}, filter: {(b)-->(c)}}
    //      }
    //      Optimized PlanGraph: {
    //          PlanVertex1,
    //          PlanVertex1 : {pattern {(a)-->(b)-->(c)-->(d)}, filter: {(a)-->(d), (b)-->(c)}}
    //      }
    //
    static void EliminateBranchAND(PlanGraph &planGraph) { EliminateBranchAND(planGraph, planGraph.source); }
    static void EliminateBranchAND(PlanGraph &planGraph, PlanGraph::Vertex *cur) {
        // Move all PE_BRANCH_AND to end
        queue<PlanGraph::Edge> eliminateQueue;
        for (i64 L = 0, R = cur->out.size() - 1; L <= R; L++) {
            // LOG(INFO) << cur->out.size() << ": " << L << ", " << R << endl;
            if (cur->out[L].type == PlanGraph::PE_BRANCH_AND) std::swap(cur->out[L], cur->out[R]);
            // Move R to the first NON-PE_BRANCH_AND position
            while (L <= R && cur->out[R].type == PlanGraph::PE_BRANCH_AND) {
                eliminateQueue.push(cur->out[R--]);
                CHECK(R + eliminateQueue.size() + 1 == cur->out.size());
            }
        }

        while (!eliminateQueue.empty()) {
            // BRANCH_AND PlanGraph::Vertex should be empty
            PlanGraph::Edge &branchPe = eliminateQueue.front();
            PlanGraph::Vertex *branchV = branchPe.dstV;
            CHECK(branchV->filters.size() == 0 && branchV->patternGraph.V.size() == 0);

            for (PlanGraph::Edge &subqPe : branchV->out) {
                CHECK(subqPe.type == PlanGraph::PE_SUBQUERY);
                PlanGraph::Vertex *subqueryV = subqPe.dstV;

                // Merge filters and variables to the root vertex
                // Merge all (including anonymous) variables since subqueryV will be eliminated
                for (const Expression &filter : subqueryV->filters) cur->filters.emplace_back(filter);
                cur->vars.insert(subqueryV->vars.begin(), subqueryV->vars.end());

                // Subqueries of this subquery
                for (PlanGraph::Edge &outPe : subqueryV->out) {
                    CHECK(PlanGraph::EdgeType_IsSubquery(outPe.type));
                    if (outPe.type == PlanGraph::PE_BRANCH_AND) eliminateQueue.push(outPe);
                    PlanGraph::Vertex *nxtBranchV = outPe.dstV;

                    // Connect curV --> nxtBranchV
                    // Replace source VarId in PlanGraph::Edge::vars()
                    PlanGraph::ConnectTwoVertices(cur, nxtBranchV, outPe.type);
                    PlanGraph::Edge &newPe = cur->out.back();
                    PlanGraph::BuildVarsTransfer(newPe, cur, nxtBranchV, true);
                }

                planGraph.EraseVertex(subqueryV);
            }

            // Here PE_BRANCH_AND is removed from cur->out
            planGraph.EraseVertex(branchV);
            eliminateQueue.pop();
        }

        for (PlanGraph::Edge &pe : cur->out) {
            if (PlanGraph::EdgeType_IsBranch(pe.type)) EliminateBranchAND(planGraph, pe.dstV);
        }
    }
};

}  // namespace AGE
