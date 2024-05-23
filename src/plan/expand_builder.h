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
#include <climits>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include "base/expression.h"
#include "base/intervals.h"
#include "plan/pattern_graph.h"
#include "plan/plan_graph.h"
#include "plan/type.h"
#include "plan/var_prop_intervals.h"

using std::map;
using std::pair;
using std::priority_queue;
using std::string;
using std::to_string;
using std::vector;

namespace AGE {

constexpr int kFilterWeight = 1;

struct ExpandOrder {
    VarId src;
    vector<VarId> expand_edge_order;
    map<VarId, PropId> usedPropIndex;
    string DebugString() const {
        string ret = "ExpandOrder: [" + std::to_string(src);
        for (VarId varId : expand_edge_order) {
            ret += ", " + std::to_string(varId);
        }
        ret += "]";
        return ret;
    }
};

class ExpandBuilder {
   public:
    ExpandBuilder(const PlanGraph::Vertex &planVtx, const VarPropIntervals &indexableVarPropIntervals,
                  const set<VarId> &inputVars, const VarMap &varMap)
        : planVtx(planVtx),
          indexableVarPropIntervals(indexableVarPropIntervals),
          inputVars(inputVars),
          varMap(varMap) {}

    ExpandOrder Build() {
        ExpandOrder order;

        PreprocessData ppData = Preprocess();

        BuildExpandData buildExpandData;
        buildExpandData.vars = inputVars;
        order.src = SelectSource(buildExpandData, ppData);
        if (order.src == kVarIdNone) return order;
        // Record index scan
        if (buildExpandData.vars.count(order.src) == 0 && indexableVarPropIntervals.count(order.src) != 0)
            order.usedPropIndex.emplace(order.src, SelectPropIndex(order.src));

        // ExpandTuple::priority describe the priority of the edge to expand
        // The higher the priority, the earlier it is expanded
        struct ExpandTuple {
            ScoreType priority;
            VarId edge, dstVtx;
            bool operator<(const ExpandTuple &rhs) const { return priority < rhs.priority; }
            ExpandTuple(ScoreType priority, VarId edge, VarId dstVtx)
                : priority(priority), edge(edge), dstVtx(dstVtx) {}
            string DebugString() const {
                return "{" + to_string(priority) + ", " + to_string(edge) + ", " + to_string(dstVtx) + "}";
            };
        };

        // Prepare initial source of expand
        priority_queue<ExpandTuple> expandQueue;
        expandQueue.emplace(UINT_MAX, kVarIdNone, order.src);
        for (VarId vtxId : buildExpandData.vars) {
            if (vtxId == order.src) continue;
            expandQueue.emplace(ppData.filterStrength[vtxId], kVarIdNone, vtxId);
        }

        while (!expandQueue.empty()) {
            // Get highest score ExpandTule
            auto [curScore, edge, curVtx] = expandQueue.top();
            expandQueue.pop();

            if (buildExpandData.vars.count(edge) != 0) continue;

            // Record this expand
            if (edge != kVarIdNone) {
                // For input vtx or source, edge == kVarIdNone, no need to record edge
                order.expand_edge_order.push_back(edge);
                buildExpandData.vars.insert(edge);
            }
            buildExpandData.vars.insert(curVtx);

            // LOG(INFO) << "curVtx: " << curVtx << ", edge: " << edge << ", score: " << curScore;

            // Sometimes inputs variables will not appear in patternGraph in some situation:
            //  1. Input variables is not pattern vertex, e.g.:
            //      Variable "e" in "MATCH (n)-[e]-(m) WITH n, e MATCH (n)--(q) RETURN q"
            //  2. Input variables is pattern vertex but no need to expand, e.g.:
            //      Variable "n" in "MATCH (n)--(m) WITH n, m MATCH (m)--(q) RETURN q"
            if (planVtx.patternGraph.V.count(curVtx) == 0) continue;

            // Expand
            const PatternGraph::Vertex &patternVtx = planVtx.patternGraph.V.at(curVtx);

            for (VarId candidateEdgeId : patternVtx.edges) {
                if (buildExpandData.vars.count(candidateEdgeId) != 0) continue;
                // LOG(INFO) << "candidateEdgeId: " << candidateEdgeId;
                const PatternGraph::Edge &patternEdge = planVtx.patternGraph.E.at(candidateEdgeId);
                VarId nxtVtxId = patternEdge.src ^ patternEdge.dst ^ curVtx;

                // TODO(ycli): consider pattern edge filterStrength score after we
                //  support pattern edge variable expr.

                // Prioritize expand to existing vtx
                ScoreType score = (buildExpandData.vars.count(nxtVtxId)) * 1000 + ppData.filterStrength[nxtVtxId];

                expandQueue.emplace(score, candidateEdgeId, nxtVtxId);
            }
        }

        return order;
    }

   private:
    typedef unsigned int ScoreType;

    // FilterStrength represents the strength of filters on a variable.
    // For a given variable, the higher the filter strength, the less amount it is estimated
    typedef map<VarId, ScoreType> FilterStrengthMap;

    // Data generated by pre-build process
    struct PreprocessData {
        FilterStrengthMap filterStrength;
    };

    // Data for building expand
    struct BuildExpandData {
        // Existed vars
        set<VarId> vars;
    };

    PreprocessData Preprocess() {
        PreprocessData ppData;

        // Calc variable filterStrength
        ppData.filterStrength = EstimateFilterStrength();
        return ppData;
    }

    FilterStrengthMap EstimateFilterStrength() {
        FilterStrengthMap filterStrengthMap;

        // Initialize
        for (VarId var : planVtx.vars) {
            filterStrengthMap[var] = 0;
        }

        // Vtx label and connectivity
        for (const auto &[id, patternVtx] : planVtx.patternGraph.V) {
            if (patternVtx.label != ALL_LABEL) filterStrengthMap[id] += 1;
            filterStrengthMap[id] += patternVtx.edges.size();
        }

        // Edge label
        for (const auto &[id, patternEdge] : planVtx.patternGraph.E) {
            if (patternEdge.label != ALL_LABEL) filterStrengthMap[id] += 1;
        }

        // Filter
        for (const Expression *const var : ExprUtil::GetTypedExpr(EXP_VARIABLE, planVtx.filters)) {
            filterStrengthMap[GetObjectId(var->varId)] += kFilterWeight;
        }
        for (const Expression *const var : ExprUtil::GetTypedExpr(EXP_EQ, planVtx.filters)) {
            for (const Expression& child : var->GetChildren()) {
                if (child.type == EXP_VARIABLE) {
                    filterStrengthMap[GetObjectId(child.varId)] += 10 * kFilterWeight;
                }
            }
        }

        return filterStrengthMap;
    }

    PropId SelectPropIndex(VarId var) {
        assert(indexableVarPropIntervals.count(var) != 0);
        // TODO(ycli): more complex procedure to choose indexed prop
        return indexableVarPropIntervals.at(var).begin()->first;
    }

    VarId SelectSource(const BuildExpandData &buildExpandData, const PreprocessData &ppData) {
        vector<VarId> candidateSrc = GetCandidateSource(buildExpandData);

        // Sort candidate by filterStrength
        sort(candidateSrc.begin(), candidateSrc.end(),
             [&ppData](VarId lhs, VarId rhs) { return ppData.filterStrength.at(lhs) > ppData.filterStrength.at(rhs); });

        return candidateSrc.size() ? candidateSrc[0] : kVarIdNone;
    }

    vector<VarId> GetCandidateSource(const BuildExpandData &buildExpandData) {
        vector<VarId> ret;

        // Firstly we consider start from existing pattern vertex
        for (auto &[var, vertex] : planVtx.patternGraph.V) {
            if (buildExpandData.vars.count(var) == 0) continue;
            ret.push_back(var);
        }

        // Secondly we consider start from index scan
        if (!ret.empty()) return ret;
        for (auto &[var, _] : indexableVarPropIntervals) {
            ret.push_back(var);
        }

        // Lastly we consider start from non-existing pattern vertex by vertex scan
        if (!ret.empty()) return ret;
        for (auto &[var, vertex] : planVtx.patternGraph.V) {
            ret.push_back(var);
        }

        return ret;
    }

    inline VarId GetObjectId(VarId prop_var_id) { return varMap.ParsePropVarId(prop_var_id).first; }

    const PlanGraph::Vertex &planVtx;
    const VarPropIntervals &indexableVarPropIntervals;
    set<VarId> inputVars;
    const VarMap &varMap;
};

}  // namespace AGE
