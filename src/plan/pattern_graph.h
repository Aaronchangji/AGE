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

#include <cypher-parser.h>
#include <glog/logging.h>
#include <limits.h>
#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/expr_util.h"
#include "base/expression.h"
#include "execution/physical_plan.h"
#include "operator/ops.h"
#include "plan/ast.h"
#include "plan/ast_helper.h"
#include "plan/col_assigner.h"
#include "plan/logical_plan.h"
#include "plan/type.h"
#include "util/tool.h"

using std::map;
using std::set;
using std::vector;

namespace AGE {
#define QG_INF_LEN (-1)
#define QG_LOGIC (true)
#define QG_PHYSIC (false)

// This file describe the pattern graph.
// In subgraph matching, we find subgraphs which is equal to (A)-[e]-(B)-[f]-(C) in a real-world graph.
// Then (A)-[e]-(B)-[f]-(C) is called "pattern graph", and that real-world graph often called "data graph".
// So "A", "B", "C" is the pattern vertices in pattern graph
// "e", "f" is pattern edges

// Pattern graph provided by cypher query
class PatternGraph {
   public:
    // Pattern vertex
    class Vertex {
       public:
        LabelId label;
        VarId id;
        vector<VarId> edges;

        Vertex() : label(INVALID_LABEL) {}
        explicit Vertex(VarId id);
        Vertex(const Vertex &rhs);

        bool operator<(const Vertex &rhs) const;
        Vertex &operator=(const Vertex &rhs) = default;
    };

    // Pattern edge
    class Edge {
       public:
        LabelId label;
        VarId id, src, dst;
        int minLen, maxLen;
        bool indirect;

        // len in [minLen, maxLen].
        Edge() : label(INVALID_LABEL), minLen(1), maxLen(1) {}
        explicit Edge(VarId id);
        Edge(const Edge &rhs);

        bool operator<(const Edge &rhs) const;
        Edge &operator=(const Edge &rhs) = default;
    };

    map<VarId, PatternGraph::Vertex> V;
    map<VarId, PatternGraph::Edge> E;

    const VarMap &varMap;

    explicit PatternGraph(const VarMap &varMap) : varMap(varMap) {}
    ~PatternGraph() {}

    // Record V/E in a path into query graph
    void AddPath(const ASTNode *ast_path, const VarMap &varMap, const StrMap &strMap);
    void AddMatchPattern(const ASTNode *ast_match, const VarMap &varMap, const StrMap &strMap);

    static bool IsVarLengthPattern(const PatternGraph::Edge &edge);
    bool HasVarLengthPatterns() const;

   private:
    // Recursively get pattern path from continuous `AND`
    // e.g.      AND
    //     AND        OR
    //  a-b  c-d   e-f  g-h
    // Only build "a-b", "c-d" in this query graph
    // "e-f", "g-h" will be processed by qg of subquery
    vector<const ASTNode *> GetExprPatternPaths(const Expression &expr);
    void GetExprPatternPaths_(const Expression &expr, vector<const ASTNode *> &ret);

    void EnrichVertex(const ASTNode *vtx_n, PatternGraph::Vertex &qv, const StrMap &strMap);
    void EnrichEdge(const ASTNode *edge_n, const ASTNode *src_n, const ASTNode *dst_n, PatternGraph::Edge &patternEdge,
                    const VarMap &varMap, const StrMap &strMap);
};
}  // namespace AGE
