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

#include "plan/pattern_graph.h"
#include <cypher-parser.h>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <string>
#include "plan/plan_graph.h"
#include "plan/type.h"

namespace AGE {
PatternGraph::Vertex::Vertex(VarId id) : label(ALL_LABEL), id(id) {}
PatternGraph::Vertex::Vertex(const Vertex &rhs) { *this = rhs; }

bool PatternGraph::Vertex::operator<(const PatternGraph::Vertex &rhs) const { return id < rhs.id; }

PatternGraph::Edge::Edge(VarId id) : label(ALL_LABEL), id(id), minLen(1), maxLen(1) {}
PatternGraph::Edge::Edge(const Edge &rhs) { *this = rhs; }

bool PatternGraph::Edge::operator<(const PatternGraph::Edge &rhs) const { return id < rhs.id; }

void PatternGraph::AddMatchPattern(const ASTNode *astMatch, const VarMap &varMap, const StrMap &strMap) {
    assert(cypher_astnode_type(astMatch) == CYPHER_AST_MATCH);

    const ASTNode *astPattern = cypher_ast_match_get_pattern(astMatch);
    u32 path_count = cypher_ast_pattern_npaths(astPattern);
    for (u32 i = 0; i < path_count; i++) {
        AddPath(cypher_ast_pattern_get_path(astPattern, i), varMap, strMap);
    }
}

void PatternGraph::AddPath(const ASTNode *astPath, const VarMap &varMap, const StrMap &strMap) {
    assert(cypher_astnode_type(astPath) == CYPHER_AST_PATTERN_PATH);
    u32 n = cypher_ast_pattern_path_nelements(astPath);
    for (u32 i = 0; i < n; i += 2) {
        const ASTNode *astVtx = cypher_ast_pattern_path_get_element(astPath, i);
        VarName varName = AstHelper::GetVarName(astVtx);
        VarId varId = varMap[varName];

        auto itr = V.find(varId);
        if (itr == V.end()) {
            V.emplace(varId, PatternGraph::Vertex(varId));
            itr = V.find(varId);
        }
        EnrichVertex(astVtx, itr->second, strMap);
    }

    for (u32 i = 1; i < n; i += 2) {
        const ASTNode *src = cypher_ast_pattern_path_get_element(astPath, i - 1),
                      *dst = cypher_ast_pattern_path_get_element(astPath, i + 1),
                      *astRel = cypher_ast_pattern_path_get_element(astPath, i);

        VarName varName = AstHelper::GetVarName(astRel);
        VarId varId = varMap[varName];

        auto [itr, insert] = E.emplace(varId, PatternGraph::Edge(varId));
        if (!insert)
            throw PlannerException("Cannot use the same relationship variable '" + varName + "' for multiple patterns");

        EnrichEdge(astRel, src, dst, itr->second, varMap, strMap);

        V.at(itr->second.src).edges.emplace_back(varId);
        V.at(itr->second.dst).edges.emplace_back(varId);
    }
}

// Recursively get pattern path from continuous `AND`
// e.g.      AND
//     AND        OR
//  a-b  c-d   e-f  g-h
// Only build "a-b", "c-d" in this query graph
// "e-f", "g-h" will be processed by patternGraph of subquery
vector<const ASTNode *> PatternGraph::GetExprPatternPaths(const Expression &expr) {
    vector<const ASTNode *> ret;
    GetExprPatternPaths_(expr, ret);
    return ret;
}
void PatternGraph::GetExprPatternPaths_(const Expression &expr, vector<const ASTNode *> &ret) {
    if (expr.type == EXP_AND) {
        for (const Expression &child : expr.GetChildren()) {
            GetExprPatternPaths_(child, ret);
        }
    } else if (expr.type == EXP_PATTERN_PATH) {
        // TODO(ycli): try to combine EXP_PATTERN_PATH creation & use
        const ASTNode *astPatternPath = reinterpret_cast<const ASTNode *>(expr.GetValue().integerVal);
        ret.push_back(astPatternPath);
    }
}

bool PatternGraph::IsVarLengthPattern(const PatternGraph::Edge &edge) {
    return (edge.minLen != 1 || edge.maxLen != 1) ? true : false;
}

bool PatternGraph::HasVarLengthPatterns() const {
    for (auto &[varId, edge] : E)
        if (IsVarLengthPattern(edge)) return true;
    return false;
}

void PatternGraph::EnrichVertex(const ASTNode *vtx_n, PatternGraph::Vertex &vertex, const StrMap &strMap) {
    // Label.
    u32 label_count = cypher_ast_node_pattern_nlabels(vtx_n);
    if (label_count > 1) throw PlannerException("Not supported: multiple labels on single variable");

    if (label_count) {
        string label_str = cypher_ast_label_get_name(cypher_ast_node_pattern_get_label(vtx_n, 0));
        LabelId labelId = strMap.GetLabelId(label_str);

        if (vertex.label != ALL_LABEL && vertex.label != labelId)
            vertex.label = INVALID_LABEL;
        else
            vertex.label = labelId;
    }
}

void PatternGraph::EnrichEdge(const ASTNode *edge_n, const ASTNode *src_n, const ASTNode *dst_n,
                              PatternGraph::Edge &edge, const VarMap &varMap, const StrMap &strMap) {
    // Note that BuildPatternGraph() ensure that edge alias only appear once in pattern.
    edge.src = varMap[AstHelper::GetVarName(src_n)];
    edge.dst = varMap[AstHelper::GetVarName(dst_n)];

    // Direction.
    cypher_rel_direction dir = cypher_ast_rel_pattern_get_direction(edge_n);
    if (dir == CYPHER_REL_INBOUND) std::swap(edge.src, edge.dst);
    edge.indirect = (dir == CYPHER_REL_BIDIRECTIONAL);

    // Label.
    u32 label_count = cypher_ast_rel_pattern_nreltypes(edge_n);
    if (label_count > 1) throw PlannerException("Not supported: multiple labels on single variable");
    if (label_count) {
        string label_str = cypher_ast_reltype_get_name(cypher_ast_rel_pattern_get_reltype(edge_n, 0));

        // Notice every pattern edge only appear once
        edge.label = strMap.GetLabelId(label_str);
    }

    // Varaible length
    const cypher_astnode_t *varLength = cypher_ast_rel_pattern_get_varlength(edge_n);
    if (varLength != NULL) {
        const cypher_astnode_t *start = cypher_ast_range_get_start(varLength),
                               *end = cypher_ast_range_get_end(varLength);
        const char *startValueStr = start == NULL ? NULL : cypher_ast_integer_get_valuestr(start),
                   *endValueStr = end == NULL ? NULL : cypher_ast_integer_get_valuestr(end);
        char *strPtr = NULL;
        if (startValueStr != NULL) edge.minLen = strtol(startValueStr, &strPtr, 10);
        if (endValueStr != NULL) edge.maxLen = strtol(endValueStr, &strPtr, 10);
        // e.g., [*3..]
        if (startValueStr != NULL && endValueStr == NULL) edge.maxLen = INT_MAX;
    }
    CHECK(edge.minLen <= edge.maxLen);
}
}  // namespace AGE
