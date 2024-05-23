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

#include "plan/plan_graph.h"
#include <algorithm>
#include <cstddef>
#include <set>
#include <string>
#include "base/expr_util.h"
#include "base/expression.h"
#include "base/graph_entity.h"
#include "glog/logging.h"
#include "plan/ast_helper.h"
#include "plan/pattern_graph.h"
#include "plan/plan_graph_optimizer.h"
#include "plan/planner_exception.h"
#include "plan/type.h"
#include "plan/var_map.h"

namespace AGE {

using std::string;

void PlanGraph::Vertex::CollectPredicateExprVars(const Expression &expr) {
    if (expr.type == EXP_PATTERN_PATH) {
        AstHelper::CollectPatternPathVars(reinterpret_cast<const ASTNode *>(expr.GetValue().integerVal), vars, varMap);
        AstHelper::CollectPatternPathPropVars(reinterpret_cast<const ASTNode *>(expr.GetValue().integerVal), vars,
                                              varMap, strMap);
    } else {
        set<VarId> varSet = ExprUtil::GetRefVar(expr);
        for (VarId var : varSet) vars.insert(var);
    }
}

void PlanGraph::Vertex::CollectMatchPatternVars(const ASTNode *ast_match) {
    assert(cypher_astnode_type(ast_match) == CYPHER_AST_MATCH);

    const ASTNode *ast_pattern = cypher_ast_match_get_pattern(ast_match);
    u32 path_cnt = cypher_ast_pattern_npaths(ast_pattern);
    for (u32 i = 0; i < path_cnt; i++) {
        const ASTNode *ast_pattern_path = cypher_ast_pattern_get_path(ast_pattern, i);
        AstHelper::CollectPatternPathVars(ast_pattern_path, vars, varMap);
    }
}

void PlanGraph::Vertex::CollectMatchInlinedPropFilter(const ASTNode *ast_match) {
    assert(cypher_astnode_type(ast_match) == CYPHER_AST_MATCH);
    const ASTNode *ast_pattern = cypher_ast_match_get_pattern(ast_match);
    u32 path_cnt = cypher_ast_pattern_npaths(ast_pattern);
    for (u32 i = 0; i < path_cnt; i++) {
        const ASTNode *ast_pattern_path = cypher_ast_pattern_get_path(ast_pattern, i);
        AstHelper::CollectPatternPathInlinedPropFilter(ast_pattern_path, varMap, strMap, filters);
    }
}

void PlanGraph::Vertex::CollectMatchPatternPropVars(const ASTNode *ast_match) {
    assert(cypher_astnode_type(ast_match) == CYPHER_AST_MATCH);

    const ASTNode *ast_pattern = cypher_ast_match_get_pattern(ast_match);
    u32 path_cnt = cypher_ast_pattern_npaths(ast_pattern);
    for (u32 i = 0; i < path_cnt; i++) {
        const ASTNode *ast_pattern_path = cypher_ast_pattern_get_path(ast_pattern, i);
        AstHelper::CollectPatternPathPropVars(ast_pattern_path, vars, varMap, strMap);
    }
}

void PlanGraph::Vertex::CheckPatternEdgeVarExpr() const {
    for (const Expression *const varExpr : ExprUtil::GetTypedExpr(EXP_VARIABLE, filters)) {
        VarId var = varMap.IsPropVarId(varExpr->varId) ? varMap.ParsePropVarId(varExpr->varId).first : varExpr->varId;
        if (patternGraph.E.count(var) != 0) {
            throw PlannerException("Edge variable expression is not supported.");
        }
    }
}

// PatternExpressions are not allowed to introduce new variables
// e.g. "MATCH (a)--(b) WHERE (b)--(c) RETURN a" is not allowed
void PlanGraph::Vertex::CheckPatternExprIntroduceNewVariable(const ASTNode *ast_pattern_path) const {
    u32 len = cypher_ast_pattern_path_nelements(ast_pattern_path);
    for (u32 i = 0; i < len; i++) {
        const ASTNode *n = cypher_ast_pattern_path_get_element(ast_pattern_path, i);
        VarName varName = AstHelper::GetVarName(n);
        if (VarMap::IsAnonVarName(varName)) continue;
        VarId var = varMap[varName];
        if (vars.count(var) == 0) {
            throw PlannerException("PatternExpressions are not allowed to introduce new variables: '" + varName + "'");
        }
    }
}
void PlanGraph::Vertex::CheckPatternExprIntroduceNewVariable(const Expression &expr) const {
    auto exprs = ExprUtil::GetTypedExpr(EXP_PATTERN_PATH, expr);
    for (const Expression *const pattern_expr : exprs) {
        CheckPatternExprIntroduceNewVariable(reinterpret_cast<const ASTNode *>(pattern_expr->GetValue().integerVal));
    }
}

void PlanGraph::Vertex::CheckExprContainUndefiendVar(const Expression &expr) const {
    auto varExprs = ExprUtil::GetTypedExpr(EXP_VARIABLE, expr);
    for (const Expression *const expr : varExprs) {
        if (vars.count(expr->varId) == 0) {
            throw PlannerException("Variable '" + varMap[expr->varId] + "' not defined");
        }

        // If variable is prop var, also check its source variable
        if (varMap.IsPropVarId(expr->varId)) {
            auto [srcVar, _] = varMap.ParsePropVarId(expr->varId);
            if (vars.count(srcVar) == 0) {
                throw PlannerException("Variable '" + varMap[srcVar] + "' not defined");
            }
        }
    }
}

bool PlanGraph::Vertex::HasPatternInSubtree(const Expression &expr, ExprMap &hasPattern) {
    if (hasPattern.count(expr)) return hasPattern[expr];

    bool has = false;
    for (const Expression &child : expr.GetChildren()) has |= HasPatternInSubtree(child, hasPattern);
    if (expr.type == EXP_PATTERN_PATH) has = true;

    hasPattern.emplace(expr, has);
    return has;
}

// Build PlanGraph::Vertex from complex expression like:
//      (a)-->(b) AND (c)--(d)
//      (a)-->(b) OR a.age = 25 OR b.age < 20
void PlanGraph::Vertex::Build(PlanGraph &graph, const Expression &expr, const Expression *parent, ExprMap &hasPattern) {
    if (!expr.IsCondition() || !HasPatternInSubtree(expr, hasPattern)) {
        // No subquery generated, only collect variables
        CollectPredicateExprVars(expr);
        filters.emplace_back(expr);
        return;
    }

    // expr is AND/OR/NOT && has PatternFilter in subtree. Need to generate subquery
    PlanGraph::Vertex *condition = graph.EmplaceVertex(graph);
    for (const Expression &child : expr.GetChildren()) {
        PlanGraph::Vertex *cd = graph.EmplaceVertex(graph);
        cd->Build(graph, child, &expr, hasPattern);

        // Collect input vars for child conditions
        ConnectTwoVertices(condition, cd, PE_SUBQUERY);
        PlanGraph::Edge &outPe = condition->out.back();
        BuildVarsTransfer(outPe, condition, cd, true);
    }

    // Connect condition & this
    PlanGraph::EdgeType peType = expr.type == EXP_AND  ? PE_BRANCH_AND
                                 : expr.type == EXP_OR ? PE_BRANCH_OR
                                                       : PE_BRANCH_NOT;
    ConnectTwoVertices(this, condition, peType);
    PlanGraph::Edge &outPe = out.back();
    BuildVarsTransfer(outPe, this, condition, true);
}

void PlanGraph::Vertex::BuildPredicate(PlanGraph &graph, const Expression &expr) {
    if (expr.IsEmpty()) return;
    ExprMap hasPattern;
    HasPatternInSubtree(expr, hasPattern);
    Build(graph, expr, nullptr, hasPattern);
}

void PlanGraph::Vertex::Build(PlanGraph &graph, VarId edgeId) {
    CHECK(patternGraph.E.count(edgeId));
    PatternGraph::Edge edge = patternGraph.E[edgeId];
    PlanGraph::Vertex *loop = graph.EmplaceVertex(graph), *subq = graph.EmplaceVertex(graph);

    // LogicalPlanBuilder id subquery with this edgeId
    loop->vars.insert(edgeId);
    // Copy the edge to subquery vertex
    subq->CopyPatternEdge(this, edgeId);
    // Variable length pattern does not have a name (hence varId)
    subq->vars.insert(edge.src), subq->vars.insert(edge.dst);

    ConnectTwoVertices(loop, subq, PE_SUBQUERY);
    ConnectTwoVertices(this, loop, PE_LOOP);
}

void PlanGraph::Vertex::BuildVarLengthPatternMatching(PlanGraph &graph) {
    for (const auto &[varId, edge] : patternGraph.E) {
        if (!PatternGraph::IsVarLengthPattern(edge)) continue;
        // Variable length edges are preserved in the original PV to facilitate expandBuilder
        Build(graph, varId);
    }
}

// Process WITH/RETURN
void PlanGraph::Vertex::BuildProj(PlanGraph &graph, PlanGraph::Vertex *prev, const ASTNode *astProj) {
    ConnectTwoVertices(prev, this, PE_PROJ);
    PlanGraph::Edge &outPe = prev->out.back();

    // Collect variables for previous PlanGraph::Vertex
    AstHelper::CollectProjPropVars(astProj, prev->vars, varMap, strMap);

    // Last clause of prev PlanGraph::Vertex should be a projection
    // Generate them and fetch DISTINCT/LIMIT parameters
    auto projExprs = AstHelper::BuildProjExprs(astProj, varMap, strMap, outPe.GetDistinct(), outPe.GetLimit());
    try {
        // Check pattern_path in projection exprs
        for (auto [expr, _] : projExprs) {
            if (ExprUtil::GetTypedExpr(EXP_PATTERN_PATH, expr).size() > 0) {
                throw PlannerException("PatternExpressions are not allowed to appear in projection");
            }
            prev->CheckExprContainUndefiendVar(expr);
        }
    } catch (PlannerException &e) {
        throw e;
    }
    // Insert into planEdge
    for (auto &[expr, varId] : projExprs) {
        vars.insert(varId);
        outPe.AddVar(expr, varId);
    }

    // Generate ORDER BY expressions
    auto orderExprs = AstHelper::BuildOrderExprs(astProj, varMap, strMap);
    try {
        // Check pattern_path in orderby exprs
        for (auto [expr, _] : orderExprs) {
            if (ExprUtil::GetTypedExpr(EXP_PATTERN_PATH, expr).size() > 0) {
                throw PlannerException("PatternExpressions are not allowed to appear in ORDER BY");
            }
            prev->CheckExprContainUndefiendVar(expr);
        }
    } catch (PlannerException &e) {
        throw e;
    }
    // Insert into planEdge
    for (auto &[expr, asc] : orderExprs) outPe.AddOrderBy(expr, asc);

    // Check projection predicate
    // TODO(ycli): support "MATCH (a)--(b) WITH a AS aa, b AS bb WHERE a = bb RETURN aa, bb"
    ASTNodeType type = cypher_astnode_type(astProj);
    const ASTNode *pred = type == CYPHER_AST_WITH ? cypher_ast_with_get_predicate(astProj) : nullptr;
    if (pred != nullptr) {
        AstHelper::CollectExprPropVars(pred, vars, varMap, strMap);
        Expression predExpr = AstHelper::ToExpression(pred, varMap, strMap);
        BuildPredicate(graph, predExpr);
        CheckPatternExprIntroduceNewVariable(predExpr);
        CheckExprContainUndefiendVar(predExpr);
    }
}

void PlanGraph::Vertex::BuildMatch(PlanGraph &graph, Vertex *prev, const ASTNode *match_clause) {
    CollectMatchPatternVars(match_clause);
    CollectMatchPatternPropVars(match_clause);
    CollectMatchInlinedPropFilter(match_clause);

    // Build predicates, branch subqueries if any
    const ASTNode *pred = cypher_ast_match_get_predicate(match_clause);
    if (pred != nullptr) {
        AstHelper::CollectExprPropVars(pred, vars, varMap, strMap);
        Expression predExpr = AstHelper::ToExpression(pred, varMap, strMap);
        BuildPredicate(graph, predExpr);
        CheckPatternExprIntroduceNewVariable(predExpr);
        CheckExprContainUndefiendVar(predExpr);
    }

    // Build patternGraphs, matching subqueries if any
    patternGraph.AddMatchPattern(match_clause, varMap, strMap);
    if (patternGraph.HasVarLengthPatterns()) BuildVarLengthPatternMatching(graph);

    // Connect this OPTIONAL MATCH with previous MATCH
    if (prev != nullptr) {
        CHECK(cypher_ast_match_is_optional(match_clause));
        ConnectTwoVertices(prev, this, PE_OPTIONAL_MATCH);
        PlanGraph::Edge &outPe = prev->out.back();
        // 1. If a variable is generated outside the subquery, build a tranfer from outside
        // 2. If the subquery introduces new variables, add it to srcV
        BuildVarsTransfer(outPe, prev, this, false);
    }
}

// Build PlanGraph::Vertex from cypher_ast
void PlanGraph::Vertex::Build(PlanGraph &graph, const ASTNode *ast_query, size_t beg) {
    assert(cypher_astnode_type(ast_query) == CYPHER_AST_QUERY);

    u32 clause_count = cypher_ast_query_nclauses(ast_query);
    for (size_t i = beg; i < clause_count; i++) {
        const ASTNode *ast_clause = cypher_ast_query_get_clause(ast_query, i);

        ASTNodeType type = cypher_astnode_type(ast_clause);
        if (type == CYPHER_AST_MATCH) {
            if (cypher_ast_match_is_optional(ast_clause)) {
                if (i == 0)
                    throw PlannerException("OPTIONAL MATCH cannot be the first clause of a query. Use MATCH instead");
                Vertex *nxt = graph.EmplaceVertex(graph);
                nxt->BuildMatch(graph, this, ast_clause);
            } else {
                BuildMatch(graph, nullptr, ast_clause);
            }
        } else if (type == CYPHER_AST_WITH) {
            Vertex *nxt = graph.EmplaceVertex(graph);
            nxt->BuildProj(graph, this, ast_clause);
            nxt->Build(graph, ast_query, i + 1);
            break;
        } else if (type == CYPHER_AST_RETURN) {
            if (i != clause_count - 1) throw PlannerException("RETURN can only be use at the end of query");
            Vertex *nxt = graph.EmplaceVertex(graph);
            nxt->BuildProj(graph, this, ast_clause);
            nxt->Build(graph, ast_query, i + 1);
            break;
        } else {
            throw PlannerException("Unsupported clause: " + string(cypher_astnode_typestr(type)));
        }

        // Check last clause must be return
        if (i == clause_count - 1 && type != CYPHER_AST_RETURN) {
            throw PlannerException("Query cannot conclude with " + string(cypher_astnode_typestr(type)));
        }
    }
}

void PlanGraph::Build() {
    source = EmplaceVertex(*this);
    try {
        // To make it convenient for LogicalPlanBuilder::Build()
        source->in.emplace_back(PE_SUBQUERY, nullptr, source);
        source->Build(*this, root, 0);

        CheckPatternEdgeVarExpr();
    } catch (PlannerException &e) {
        CHECK(CheckIntegrity());
        throw e;
    }
}

// Copy a pattern edge from srcV to this. Copy pattern nodes accordingly
void PlanGraph::Vertex::CopyPatternEdge(const Vertex *const srcV, VarId edgeId) {
    CHECK(srcV->patternGraph.E.count(edgeId));
    PatternGraph::Edge edge = srcV->patternGraph.E.at(edgeId);

    CHECK(patternGraph.E.emplace(edgeId, edge).second);
    if (!this->patternGraph.V.count(edge.src)) {
        auto [iter, insert] = this->patternGraph.V.emplace(edge.src, srcV->patternGraph.V.at(edge.src));
        CHECK(insert);
        iter->second.edges.clear();
    }
    if (!this->patternGraph.V.count(edge.dst)) {
        auto [iter, insert] = this->patternGraph.V.emplace(edge.dst, srcV->patternGraph.V.at(edge.dst));
        CHECK(insert);
        iter->second.edges.clear();
    }
    this->patternGraph.V[edge.src].edges.emplace_back(edgeId);
    this->patternGraph.V[edge.dst].edges.emplace_back(edgeId);
}

PlanGraph::Vertex *const PlanGraph::Edge::GetOpposite(const Vertex *v) const {
    CHECK(srcV != nullptr && dstV != nullptr);
    if (v == srcV) return dstV;
    if (v == dstV) return srcV;
    return nullptr;
}

bool PlanGraph::Edge::operator==(const PlanGraph::Edge &rhs) const {
    return type == rhs.type && srcV == rhs.srcV && dstV == rhs.dstV;
}

void PlanGraph::Edge::AddVar(const Expression &expr, VarId dst_var_id) { vars.emplace(expr, dst_var_id); }

void PlanGraph::Edge::AddVar(VarId src_var_id, VarId dst_var_id) {
    vars.emplace(Expression(EXP_VARIABLE, src_var_id), dst_var_id);
}

void PlanGraph::Edge::AddOrderBy(const Expression &expr, isASC asc) { orderbyExps.emplace_back(expr, asc); }

PlanGraph::Vertex *PlanGraph::EmplaceVertex(const PlanGraph &graph) {
    vtxList.emplace_back(new Vertex(graph));
    return vtxList.back();
}

void PlanGraph::EraseVertex(Vertex *v) {
    // LOG(INFO) << "Erasing: " << v->DebugString();
    auto itr = std::find(vtxList.begin(), vtxList.end(), v);
    CHECK(itr != vtxList.end());

    for (auto &outPe : v->out) {
        Vertex *oppositeV = outPe.GetOpposite(v);
        auto itr = std::find(oppositeV->in.begin(), oppositeV->in.end(), outPe);
        CHECK(itr != oppositeV->in.end());
        oppositeV->in.erase(itr);
    }
    for (auto &inPe : v->in) {
        Vertex *oppositeV = inPe.GetOpposite(v);
        auto itr = std::find(oppositeV->out.begin(), oppositeV->out.end(), inPe);
        CHECK(itr != oppositeV->out.end());
        oppositeV->out.erase(itr);
    }

    vtxList.erase(itr);
    delete v;
}

void PlanGraph::ConnectTwoVertices(Vertex *const srcV, Vertex *const dstV, EdgeType type) {
    srcV->out.emplace_back(type, srcV, dstV);
    dstV->in.emplace_back(type, srcV, dstV);
}

void PlanGraph::BuildVarsTransfer(Edge &outPe, Vertex *const srcV, Vertex *const dstV, bool isMandatory) {
    set<VarId> newVarsToSource;
    for (VarId varId : dstV->vars) {
        VarName varName = srcV->varMap[varId];
        // Since creating new vars in expression is not allowed, all non-anonymous variables are input vars
        // Vid of a property is transferred and propertyOp will be generated inside subqueries
        if (VarMap::IsAnonVarName(varName)) continue;
        if (VarMap::IsPropVarName(varName)) varId = srcV->varMap.ParsePropVarId(varId).first;

        // isMandatory == true: building filter subqueries (i.e., predicates DON'T introduce variables)
        // isMandatory == false && sourceHasVariable == false: opt match that introduces new vars to the main planVtx
        // (hence do not require a transfer)
        bool sourceHasVariable = srcV->vars.count(varId);
        if (isMandatory || sourceHasVariable) {
            outPe.AddVar(varId, varId);
        }
        newVarsToSource.insert(varId);
    }
    srcV->vars.insert(newVarsToSource.begin(), newVarsToSource.end());
}

void PlanGraph::CheckPatternEdgeVarExpr() const {
    for (Vertex *vtx : vtxList) vtx->CheckPatternEdgeVarExpr();
}

bool PlanGraph::CheckIntegrity() const {
    for (const Vertex *vtx : vtxList) {
        for (const Edge &pe : vtx->out) {
            Vertex *dstV = pe.GetOpposite(vtx);
            // For now each node has only one in edge
            CHECK(dstV != nullptr);
            CHECK(dstV->in.size() == 1 && dstV->in.front().GetOpposite(dstV) == vtx);
        }
        for (const Edge &pe : vtx->in) {
            // except nullptr - [SUBQUERY] -> source
            if (vtx != source) CHECK(pe.GetOpposite(vtx) != nullptr);
        }
    }
    return true;
}

PlanGraph::~PlanGraph() {
    // PlanGraph owns all vertices
    for (Vertex *vtx : vtxList) delete vtx;
}
}  // namespace AGE
