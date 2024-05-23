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
#include <list>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/expression.h"
#include "base/graph_entity.h"
#include "base/proj_util.h"
#include "base/type.h"
#include "plan/ast.h"
#include "plan/logical_plan.h"
#include "plan/pattern_graph.h"
#include "plan/planner_exception.h"
#include "plan/semantic_checker.h"
#include "plan/type.h"
#include "plan/var_map.h"
#include "storage/str_map.h"

namespace AGE {
using std::list;
using std::map;
using std::set;
using std::string;

class PlanGraph {
   public:
    typedef enum : uint16_t {
        PE_SUBQUERY = 0,
        PE_BRANCH_AND = 1,
        PE_BRANCH_OR = 2,
        PE_BRANCH_NOT = 3,
        PE_OPTIONAL_MATCH = 4,
        PE_LOOP = 5,
        PE_PROJ = 6
    } EdgeType;

    static constexpr char *EdgeTypeStr[] = {(char *)"Subquery",  (char *)"BranchAnd",     (char *)"BranchOr",
                                            (char *)"BranchNot", (char *)"OptionalMatch", (char *)"Loop",
                                            (char *)"Proj"};
    static bool EdgeType_IsBranch(EdgeType type) {
        return type == PE_BRANCH_AND || type == PE_BRANCH_OR || type == PE_BRANCH_NOT;
    }
    static bool EdgeType_IsSubquery(EdgeType type) { return type != PE_PROJ; }
    static string EdgeType_DebugString(EdgeType type) { return EdgeTypeStr[type]; }

    // PlanGraph::Edge illustrate the relationship between two PlanGraph::Vertex
    // A PlanGraph::Edge from PlanGraph::Vertex {A} to PlanGraph::Vertex {B} could be:
    //  1. Subquery(BranchAnd/BranchOr/OptionalMatch ... ) means B is a sub-query of A
    //  2. Projection(WITH) means A is followed by B through projection
    class Vertex;
    class Edge {
       public:
        Edge(EdgeType type, Vertex *srcV, Vertex *dstV) : type(type), srcV(srcV), dstV(dstV) {}
        ~Edge() = default;

        // srcV - [type] -> dstV
        PlanGraph::EdgeType type;
        PlanGraph::Vertex *srcV, *dstV;

        bool operator==(const PlanGraph::Edge &rhs) const;
        void AddVar(const Expression &expr, VarId dst_var_id);
        void AddVar(VarId src_var_id, VarId dst_var_id);
        void AddOrderBy(const Expression &expr, isASC asc);

        Vertex *const GetOpposite(const Vertex *v) const;
        map<Expression, VarId> &GetVars() { return vars; }
        const map<Expression, VarId> &GetVars() const { return vars; }
        vector<pair<Expression, isASC>> &GetOrderBy() { return orderbyExps; }
        const vector<pair<Expression, isASC>> &GetOrderBy() const { return orderbyExps; }
        bool &GetDistinct() { return globalDistinct; }
        const bool &GetDistinct() const { return globalDistinct; }
        int &GetLimit() { return limitNum; }
        const int &GetLimit() const { return limitNum; }

        string DebugString() const {
            string ret =
                "[address: " + to_string(reinterpret_cast<u64>(this)) + ", " + EdgeType_DebugString(type) + ", vars: [";
            for (const auto &[_, varId] : GetVars()) ret += to_string(varId) + " ";
            ret += "]]";
            return ret;
        }

        string DebugString(const PlanGraph::Vertex *srcV) const {
            CHECK(srcV == this->srcV);
            string ret = srcV->DebugString() + "-[";
            ret += to_string(reinterpret_cast<uint64_t>(this)) + ", " + EdgeType_DebugString(type) + ", vars: [";
            for (const auto &[_, varId] : GetVars()) ret += to_string(varId) + " ";
            ret += "]]->" + dstV->DebugString();
            return ret;
        }

       private:
        // Describe the variable transfer between PlanGraph::Vertex
        map<Expression, VarId> vars;

        // Projection (WITH/RETURN) Parameters: ORDER BY, DISTINCT and LIMIT
        vector<pair<Expression, isASC>> orderbyExps;
        bool globalDistinct = false;
        int limitNum = LimitUtil::WITHOUT_LIMIT;
    };

    // PlanGraph::Vertex is a continuous(on final execution order) execution unit
    // e.g.
    // "MATCH (n) WITH n match (n)--(m) return n, m;" can be divided into 2 PlanGraph::Vertex:
    //      1. "MATCH (n) WITH n"
    //      2. "MATCH (n)--(m) RETURN n, m;"
    //
    // "MATCH (a)-->(b)-->(c)-->(d) WHERE (a)-->(c) AND (b)-->(d) RETURN a, b, c, d" can be divided into 4
    // PlanGraph::Vertex:
    //      1. "MATCH (a)-->(b)-->(c)-->(d) WHERE ... RETURN a, b, c, d"
    //          2. "(a)-->(c) AND (b)-->(d)"
    //              3. "(a)-->(c)"
    //              4. "(b)-->(d)"
    class Vertex {
       public:
        typedef map<Expression, bool> ExprMap;

        // Plan edge
        vector<PlanGraph::Edge> in, out;

        // Variable set for this PlanGraph::Vertex
        set<VarId> vars;

        // Filters
        vector<Expression> filters;

        // Global through one PlanGraph (= graph.varMap/graph.strMap)
        VarMap &varMap;
        const StrMap &strMap;

        // Pattern graph
        PatternGraph patternGraph;

        explicit Vertex(const PlanGraph &graph)
            : varMap(graph.varMap), strMap(graph.strMap), patternGraph(graph.varMap) {}
        ~Vertex() = default;
        Vertex(Vertex &&rhs) = delete;
        Vertex(const Vertex &rhs) = delete;
        Vertex &operator=(Vertex &&rhs) = delete;
        Vertex &operator=(const Vertex &rhs) = delete;

        // Build PlanGraph::Vertex from complex expression like:
        //      (a)-->(b) AND (c)--(d)
        //      (a)-->(b) OR a.age = 25 OR b.age < 20
        void Build(PlanGraph &graph, const Expression &expr, const Expression *parent, ExprMap &hasPattern);

        // Build PlanGraph::Vertex from variable length pattern like:
        //      (a)--[*2..3]->(b)
        void Build(PlanGraph &graph, VarId edgeId);

        // Build PlanGraph::Vertex from cypher_ast
        void Build(PlanGraph &graph, const ASTNode *ast_query, size_t beg);

        string DebugString() const {
            string ret = "{address: " + to_string(reinterpret_cast<u64>(this));

            // Vars
            ret += ", vars: [";
            for (VarId var : vars) ret += to_string(var) + " ";
            ret += "]}";

            return ret;
        }

        // TODO(ycli): support pattern edge variable related expression
        // Currently not support pattern edge variable expression, check and throw exception.
        void CheckPatternEdgeVarExpr() const;

       private:
        // Process (OPTIONAL) MATCH clauses
        void BuildMatch(PlanGraph &graph, Vertex *prev, const ASTNode *proj_clause);

        // Process WHERE clauses
        bool HasPatternInSubtree(const Expression &expr, ExprMap &hasPattern);
        void BuildPredicate(PlanGraph &graph, const Expression &expr);

        // Process variable length edges
        void CopyPatternEdge(const Vertex *const srcV, VarId edgeId);
        void BuildVarLengthPatternMatching(PlanGraph &graph);

        // Process WITH/RETURN
        void BuildProj(PlanGraph &graph, Vertex *prev, const ASTNode *proj_clause);

        // Vars / Filters collection
        void CollectPredicateExprVars(const Expression &expr);
        void CollectMatchPatternVars(const ASTNode *match_clause);
        void CollectMatchInlinedPropFilter(const ASTNode *match_clause);
        void CollectMatchPatternPropVars(const ASTNode *match_clause);

        // PatternExpressions are not allowed to introduce new variables
        // e.g. "MATCH (a)--(b) WHERE (b)--(c) RETURN a" is not allowed
        void CheckPatternExprIntroduceNewVariable(const ASTNode *ast_pattern_path) const;
        void CheckPatternExprIntroduceNewVariable(const Expression &expr) const;

        // Check "MATCH (n) WHERE m.id = 5 RETURN n"
        void CheckExprContainUndefiendVar(const Expression &expr) const;
    };

    PlanGraph(const ASTNode *root, const StrMap &strMap, VarMap &varMap) : root(root), strMap(strMap), varMap(varMap) {}

    const ASTNode *root;  // Query cypher_astnode
    const StrMap &strMap;

    VarMap &varMap;

    // PlanGraph components
    Vertex *source;
    list<Vertex *> vtxList;

    void Build();
    ~PlanGraph();
    string DebugString() const {
        string v = "Vertices:\n", e = "Edges:\n";
        DebugString(source, v, e);
        return v + e;
    }

    Vertex *EmplaceVertex(const PlanGraph &graph);
    // Remove an existing vertex from vtxList, and all connected edges
    void EraseVertex(Vertex *const v);

    // Connect srcV - [type] -> dstV
    static void ConnectTwoVertices(Vertex *const srcV, Vertex *const dstV, EdgeType type);

    static void BuildVarsTransfer(Edge &outPe, Vertex *const srcV, Vertex *const dstV, bool isMandatory = false);

   private:
    // TODO(ycli): support pattern edge variable related expression
    // Currently not support pattern edge variable expression, check and throw exception.
    void CheckPatternEdgeVarExpr() const;

    // For successful recycle, check whether this plan graph is integral which means:
    //  - No edge has a nullptr oppositeV
    //  - Every edge appears both in inV and outV
    bool CheckIntegrity() const;

    void DebugString(const PlanGraph::Vertex *planVtx, string &v, string &e) const {
        v += planVtx->DebugString() + "\n";
        for (const PlanGraph::Edge &pe : planVtx->out) e += pe.DebugString(planVtx) + "\n";
        for (const PlanGraph::Edge &pe : planVtx->out) DebugString(pe.GetOpposite(planVtx), v, e);
    }
};
}  // namespace AGE
