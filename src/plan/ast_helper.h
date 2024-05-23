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
#include <set>
#include <string>
#include <utility>
#include <vector>
#include "base/proj_util.h"
#include "plan/ast.h"
#include "plan/type.h"
#include "plan/var_map.h"
#include "storage/str_map.h"

namespace AGE {
using std::set;
using std::vector;

// Complex ast function provider
class AstHelper {
   public:
    static ExecutionType GetExecutionType(const ASTNode *n);
    static void GetAstnodeByType(const ASTNode *root, ASTNodeType type, vector<const ASTNode *> &v);
    static vector<u32> GetClauseIndices(const ASTNode *n, ASTNodeType type);
    static void CollectPatternPathInlinedPropFilter(const ASTNode *n, const VarMap &varMap, const StrMap &strMap,
                                                    vector<Expression> &exprs);
    static void CollectInlinedPropFilter(const ASTNode *n, const VarMap &varMap, const StrMap &strMap,
                                         vector<Expression> &exps);
    static void CollectPatternPathPropVars(const ASTNode *n, set<VarId> &vars, VarMap &varMap, const StrMap &strMap);
    static void CollectInlinedPropVars(const ASTNode *n, set<VarId> &vars, VarMap &varMap, const StrMap &strMap);
    static void CollectExprPropVars(const ASTNode *n, set<VarId> &vars, VarMap &varMap, const StrMap &strMap);
    static void CollectProjPropVars(const ASTNode *n, set<VarId> &vars, VarMap &varMap, const StrMap &strMap);
    static vector<string> GetReturnVarStrings(const ASTNode *ast_query);

    // Get var name of a CYPHER_AST_{NODE/REL}_PATTERN. Generate an var name if not given.
    static VarName GetVarName(const ASTNode *n);
    static VarName GetVarMapPropName(const string &s, const VarMap &varMap, const StrMap &strMap);

    // Collect variables in CYPHER_AST_PATTERN_PATH to VarMap
    static void CollectPatternPathVars(const ASTNode *n, set<VarId> &vars, VarMap &varMap);

    static vector<pair<Expression, VarId>> BuildProjExprs(const ASTNode *astProj, VarMap &varMap, const StrMap &strMap,
                                                          bool &globalDistinct, int &limitNum);

    static vector<pair<Expression, isASC>> BuildOrderExprs(const ASTNode *astProj, const VarMap &varMap,
                                                           const StrMap &strMap);

    // Expression convert function.
    static Expression ToExpression(const ASTNode *n, const VarMap &varMap, const StrMap &strMap);

   private:
    static Expression FromApplyOperator(const ASTNode *n, const VarMap &varMap, const StrMap &strMap);
    static Expression FromIdentifier(const ASTNode *n, const VarMap &varMap);
    static Expression FromPropertyOperator(const ASTNode *n, const VarMap &varMap, const StrMap &strMap);
    static Expression FromScalar(const ASTNode *n);
    static Expression FromInterger(const ASTNode *n);
    static Expression FromFloat(const ASTNode *n);
    static Expression CreateFromString(const ASTNode *n);
    static Expression FromTrue(const ASTNode *n);
    static Expression FromFalse(const ASTNode *n);
    static Expression FromNull(const ASTNode *n);
    static Expression FromUnaryOperator(const ASTNode *n, const VarMap &varMap, const StrMap &strMap);
    static Expression FromBinaryOperator(const ASTNode *n, const VarMap &varMap, const StrMap &strMap);
    static Expression FromComparison(const ASTNode *n, const VarMap &varMap, const StrMap &strMap);
    static Expression FromPattern(const ASTNode *n, const VarMap &varMap);
    static Expression FromPatternPath(const ASTNode *n);
    static Expression ToExpression_(const ASTNode *n, const VarMap &varMap, const StrMap &strMap);
    static ExpType FromBinaryCypherOperator(const cypher_operator_t *op);
};
}  // namespace AGE
