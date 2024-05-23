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

#include <glog/logging.h>
#include <stdlib.h>
#include <cstddef>
#include <string>
#include <utility>

#include "base/expr_func.h"
#include "base/expr_util.h"
#include "base/graph_entity.h"
#include "base/intervals.h"
#include "base/proj_util.h"
#include "plan/ast_helper.h"
#include "plan/planner_exception.h"
#include "plan/semantic_checker.h"
#include "plan/type.h"
#include "plan/var_map.h"
#include "storage/red_black_tree.h"

namespace AGE {
ExecutionType AstHelper::GetExecutionType(const ASTNode *n) {
    const ASTNodeType t = cypher_astnode_type(n);
    if (t == CYPHER_AST_QUERY) return EXECUTION_TYPE_QUERY;
    if (t == CYPHER_AST_CREATE_NODE_PROPS_INDEX) return EXECUTION_TYPE_INDEX_CREATE;
    if (t == CYPHER_AST_DROP_NODE_PROPS_INDEX) return EXECUTION_TYPE_INDEX_DROP;
    assert(false && "Unknown execution type");
    return static_cast<ExecutionType>(0);
}

void AstHelper::GetAstnodeByType(const ASTNode *root, ASTNodeType type, vector<const ASTNode *> &v) {
    if (cypher_astnode_type(root) == type) v.emplace_back(root);
    for (int i = cypher_astnode_nchildren(root) - 1; i >= 0; i--) {
        GetAstnodeByType(cypher_astnode_get_child(root, i), type, v);
    }
}

vector<u32> AstHelper::GetClauseIndices(const ASTNode *n, ASTNodeType type) {
    vector<u32> ret;
    u32 nClause = cypher_ast_query_nclauses(n);
    for (u32 i = 0; i < nClause; i++) {
        if (cypher_astnode_type(cypher_ast_query_get_clause(n, i)) == type) {
            ret.push_back(i);
        }
    }
    return ret;
}

VarName AstHelper::GetVarMapPropName(const string &s, const VarMap &varMap, const StrMap &strMap) {
    u64 delimiterPos = s.find('.');
    if (delimiterPos == string::npos || s.find(' ') != string::npos || s.find('(') != string::npos) return "";
    string sourceVarName = s.substr(0, delimiterPos), propKey = s.substr(delimiterPos + 1, string::npos);
    string propVarName = varMap.CreatePropVarName(sourceVarName, strMap.GetPropId(propKey));
    CHECK(varMap.nameIdMap.count(propVarName));
    return propVarName;
}

void AstHelper::CollectPatternPathInlinedPropFilter(const ASTNode *n, const VarMap &varMap, const StrMap &strMap,
                                                    vector<Expression> &exprs) {
    assert(cypher_astnode_type(n) == CYPHER_AST_PATTERN_PATH);
    u32 len = cypher_ast_pattern_path_nelements(n);
    for (u32 i = 0; i < len; i++) {
        AstHelper::CollectInlinedPropFilter(cypher_ast_pattern_path_get_element(n, i), varMap, strMap, exprs);
    }
}

void AstHelper::CollectInlinedPropFilter(const ASTNode *n, const VarMap &varMap, const StrMap &strMap,
                                         vector<Expression> &exprs) {
    const ASTNodeType t = cypher_astnode_type(n);
    const ASTNode *props = (t == CYPHER_AST_NODE_PATTERN) ? cypher_ast_node_pattern_get_properties(n)
                                                          : cypher_ast_rel_pattern_get_properties(n);

    if (props == NULL) return;

    VarName varName = AstHelper::GetVarName(n);
    for (int i = cypher_ast_map_nentries(props) - 1; i >= 0; i--) {
        string pkey = cypher_ast_prop_name_get_value(cypher_ast_map_get_key(props, i));
        PropId pid = strMap.GetPropId(pkey);
        const ASTNode *ast_val = cypher_ast_map_get_value(props, i);

        VarId propVarId = varMap[varMap.CreatePropVarName(varName, pid)];
        Expression eq(EXP_EQ);
        eq.EmplaceChild(ToExpression_(ast_val, varMap, strMap));
        eq.EmplaceChild(EXP_VARIABLE, propVarId);
        exprs.emplace_back(std::move(eq));
    }
}

void AstHelper::CollectExprPropVars(const ASTNode *n, set<VarId> &vars, VarMap &varMap, const StrMap &strMap) {
    ASTNodeType t = cypher_astnode_type(n);
    if (t == CYPHER_AST_PROPERTY_OPERATOR) {
        const ASTNode *prop_exp = cypher_ast_property_operator_get_expression(n),
                      *prop_key = cypher_ast_property_operator_get_prop_name(n);

        // arr[0].b  not support.
        if (cypher_astnode_type(prop_exp) != CYPHER_AST_IDENTIFIER)
            throw PlannerException("Unsupport feature: Array-type variable");

        string varName = cypher_ast_identifier_get_name(prop_exp), pkey = cypher_ast_prop_name_get_value(prop_key);
        PropId pid = strMap.GetPropId(pkey);

        // If using prop operator, replace prop op with variable
        VarName propVarName = varMap.CreatePropVarName(varName, pid);
        varMap.insert(propVarName);
        vars.insert(varMap[propVarName]);
        return;
    }

    u32 nChild = cypher_astnode_nchildren(n);
    for (u32 i = 0; i < nChild; i++) {
        const ASTNode *child = cypher_astnode_get_child(n, i);
        CollectExprPropVars(child, vars, varMap, strMap);
    }
}

void AstHelper::CollectPatternPathPropVars(const ASTNode *n, set<VarId> &vars, VarMap &varMap, const StrMap &strMap) {
    assert(cypher_astnode_type(n) == CYPHER_AST_PATTERN_PATH);

    u32 len = cypher_ast_pattern_path_nelements(n);
    for (u32 i = 0; i < len; i++) {
        CollectInlinedPropVars(cypher_ast_pattern_path_get_element(n, i), vars, varMap, strMap);
    }
}

void AstHelper::CollectInlinedPropVars(const ASTNode *n, set<VarId> &vars, VarMap &varMap, const StrMap &strMap) {
    const ASTNodeType t = cypher_astnode_type(n);
    const ASTNode *props = (t == CYPHER_AST_NODE_PATTERN) ? cypher_ast_node_pattern_get_properties(n)
                                                          : cypher_ast_rel_pattern_get_properties(n);

    if (props == NULL) return;

    VarName varName = AstHelper::GetVarName(n);
    for (int i = cypher_ast_map_nentries(props) - 1; i >= 0; i--) {
        string pkey = cypher_ast_prop_name_get_value(cypher_ast_map_get_key(props, i));

        PropId pid = strMap.GetPropId(pkey);
        VarName propVarName = varMap.CreatePropVarName(varName, pid);
        varMap.insert(propVarName);
        vars.insert(varMap[propVarName]);
    }
}

VarName AstHelper::GetVarName(const ASTNode *n) {
    ASTNodeType t = cypher_astnode_type(n);
    assert(t == CYPHER_AST_NODE_PATTERN || t == CYPHER_AST_REL_PATTERN);

    const ASTNode *identifierNode = (t == CYPHER_AST_NODE_PATTERN) ? cypher_ast_node_pattern_get_identifier(n)
                                                                   : cypher_ast_rel_pattern_get_identifier(n);

    VarName varName =
        (identifierNode == NULL) ? VarMap::CreateAnonVarName(n) : cypher_ast_identifier_get_name(identifierNode);

    return varName;
}

// Collect variables in CYPHER_AST_PATTERN_PATH to VarMap
void AstHelper::CollectPatternPathVars(const ASTNode *n, set<VarId> &vars, VarMap &varMap) {
    ASTNodeType t = cypher_astnode_type(n);
    assert(t == CYPHER_AST_PATTERN_PATH);

    u32 len = cypher_ast_pattern_path_nelements(n);

    for (u32 i = 0; i < len; i++) {
        VarName varName = GetVarName(cypher_ast_pattern_path_get_element(n, i));
        if ((i & 1) && varMap.nameIdMap.count(varName) != 0)
            throw PlannerException("Cannot use the same relationship variable " + varName + "for multiple patterns");

        varMap.insert(varName);
        vars.insert(varMap.at(varName));
    }
}

void AstHelper::CollectProjPropVars(const ASTNode *n, set<VarId> &vars, VarMap &varMap, const StrMap &strMap) {
    u32 nProj = 0, nSort = 0;
    const ASTNode *orderbyNode = NULL;
    ASTNodeType t = cypher_astnode_type(n);
    assert(t == CYPHER_AST_RETURN || t == CYPHER_AST_WITH);

    if (t == CYPHER_AST_RETURN) {
        nProj = cypher_ast_return_nprojections(n);
        orderbyNode = cypher_ast_return_get_order_by(n);
        if (orderbyNode != NULL) nSort = cypher_ast_order_by_nitems(orderbyNode);
    } else if (t == CYPHER_AST_WITH) {
        nProj = cypher_ast_with_nprojections(n);
        orderbyNode = cypher_ast_with_get_order_by(n);
        if (orderbyNode != NULL) nSort = cypher_ast_order_by_nitems(orderbyNode);
    }

    // Collect From projections
    for (u32 i = 0; i < nProj; i++) {
        const ASTNode *proj =
            (t == CYPHER_AST_RETURN) ? cypher_ast_return_get_projection(n, i) : cypher_ast_with_get_projection(n, i);

        const ASTNode *astExpr = cypher_ast_projection_get_expression(proj);
        CollectExprPropVars(astExpr, vars, varMap, strMap);
    }
    // Collect From ORDER BYs
    for (u32 i = 0; i < nSort; i++) {
        const ASTNode *orderbyItem = cypher_ast_order_by_get_item(orderbyNode, i);
        const ASTNode *orderbyExpr = cypher_ast_sort_item_get_expression(orderbyItem);
        CollectExprPropVars(orderbyExpr, vars, varMap, strMap);
    }
}

vector<string> AstHelper::GetReturnVarStrings(const ASTNode *ast_query) {
    vector<string> ret;
    u32 clause_count = cypher_ast_query_nclauses(ast_query);
    const ASTNode *lastAstNode = cypher_ast_query_get_clause(ast_query, clause_count - 1);
    const ASTNodeType type = cypher_astnode_type(lastAstNode);

    // Check last clause must be return
    if (cypher_astnode_type(lastAstNode) != CYPHER_AST_RETURN) {
        throw PlannerException("Query cannot conclude with " + string(cypher_astnode_typestr(type)));
    }

    bool projAll = cypher_ast_return_has_include_existing(lastAstNode);
    u32 nProj = cypher_ast_return_nprojections(lastAstNode);

    if (projAll) throw PlannerException("RETURN * not supported");

    for (u32 i = 0; i < nProj; i++) {
        const ASTNode *proj = cypher_ast_return_get_projection(lastAstNode, i),
                      *astAlias = cypher_ast_projection_get_alias(proj),
                      *astExpr = cypher_ast_projection_get_expression(proj);

        bool unAliased = !astAlias && cypher_astnode_type(astExpr) != CYPHER_AST_IDENTIFIER;
        if (unAliased) throw PlannerException("Expression in WITH must be aliased (use AS)");

        ret.emplace_back(astAlias ? cypher_ast_identifier_get_name(astAlias) : cypher_ast_identifier_get_name(astExpr));
    }

    return ret;
}

vector<pair<Expression, VarId>> AstHelper::BuildProjExprs(const ASTNode *astProj, VarMap &varMap, const StrMap &strMap,
                                                          bool &globalDistinct, int &limitNum) {
    u32 nProj = 0;
    bool projAll = false;
    vector<pair<Expression, VarId>> ret;
    ASTNodeType t = cypher_astnode_type(astProj);
    assert(t == CYPHER_AST_RETURN || t == CYPHER_AST_WITH);

    if (t == CYPHER_AST_RETURN) {
        projAll = cypher_ast_return_has_include_existing(astProj);
        nProj = cypher_ast_return_nprojections(astProj);
        globalDistinct = cypher_ast_return_is_distinct(astProj);

        const cypher_astnode_t *limitNode = cypher_ast_return_get_limit(astProj);
        if (limitNode != NULL) limitNum = std::stoi(cypher_ast_integer_get_valuestr(limitNode));
    } else if (t == CYPHER_AST_WITH) {
        projAll = cypher_ast_with_has_include_existing(astProj);
        nProj = cypher_ast_with_nprojections(astProj);
        globalDistinct = cypher_ast_with_is_distinct(astProj);

        const cypher_astnode_t *limitNode = cypher_ast_with_get_limit(astProj);
        if (limitNode != NULL) limitNum = std::stoi(cypher_ast_integer_get_valuestr(limitNode));
    }

    if (projAll) throw PlannerException("RETURN * not supported");

    // "RETURN a, b, a + b AS aplusb".
    for (u32 i = 0; i < nProj; i++) {
        const ASTNode *proj = (t == CYPHER_AST_RETURN) ? cypher_ast_return_get_projection(astProj, i)
                                                       : cypher_ast_with_get_projection(astProj, i);

        const ASTNode *astAlias = cypher_ast_projection_get_alias(proj),
                      *astExpr = cypher_ast_projection_get_expression(proj);

        // Expression in WITH must be aliased
        // e.g. WITH count(b) -> error
        bool unAliased = (t == CYPHER_AST_WITH && !astAlias && cypher_astnode_type(astExpr) != CYPHER_AST_IDENTIFIER);
        if (unAliased) {
            ret.clear();
            throw PlannerException("Expression in WITH must be aliased (use AS)");
        }

        Expression expr = ToExpression(astExpr, varMap, strMap);

        VarName varName = astAlias ? cypher_ast_identifier_get_name(astAlias) : cypher_ast_identifier_get_name(astExpr);
        VarName innerVarName = GetVarMapPropName(varName, varMap, strMap);
        if (innerVarName.size()) {
            // Got a property variable name (that should have already been collected), e.g., n.int -> _prop:n.2
            // LOG(INFO) << varName << " -> " << innerVarName;
            varMap.nameIdMap[varName] = varMap.nameIdMap[innerVarName];
        } else {
            varMap.insert(varName);
        }

        ret.emplace_back(std::move(expr), varMap[varName]);
    }

    return ret;
}

vector<pair<Expression, isASC>> AstHelper::BuildOrderExprs(const ASTNode *astProj, const VarMap &varMap,
                                                           const StrMap &strMap) {
    u32 nsort = 0;
    vector<pair<Expression, isASC>> ret;
    cypher_astnode_type_t t = cypher_astnode_type(astProj);
    assert(t == CYPHER_AST_RETURN || t == CYPHER_AST_WITH);

    const ASTNode *orderbyNode =
        (t == CYPHER_AST_RETURN) ? cypher_ast_return_get_order_by(astProj) : cypher_ast_with_get_order_by(astProj);
    if (orderbyNode == NULL) return ret;

    nsort = cypher_ast_order_by_nitems(orderbyNode);
    for (u32 i = 0; i < nsort; i++) {
        const ASTNode *orderbyItem = cypher_ast_order_by_get_item(orderbyNode, i);
        const ASTNode *orderbyExpr = cypher_ast_sort_item_get_expression(orderbyItem);
        bool isAscend = cypher_ast_sort_item_is_ascending(orderbyItem);
        Expression expr = ToExpression(orderbyExpr, varMap, strMap);
        ret.emplace_back(std::move(expr), isAscend);
    }
    return ret;
}

Expression AstHelper::FromApplyOperator(const ASTNode *n, const VarMap &varMap, const StrMap &strMap) {
    // Currently only support aggregation function.
    // min(x), max(x), avg(x), count(x) supported.

    ExprFunc *exprFunc = ExprFunc::GetInstance();

    u32 nArg = cypher_ast_apply_operator_narguments(n);
    const ASTNode *funcNode = cypher_ast_apply_operator_get_func_name(n);
    const char *funcName = cypher_ast_function_name_get_value(funcNode);
    bool aggregate = exprFunc->isAggFunc(funcName);
    bool distinct = cypher_ast_apply_operator_get_distinct(n);

    assert(aggregate);

    Expression expr = Expression(EXP_AGGFUNC, exprFunc->getAggType(funcName), distinct);
    for (u32 i = 0; i < nArg; i++) {
        const ASTNode *arg = cypher_ast_apply_operator_get_argument(n, i);
        expr.EmplaceChild(ToExpression_(arg, varMap, strMap));
    }
    return expr;
}

Expression AstHelper::FromIdentifier(const ASTNode *n, const VarMap &varMap) {
    assert(cypher_astnode_type(n) == CYPHER_AST_IDENTIFIER);
    const char *varName = cypher_ast_identifier_get_name(n);
    return Expression(EXP_VARIABLE, varMap[varName]);
}

Expression AstHelper::FromPropertyOperator(const ASTNode *n, const VarMap &varMap, const StrMap &strMap) {
    // Two kinds:
    //      1. a.b
    //      2. arr[0].b  not support.

    const ASTNode *prop_exp = cypher_ast_property_operator_get_expression(n),
                  *prop_key = cypher_ast_property_operator_get_prop_name(n);

    // arr[0].b  not support.
    if (cypher_astnode_type(prop_exp) != CYPHER_AST_IDENTIFIER)
        throw PlannerException("Unsupport feature: Array-type variable");

    string varName = cypher_ast_identifier_get_name(prop_exp), pkey = cypher_ast_prop_name_get_value(prop_key);

    PropId pid = strMap.GetPropId(pkey);

    // If using prop operator, replace prop op with variable
    VarName prop_varName = varMap.CreatePropVarName(varName, pid);
    assert(varMap.nameIdMap.count(prop_varName) > 0);
    Expression var = Expression(EXP_VARIABLE, varMap[prop_varName]);
    return var;
}

Expression AstHelper::FromInterger(const ASTNode *n) {
    const char *str = cypher_ast_integer_get_valuestr(n);
    return Expression(EXP_CONSTANT, Item(T_INTEGER, static_cast<int64_t>(strtoll(str, NULL, 0))));
}
Expression AstHelper::FromFloat(const ASTNode *n) {
    const char *str = cypher_ast_float_get_valuestr(n);
    return Expression(EXP_CONSTANT, Item(T_FLOAT, strtod(str, NULL)));
}
Expression AstHelper::CreateFromString(const ASTNode *n) {
    return Expression(EXP_CONSTANT, Item(T_STRING, cypher_ast_string_get_value(n)));
}
Expression AstHelper::FromTrue(const ASTNode *n) { return Expression(EXP_CONSTANT, Item(T_BOOL, true)); }
Expression AstHelper::FromFalse(const ASTNode *n) { return Expression(EXP_CONSTANT, Item(T_BOOL, false)); }
Expression AstHelper::FromNull(const ASTNode *n) { return Expression(EXP_CONSTANT, Item(T_NULL)); }

Expression AstHelper::FromUnaryOperator(const ASTNode *n, const VarMap &aliasMap, const StrMap &strMap) {
    const ASTNode *arg = cypher_ast_unary_operator_get_argument(n);
    const cypher_operator_t *op = cypher_ast_unary_operator_get_operator(n);

    Expression expr;
    if (op == CYPHER_OP_UNARY_MINUS) {
        // -1 or -a.b
        expr = Expression(EXP_MUL);
        expr.EmplaceChild(EXP_CONSTANT, Item(T_INTEGER, -1));
        expr.EmplaceChild(ToExpression_(arg, aliasMap, strMap));
    } else if (op == CYPHER_OP_UNARY_PLUS) {
        // +1 or +a.b
        expr = ToExpression_(arg, aliasMap, strMap);
    } else if (op == CYPHER_OP_NOT) {
        expr = Expression(EXP_NOT);
        expr.EmplaceChild(ToExpression_(arg, aliasMap, strMap));
    } else if (op == CYPHER_OP_IS_NULL) {
        expr = Expression(EXP_EQ);
        expr.EmplaceChild(EXP_CONSTANT, Item(T_NULL));
        expr.EmplaceChild(ToExpression_(arg, aliasMap, strMap));
    } else if (op == CYPHER_OP_IS_NOT_NULL) {
        expr = Expression(EXP_NEQ);
        expr.EmplaceChild(EXP_CONSTANT, Item(T_NULL));
        expr.EmplaceChild(ToExpression_(arg, aliasMap, strMap));
    } else {
        assert(false && "unary op not support");
    }

    return expr;
}

Expression AstHelper::FromBinaryOperator(const ASTNode *n, const VarMap &aliasMap, const StrMap &strMap) {
    const cypher_operator_t *op = cypher_ast_binary_operator_get_operator(n);
    const ASTNode *lhs = cypher_ast_binary_operator_get_argument1(n),
                  *rhs = cypher_ast_binary_operator_get_argument2(n);

    Expression expr(FromBinaryCypherOperator(op));
    expr.EmplaceChild(ToExpression_(lhs, aliasMap, strMap));
    expr.EmplaceChild(ToExpression_(rhs, aliasMap, strMap));
    return expr;
}

Expression AstHelper::FromComparison(const ASTNode *n, const VarMap &varMap, const StrMap &strMap) {
    // e.g. 7 > 3 or 1 < 2 > 4
    vector<Expression> vec;

    u32 length = cypher_ast_comparison_get_length(n);
    for (u32 i = 0; i < length; i++) {
        const cypher_operator_t *op = cypher_ast_comparison_get_operator(n, i);
        const ASTNode *lhs = cypher_ast_comparison_get_argument(n, i),
                      *rhs = cypher_ast_comparison_get_argument(n, i + 1);

        Expression expr(FromBinaryCypherOperator(op));
        expr.EmplaceChild(ToExpression_(lhs, varMap, strMap));
        expr.EmplaceChild(ToExpression_(rhs, varMap, strMap));
        vec.emplace_back(std::move(expr));
    }

    return ExprUtil::CombineFilter(vec);
}

Expression AstHelper::FromPattern(const ASTNode *n, const VarMap &varMap) {
    return Expression(EXP_VARIABLE, varMap[AstHelper::GetVarName(n)]);
}

Expression AstHelper::FromPatternPath(const ASTNode *n) {
    return Expression(EXP_PATTERN_PATH, Item(T_INTEGER, reinterpret_cast<int64_t>(n)));
}

ExpType AstHelper::FromBinaryCypherOperator(const cypher_operator_t *op) {
    if (op == CYPHER_OP_OR)
        return EXP_OR;
    else if (op == CYPHER_OP_AND)
        return EXP_AND;
    else if (op == CYPHER_OP_EQUAL)
        return EXP_EQ;
    else if (op == CYPHER_OP_NEQUAL)
        return EXP_NEQ;
    else if (op == CYPHER_OP_LT)
        return EXP_LT;
    else if (op == CYPHER_OP_GT)
        return EXP_GT;
    else if (op == CYPHER_OP_LTE)
        return EXP_LE;
    else if (op == CYPHER_OP_GTE)
        return EXP_GE;
    else if (op == CYPHER_OP_PLUS)
        return EXP_ADD;
    else if (op == CYPHER_OP_MINUS)
        return EXP_SUB;
    else if (op == CYPHER_OP_MULT)
        return EXP_MUL;
    else if (op == CYPHER_OP_DIV)
        return EXP_DIV;
    else if (op == CYPHER_OP_MOD)
        return EXP_MOD;

    assert(false && "binary op not support");
    return EXP_CONSTANT;
}

Expression AstHelper::ToExpression(const ASTNode *n, const VarMap &aliasMap, const StrMap &strMap) {
    Expression expr = ToExpression_(n, aliasMap, strMap);
    expr.ChildNumCheck();
    ExprUtil::CompactCondition(expr);
    ExprUtil::ApplyDeMorgan(expr);
    ExprUtil::ReduceConstant(expr);
    SemanticChecker::checkNestedAgg(expr);
    SemanticChecker::checkVarAboveAgg(expr);
    SemanticChecker::checkOnlyConditionOverPath(expr);
    SemanticChecker::checkOnlyConditionOverCondition(expr);
    return expr;
}

Expression AstHelper::ToExpression_(const ASTNode *n, const VarMap &varMap, const StrMap &strMap) {
    assert(n != NULL);
    const ASTNodeType t = cypher_astnode_type(n);
    try {
        if (t == CYPHER_AST_APPLY_OPERATOR)
            return FromApplyOperator(n, varMap, strMap);
        else if (t == CYPHER_AST_IDENTIFIER)
            return FromIdentifier(n, varMap);
        else if (t == CYPHER_AST_PROPERTY_OPERATOR)
            return FromPropertyOperator(n, varMap, strMap);
        else if (t == CYPHER_AST_INTEGER)
            return FromInterger(n);
        else if (t == CYPHER_AST_FLOAT)
            return FromFloat(n);
        else if (t == CYPHER_AST_STRING)
            return CreateFromString(n);
        else if (t == CYPHER_AST_TRUE)
            return FromTrue(n);
        else if (t == CYPHER_AST_FALSE)
            return FromFalse(n);
        else if (t == CYPHER_AST_NULL)
            return FromNull(n);
        else if (t == CYPHER_AST_UNARY_OPERATOR)
            return FromUnaryOperator(n, varMap, strMap);
        else if (t == CYPHER_AST_BINARY_OPERATOR)
            return FromBinaryOperator(n, varMap, strMap);
        else if (t == CYPHER_AST_COMPARISON)
            return FromComparison(n, varMap, strMap);
        else if (t == CYPHER_AST_NODE_PATTERN)
            return FromPattern(n, varMap);
        else if (t == CYPHER_AST_REL_PATTERN)
            return FromPattern(n, varMap);
        else if (t == CYPHER_AST_PATTERN_PATH)
            return FromPatternPath(n);
        else
            throw PlannerException("Not supported expression type: " + std::string(cypher_astnode_typestr(t)));
    } catch (PlannerException &e) {
        throw e;
    }
    return Expression();
}
}  // namespace AGE
