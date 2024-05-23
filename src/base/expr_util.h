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

#include <glog/logging.h>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include "base/expression.h"
#include "base/serializer.h"
#include "base/type.h"

using std::vector;

namespace AGE {
using std::set;

// Expression util to implement complex functions for expression tree
class ExprUtil {
   public:
    static void ToString(string* s, const Expression* root) {
        u8 expCnt = 0;
        if (root == nullptr) {
            Serializer::appendU8(s, 0);
            return;
        }

        Serializer::appendU8(s, root->GetSize());
        ToString_(s, *root, 0, expCnt);
    }

    static Expression CreateFromString(const string& s, size_t& pos) {
        uint8_t expCnt = Serializer::readU8(s, pos);
        if (!expCnt) return Expression();

        vector<Expression> exprs(expCnt);
        vector<Expression*> ptrs(expCnt);

        for (u8 i = 0; i < exprs.size(); i++) {
            exprs[i] = Expression();

            u8 parent = Serializer::readU8(s, pos);
            Serializer::readVar(s, pos, &exprs[i].type);
            switch (exprs[i].type) {
            case EXP_PROP:
                exprs[i].key = Serializer::readU8(s, pos);
                break;
            case EXP_AGGFUNC:
                Serializer::readVar(s, pos, &exprs[i].aggFunc);
                break;
            case EXP_VARIABLE:
                exprs[i].colIdx = Serializer::readI8(s, pos);
                break;
            case EXP_CONSTANT:
                exprs[i].SetValue(Item::CreateFromString(s, pos));
                break;
            default:
                break;
            }

            if (i == 0) {
                ptrs[i] = &exprs[i];
            }
            if (parent > 0) ptrs[i] = &(ptrs[parent - 1]->EmplaceChild(exprs[i]));
        }
        return exprs[0];
    }

    // compare expression. Moved to Expression
    static bool ExpCmp(const Expression& lhs, const Expression& rhs) {
        if (lhs == rhs) return true;
        return false;
    }

    // Get referenced variable of expressions.
    static set<ColIndex> GetRefVar(const Expression& expr) {
        set<ColIndex> ret;
        GetRefVar(expr, ret);
        return ret;
    }
    static set<ColIndex> GetRefVar(const vector<Expression>& exprs) {
        set<ColIndex> ret;
        for (const Expression& expr : exprs) GetRefVar(expr, ret);
        return ret;
    }

    static Expression CompactCondition(const Expression& root) {
        // TODO(ycli): "x AND (y AND z)" --> "x AND y AND z"
        return Expression();
    }

    static Expression CombineFilter(const vector<Expression>& exps) {
        if (exps.size() == 0) {
            return Expression();
        } else if (exps.size() == 1) {
            return exps[0];
        } else {
            Expression exp = Expression(EXP_AND);
            exp.GetChildren() = exps;
            return exp;
        }
    }

    static Expression CombineFilter(Expression* lhs, Expression* rhs) {
        if (lhs == nullptr) return *rhs;
        if (rhs == nullptr) return *lhs;
        Expression exp = Expression(EXP_AND);
        exp.EmplaceChild(*lhs);
        exp.EmplaceChild(*rhs);
        return exp;
    }

    static void ReduceConstant(Expression& exp, bool isRoot = true) {
        exp.Eval();
        _ReduceConstant(exp);
    }

    static void ApplyDeMorgan(Expression& root, bool neg = false) {
        ExpType type = root.type;
        if (type == EXP_NOT) {
            ApplyDeMorgan(root.GetChild(0), !neg);
            Expression child(root.GetChild(0));
            root = child;
        } else {
            negate(root, neg);
            for (Expression& child : root.GetChildren()) {
                ApplyDeMorgan(child, neg);
            }
        }
    }

    static vector<Expression> DivideFilter(Expression& exp) {
        vector<Expression> subExps;
        DivideFilter(exp, subExps);
        return subExps;
    }

    static void DivideFilter(Expression& exp, vector<Expression>& subExps) {
        if (exp.type == EXP_AND) {
            for (Expression& child : exp.GetChildren()) {
                DivideFilter(child, subExps);
            }
            exp.RemoveChildren();
        } else {
            subExps.emplace_back(std::move(exp));
        }
    }

    bool HasTypedExprNode(ExpType type, const Expression& expr) {
        if (type == expr.type) return true;
        for (const Expression& child : expr.GetChildren()) {
            if (HasTypedExprNode(type, child)) return true;
        }
        return false;
    }

    static vector<Expression*> GetTypedExpr(ExpType type, const vector<Expression>& exprs) {
        vector<Expression*> ret;
        for (const Expression& expr : exprs) GetTypedExpr(type, expr, ret);
        return ret;
    }

    static vector<Expression*> GetTypedExpr(ExpType type, const Expression& expr) {
        vector<Expression*> ret;
        GetTypedExpr(type, expr, ret);
        return ret;
    }

    static void GetTypedExpr(ExpType type, const Expression& n, vector<Expression*>& result) {
        if (type == n.type) result.emplace_back(const_cast<Expression*>(&n));
        for (const Expression& child : n.GetChildren()) {
            GetTypedExpr(type, child, result);
        }
    }

   private:
    static void ToString_(string* s, const Expression& cur, u8 parent, u8& expCnt) {
        u8 id = ++expCnt;

        Serializer::appendU8(s, parent);
        Serializer::appendVar(s, cur.type);
        switch (cur.type) {
        case EXP_PROP:
            Serializer::appendU8(s, cur.key);
            break;
        case EXP_AGGFUNC:
            Serializer::appendVar(s, cur.aggFunc);
            break;
        case EXP_VARIABLE:
            Serializer::appendI8(s, cur.colIdx);
            break;
        case EXP_CONSTANT:
            cur.GetValue().ToString(s);
            break;
        default:
            break;
        }

        for (const Expression& exp : cur.GetChildren()) {
            ToString_(s, exp, id, expCnt);
        }
    }

    static void GetRefVar(const Expression& exp, set<ColIndex>& st) {
        if (exp.type == EXP_VARIABLE) st.insert(exp.colIdx);
        for (const Expression& child : exp.GetChildren()) {
            GetRefVar(child, st);
        }
    }

    static void negate(Expression& root, bool& neg) {
        ExpType& type = root.type;
        switch (type) {
#define _negate_type(neg, type, x, y)                               \
    {                                                               \
        if (neg) (type) = static_cast<ExpType>((type) ^ (x) ^ (y)); \
    }
        case EXP_AND:
        case EXP_OR:
            _negate_type(neg, type, EXP_AND, EXP_OR);
            break;
        case EXP_EQ:
        case EXP_NEQ:
            _negate_type(neg, type, EXP_EQ, EXP_NEQ);
            neg = false;
            break;
        case EXP_GT:
        case EXP_LE:
            _negate_type(neg, type, EXP_GT, EXP_LE);
            neg = false;
            break;
        case EXP_LT:
        case EXP_GE:
            _negate_type(neg, type, EXP_LT, EXP_GE);
            neg = false;
            break;
#undef _negate_type
        case EXP_ADD:
        case EXP_SUB:
        case EXP_MUL:
        case EXP_DIV:
        case EXP_MOD:
        case EXP_CONSTANT:
        case EXP_PROP:
        case EXP_VARIABLE:
        case EXP_PATTERN_PATH:
        case EXP_AGGFUNC:
            if (neg) {
                Expression exp(EXP_NOT);
                exp.EmplaceChild(root);
                root = exp;
                neg = false;
            }
            break;
        default:
            assert(false);
        }
    }

    static void _ReduceConstant(Expression& exp) {
        if (!exp.IsConstant() && exp.GetValue().type != T_UNKNOWN) {
            Item value = exp.GetValue();
            exp.RemoveChildren();
            exp = Expression(EXP_CONSTANT, std::move(value));
        } else {
            for (Expression& child : exp.GetChildren()) {
                _ReduceConstant(child);
            }
        }
    }
};
}  // namespace AGE
