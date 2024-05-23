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

#include <utility>
#include <vector>
#include "base/expr_util.h"
#include "base/expression.h"
#include "base/intervals.h"
#include "base/type.h"
#include "plan/type.h"
#include "plan/var_map.h"
#include "storage/layout.h"

namespace AGE {

using std::make_pair;
using std::vector;

class IntervalsExprs {
   public:
    Intervals intervals;
    vector<Expression> referencedExprs;

    // IntervalsExprs() {}
    IntervalsExprs(Intervals &&intervals, const vector<Expression> &referencedExpr_)
        : intervals(intervals), referencedExprs(referencedExpr_) {}
    IntervalsExprs(const Intervals &intervals, const vector<Expression> &referencedExpr_)
        : intervals(intervals), referencedExprs(referencedExpr_) {}

    void Merge(const IntervalsExprs &rhs) {
        intervals &= rhs.intervals;
        referencedExprs.insert(referencedExprs.end(), rhs.referencedExprs.begin(), rhs.referencedExprs.end());
    }
};

typedef map<VarId, map<PropId, IntervalsExprs>> VarPropIntervals;

class VarPropIntervalsBuilder {
   public:
    VarPropIntervalsBuilder(const VarMap &varMap, const StrMap &strMap) : varMap(varMap), strMap(strMap) {}

    VarPropIntervals Build(const vector<Expression> &filters) {
        VarPropIntervals ret;
        for (const Expression &filter : filters) BuildVarPropIntervals(filter, ret);
        return ret;
    }

   private:
    // Return true if {expr} is like:
    //  - n.age > 5
    //  - n.name = "Juan"
    bool IsPropComparison(const Expression &expr) {
        if (!expr.IsPredicate()) return false;
        const Expression *lhs = &expr.GetChild(0), *rhs = &expr.GetChild(1);
        if (lhs->type != EXP_VARIABLE) std::swap(lhs, rhs);
        if (lhs->type != EXP_VARIABLE || !varMap.IsPropVarId(lhs->varId)) return false;
        if (rhs->type != EXP_CONSTANT) return false;
        return true;
    }

    typedef map<Expression, bool> ExprBoolMap;

    // Return true if subtree of {expr} only contain:
    //  - condition(AND, OR, NOT)
    //  - property variable comparison
    bool OnlyPropComparisonInSubtree(const Expression &expr, ExprBoolMap *exprBoolMap = nullptr) {
        if (exprBoolMap && exprBoolMap->count(expr)) return exprBoolMap->at(expr);
        bool ret = true;
        if (expr.IsCondition()) {
            for (const Expression &child : expr.GetChildren()) ret &= OnlyPropComparisonInSubtree(child, exprBoolMap);
        } else if (expr.IsPredicate()) {
            ret &= IsPropComparison(expr);
        } else {
            // Value expression
            ret = false;
        }
        if (exprBoolMap) exprBoolMap->emplace(expr, ret);
        return ret;
    }

    // Return true if subtree only reference one property variable
    bool OnlyOnePropVarReferencedInSubtree(const Expression &expr) {
        VarId referencedVar = kVarIdNone;
        for (const Expression *const varExpr : ExprUtil::GetTypedExpr(EXP_VARIABLE, expr)) {
            if (referencedVar == kVarIdNone)
                referencedVar = varExpr->varId;
            else if (referencedVar != varExpr->varId)
                return false;
        }
        return varMap.IsPropVarId(referencedVar);
    }

    /// @brief Recursively build {varIntervals} from Expression
    void BuildVarPropIntervals(const Expression &expr, VarPropIntervals &varPropIntervals) {
        // We required the expr has been process by ExprUtil::DivideFilter()
        // Currently we do this in PlanGraphOptimizer::DivideFilters()
        assert(expr.type != EXP_AND);

        if (!OnlyOnePropVarReferencedInSubtree(expr) || !OnlyPropComparisonInSubtree(expr)) return;

        auto [var, propIntervals] = BuildPropIntervals(expr);
        // map::operator[] will automatically insert if key is no-exist
        map<PropId, IntervalsExprs> &propIntervalsMap = varPropIntervals[var];
        auto propItr = propIntervalsMap.find(propIntervals.first);
        if (propItr == propIntervalsMap.end())
            propIntervalsMap.emplace(std::move(propIntervals));
        else
            propItr->second.Merge(propIntervals.second);
    }

    pair<VarId, pair<PropId, IntervalsExprs>> BuildPropIntervals(const Expression &expr) {
        assert(IsPropComparison(expr) || expr.IsCondition());
        // PropCmp
        if (IsPropComparison(expr)) {
            const Expression *lhs = &expr.GetChild(0), *rhs = &expr.GetChild(1);
            if (lhs->type != EXP_VARIABLE) std::swap(lhs, rhs);

            // Check type
            auto [varId, propId] = varMap.ParsePropVarId(lhs->varId);
            Intervals intervals(expr);
            if (intervals.Type() != T_UNKNOWN && intervals.Type() != strMap.GetPropType(propId)) {
                // Set to empty intervals
                intervals = Intervals();
            }

            return make_pair(varId, make_pair(propId, IntervalsExprs(std::move(intervals), {expr})));
        }

        // Condition
        assert(expr.GetChildren().size() > 0ull);
        auto [varId, propIntervals] = BuildPropIntervals(expr.GetChild(0));
        Intervals &intervals = propIntervals.second.intervals;

        if (expr.type == EXP_NOT) {
            assert(expr.GetChildren().size() == 1ull);
            return make_pair(varId, make_pair(propIntervals.first, IntervalsExprs(~intervals, {expr})));
        }

        for (size_t i = 1; i < expr.GetChildren().size(); i++) {
            auto [newVarId, newPropIntervals] = BuildPropIntervals(expr.GetChild(i));
            assert(newVarId == varId && propIntervals.first == newPropIntervals.first);

            // EXP_AND or EXP_OR
            Intervals &newIntervals = newPropIntervals.second.intervals;
            expr.type == EXP_AND ? intervals &= newIntervals : intervals |= newIntervals;
        }

        propIntervals.second.referencedExprs.clear();
        propIntervals.second.referencedExprs.emplace_back(expr);
        return make_pair(varId, std::move(propIntervals));
    }

    const VarMap &varMap;
    const StrMap &strMap;
};

}  // namespace AGE
