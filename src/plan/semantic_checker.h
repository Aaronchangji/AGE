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
#include "base/expression.h"
#include "execution/physical_plan.h"
#include "plan/planner_exception.h"

namespace AGE {

// Check semantic correctness (e.g. correct syntax but wrong logic)
class SemanticChecker {
   public:
    // e.g. "count(5 + max(a.age))" is invalid
    static void checkNestedAgg(const Expression& exp) {
        if (!checkNestedAgg_(exp)) throw PlannerException("Nested Aggregation fault");
    }

    // e.g. "a + max(a.age)" is invalid
    static void checkVarAboveAgg(const Expression& exp) {
        if (hasAgg(exp) && !checkVarAboveAgg_(exp)) {
            // printf("%s", exp->DebugString().c_str());
            throw PlannerException("Variable over aggregation fault");
        }
    }

    // "e.g. 5 + (a.age > 5 AND b.age < 5)" is fault
    static void checkOnlyConditionOverCondition(const Expression& expr) {
        for (const Expression& child : expr.GetChildren()) {
            if (!expr.IsCondition() && child.IsCondition()) {
                throw PlannerException("Put arithmetic expression above conditional expression is not allowed");
            }
        }
    }

    // e.g. "5 + (a)--(b)" is invalid
    //      "5 > (a)--(b)" is invalid
    static void checkOnlyConditionOverPath(const Expression& expr) {
        for (const Expression& child : expr.GetChildren()) {
            if (!expr.IsCondition() && child.type == EXP_PATTERN_PATH) {
                throw PlannerException("Pattern Path can only appear with condition expression (AND, OR, NOT)");
            }
            checkOnlyConditionOverPath(child);
        }
    }

   private:
    static bool hasAgg(const Expression& exp) {
        if (exp.type == EXP_AGGFUNC) return true;
        bool ret = false;
        for (const Expression& child : exp.GetChildren()) ret |= hasAgg(child);
        return ret;
    }

    static bool checkVarAboveAgg_(const Expression& exp) {
        if (exp.type == EXP_AGGFUNC)
            return true;
        else if (exp.type == EXP_VARIABLE)
            return false;
        bool ret = true;
        for (const Expression& child : exp.GetChildren()) ret &= checkVarAboveAgg_(child);
        return ret;
    }

    static bool checkNestedAgg_(const Expression& exp, bool hasAggAncestor = false) {
        bool ret = true;
        if (exp.type == EXP_AGGFUNC) {
            if (hasAggAncestor) {
                return false;
            } else {
                hasAggAncestor = true;
            }
        }

        for (const Expression& child : exp.GetChildren()) ret &= checkNestedAgg_(child, hasAggAncestor);
        return ret;
    }
};
}  // namespace AGE
