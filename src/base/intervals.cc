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

#include "base/intervals.h"
#include <utility>
#include "base/expression.h"

namespace AGE {

Intervals::Intervals(const Expression &expr) {
    assert(expr.IsVarConstantCmpPredicate());
    const Expression *lhs = &expr.GetChild(0), *rhs = &expr.GetChild(1);
    if (lhs->type != EXP_VARIABLE) std::swap(lhs, rhs);

    const Item &v = rhs->GetValue();
    switch (expr.type) {
    case EXP_NEQ:
        intervals.emplace(LBoundary(), RBoundary(v, false));
        intervals.emplace(LBoundary(v, false), RBoundary());
        break;
    case EXP_EQ:
        intervals.emplace(LBoundary(v, true), RBoundary(v, true));
        break;
    case EXP_LT:
        intervals.emplace(LBoundary(), RBoundary(v, false));
        break;
    case EXP_LE:
        intervals.emplace(LBoundary(), RBoundary(v, true));
        break;
    case EXP_GT:
        intervals.emplace(LBoundary(v, false), RBoundary());
        break;
    case EXP_GE:
        intervals.emplace(LBoundary(v, true), RBoundary());
        break;
    default:
        assert(false);
    }
}

}  // namespace AGE
