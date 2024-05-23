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

#include <map>
#include <utility>
#include <vector>
#include "base/expr_util.h"
#include "base/expression.h"
#include "base/type.h"
#include "execution/physical_plan.h"
#include "operator/ops.h"
#include "plan/var_map.h"

namespace AGE {

using std::map;
using std::pair;
using std::vector;

class PhysicalPlanOptimizer {
   public:
    explicit PhysicalPlanOptimizer(const VarMap &varMap) : varMap(varMap) {}

    void Optimize(PhysicalPlan &plan) {
        // ReplaceAggregateOpWithCountOpOp(plan);
    }

    void ReplaceAggregateOpWithCountOpOp(PhysicalPlan &plan) {
        for (size_t i = 0; i < plan.stages.size(); i++) {
            ExecutionStage *stage = plan.stages.at(i);
            for (u32 j = 0; j < stage->size(); j++) {
                if (stage->ops_[j]->type == OpType_AGGREGATE) {
                    ReplaceAggregateOpWithCountOpOp(stage->ops_[j]);
                }
            }
        }
    }

   private:
    bool checkCountAggOnly(const Expression &exp) const {
        if (exp.type == EXP_AGGFUNC) return exp.aggFunc.type == AGG_CNT;
        bool ret = true;
        for (const Expression &child : exp.GetChildren()) {
            ret &= checkCountAggOnly(child);
        }
        return ret;
    }

    void ReplaceAggregateOpWithCountOpOp(AbstractOp *&op) {
        AggregateOp *aggOp = dynamic_cast<AggregateOp *>(op);
        bool countOnly = true, hasCompressKey = false;
        for (u32 i = 0; i < aggOp->exps.size(); i++) {
            if (i < aggOp->nKey) {
                hasCompressKey |= aggOp->exps[i].first.HasCompressCol();
            } else {
                countOnly &= checkCountAggOnly(aggOp->exps[i].first);
            }
        }

        if (countOnly && !hasCompressKey) {
            // Replace aggOp by CountOp.
            op = new CountOp(aggOp->exps, aggOp->compressBeginIdx, aggOp->nKey, aggOp->newCompressBeginIdx);
            aggOp->exps.clear();
            delete aggOp;
        }
    }

    const VarMap &varMap;
};

}  // namespace AGE
