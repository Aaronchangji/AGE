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

#include <common.pb.h>
#include <google/protobuf/util/json_util.h>
#include <gremlin.pb.h>
#include <grpcpp/grpcpp.h>
#include <job_service.grpc.pb.h>
#include <cstdio>
#include <iostream>
#include <string>
#include <unordered_set>
#include <utility>
#include "base/expression.h"
#include "execution/physical_plan.h"
#include "gremlin/step_translator.h"
#include "operator/ops.h"
#include "operator/vertex_scan_op.h"
#include "plan/logical_plan.h"
#include "plan/planner.h"
#include "plan/var_map.h"
#include "storage/layout.h"

namespace AGE {
class PlanChecker {
   public:
    PlanChecker() {}

    // true for pass.
    // false for error.
    bool check(const LogicalPlan& logicalPlan) {
        bool ret = true;
        ret &= checkSingleAggregation(logicalPlan);
        return ret;
    }

   private:
    bool isAgg(const LogicalOp& logicalOp) {
        OpType type = logicalOp.type;
        return type == OpType_AGGREGATE || type == OpType_COUNT;
    }

    // Ensure only 1 aggregation in gremlin query.
    bool checkSingleAggregation(const LogicalPlan& logicalPlan) {
        bool hasAgg = false;
        for (u32 i = 0; i < logicalPlan.size(); i++) {
            if (hasAgg && isAgg(logicalPlan[i])) return false;
            hasAgg |= isAgg(logicalPlan[i]);
        }
        return true;
    }
};
}  // namespace AGE
