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
#include "plan/var_map.h"
#include "storage/layout.h"

namespace AGE {
class OpTranslator {
   public:
    explicit OpTranslator(const StrMap* strMap) : stepTranslator(strMap) {}
    // Return next current tag.
    int translateOpDef(LogicalPlan* plan, int curTag, const protocol::OperatorDef& opDef) {
        if (opDef.has_comm()) {
        } else if (opDef.has_map()) {
        } else if (opDef.has_flat_map()) {
            printf("OpTranslator::flat_map()\n");
            return translateFlatMap(plan, curTag, opDef.flat_map());
        } else if (opDef.has_filter()) {
            return translateFilter(plan, curTag, opDef.filter());
        } else if (opDef.has_limit()) {
        } else if (opDef.has_order()) {
        } else if (opDef.has_fold()) {
            printf("fold\n");
            const protocol::Fold& fold = opDef.fold();
            const protocol::AccumKind& accumKind = fold.accum();
            int outTag = -plan->size() - 1;
            if (accumKind == protocol::CNT) {
                plan->appendOp(OpType_COUNT);
                printf("op: %s\n", plan->ops.back().DebugString().c_str());
                plan->ops.back().input.insert(curTag);
                plan->ops.back().output.insert(outTag);
                printf("op: %s\n", plan->ops.back().DebugString().c_str());
            }
            return outTag;
        } else if (opDef.has_group()) {
        } else if (opDef.has_union_()) {
        } else if (opDef.has_iterate()) {
        } else if (opDef.has_subtask()) {
        } else if (opDef.has_dedup()) {
        }

        return curTag;
    }

   private:
    int translateFlatMap(LogicalPlan* plan, int curTag, const protocol::FlatMap& flatMap) {
        gremlin::GremlinStep gremlinStep;
        gremlinStep.ParseFromString(flatMap.resource());
        return stepTranslator.translateGremlinStep(plan, curTag, gremlinStep);
    }

    int translateFilter(LogicalPlan* plan, int curTag, const protocol::Filter& filter) {
        gremlin::GremlinStep gremlinStep;
        gremlinStep.ParseFromString(filter.resource());
        return stepTranslator.translateGremlinStep(plan, curTag, gremlinStep);
    }
    StepTranslator stepTranslator;
};

}  // namespace AGE
