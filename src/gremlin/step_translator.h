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
#include "gremlin/filter_translator.h"
#include "operator/ops.h"
#include "operator/vertex_scan_op.h"
#include "plan/logical_plan.h"
#include "plan/var_map.h"
#include "storage/layout.h"

namespace AGE {

class StepTranslator {
   public:
    explicit StepTranslator(const StrMap* strMap) : filterTranslator(strMap) {}
    // Return next current tag.
    int translateGremlinStep(LogicalPlan* plan, int curTag, const gremlin::GremlinStep& gremlinStep) {
        if (gremlinStep.has_graph_step()) {
        } else if (gremlinStep.has_vertex_step()) {
            return translateVertexStep(plan, curTag, gremlinStep);
        } else if (gremlinStep.has_vertex_step()) {
        } else if (gremlinStep.has_has_step_()) {
            return translateHasStep(plan, curTag, gremlinStep);
        } else if (gremlinStep.has_where_step()) {
        } else if (gremlinStep.has_path_filter_step()) {
        } else if (gremlinStep.has_range_global_step()) {
        } else if (gremlinStep.has_path_step()) {
        } else if (gremlinStep.has_select_step()) {
        } else if (gremlinStep.has_identity_step()) {
        } else if (gremlinStep.has_order_by_step()) {
        } else if (gremlinStep.has_group_by_step()) {
        } else if (gremlinStep.has_select_one_without_by()) {
        } else if (gremlinStep.has_path_local_count_step()) {
        } else if (gremlinStep.has_properties_step()) {
        } else if (gremlinStep.has_edge_vertex_step()) {
        } else if (gremlinStep.has_dedup_step()) {
        } else if (gremlinStep.has_edge_both_v_step()) {
        } else if (gremlinStep.has_is_step()) {
        } else if (gremlinStep.has_transform_traverser_step()) {
        }
        return curTag;
    }

   private:
    int translateHasStep(LogicalPlan* plan, int curTag, const gremlin::GremlinStep& gremlinStep) {
        int outTag = getTag(gremlinStep, curTag);
        const gremlin::HasStep& hasStep = gremlinStep.has_step_();

        Expression* filter = filterTranslator.translateFilterChain(hasStep.predicates(), curTag);
        plan->appendOp(OpType_FILTER, reinterpret_cast<void*>(filter));
        plan->ops.back().input.insert(curTag);
        if (outTag != curTag) plan->ops.back().output.insert(outTag);

        return outTag;
    }

    int translateVertexStep(LogicalPlan* plan, int curTag, const gremlin::GremlinStep& gremlinStep) {
        printf("Vertex step\n");
        const gremlin::VertexStep& vertexStep = gremlinStep.vertex_step();
        const gremlin::Direction& dir = vertexStep.direction();
        const gremlin::QueryParams& queryParams = vertexStep.query_params();
        int outTag = getTag(gremlinStep, -plan->size() - 1);

        ExpandOp::Params* params = new ExpandOp::Params();
        params->src = curTag;
        params->dst = outTag;
        params->dir = dir == gremlin::OUT  ? DirectionType_OUT
                      : dir == gremlin::IN ? DirectionType_IN
                                           : DirectionType_BOTH;
        printf("before label\n");
        params->eLabel = queryParams.labels().labels_size() == 0 ? ALL_LABEL : queryParams.labels().labels(0);
        printf("after label\n");
        plan->appendOp(OpType_EXPAND, reinterpret_cast<void*>(params));
        plan->ops.back().input.insert(curTag);
        plan->ops.back().output.insert(outTag);
        return outTag;
    }

    inline int getTag(const gremlin::GremlinStep& gremlinStep, int defaultValue) {
        // Only consider first tag.
        return gremlinStep.tags_size() == 0 ? defaultValue : gremlinStep.tags(0).tag();
    }

    FilterTranslator filterTranslator;
};
}  // namespace AGE
