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

#include <memory>

#include "operator/abstract_op.h"
#include "operator/aggregate_global_op.h"
#include "operator/aggregate_local_op.h"
#include "operator/aggregate_op.h"
#include "operator/barrier_projection_global_op.h"
#include "operator/barrier_projection_local_op.h"
#include "operator/barrier_projection_op.h"
#include "operator/branch_filter_entry_op.h"
#include "operator/count_op.h"
#include "operator/end_op.h"
#include "operator/expand_cc_op.h"
#include "operator/expand_cn_op.h"
#include "operator/expand_into_op.h"
#include "operator/expand_nc_op.h"
#include "operator/expand_nn_op.h"
#include "operator/expand_op.h"
#include "operator/expand_ul_op.h"
#include "operator/filter_op.h"
#include "operator/index_op.h"
#include "operator/index_scan_op.h"
#include "operator/init_op.h"
#include "operator/loop_op.h"
#include "operator/op_type.h"
#include "operator/optional_match_op.h"
#include "operator/project_op.h"
#include "operator/property_op.h"
#include "operator/vertex_scan_op.h"

namespace AGE {

class Ops {
   public:
    static AbstractOp* CreateOp(OpType type) {
        switch (type) {
        case OpType_AGGREGATE:
            return new AggregateOp(OpType_AGGREGATE);
        case OpType_AGGREGATE_GLOBAL:
            return new GlobalAggregateOp(OpType_AGGREGATE_GLOBAL);
        case OpType_AGGREGATE_LOCAL:
            return new LocalAggregateOp(OpType_AGGREGATE_LOCAL);
        case OpType_COUNT:
            return new CountOp();
        case OpType_END:
            return new EndOp();
        case OpType_EXPAND:
            CHECK(false && "CreateOp(OpType_EXPAND))");
            // return new ExpandOp();
        case OpType_EXPAND_INTO:
            return new ExpandIntoOp();
        case OpType_EXPAND_NN:
            return new ExpandNNOp();
        case OpType_EXPAND_NC:
            return new ExpandNCOp();
        case OpType_EXPAND_CN:
            return new ExpandCNOp();
        case OpType_EXPAND_CC:
            return new ExpandCCOp();
        case OpType_EXPAND_UL:
            return new ExpandULOp();
        case OpType_FILTER:
            return new FilterOp();
        case OpType_INDEX:
            return new IndexOp();
        case OpType_INDEX_SCAN:
            return new IndexScanOp();
        case OpType_INIT:
            return new InitOp();
        case OpType_PROJECT:
            return new ProjectOp();
        case OpType_BARRIER_PROJECT:
            return new BarrierProjectOp(OpType_BARRIER_PROJECT);
        case OpType_BARRIER_PROJECT_LOCAL:
            return new LocalBarrierProjectOp();
        case OpType_BARRIER_PROJECT_GLOBAL:
            return new GlobalBarrierProjectOp();
        case OpType_VERTEX_SCAN:
            return new VertexScanOp();
        case OpType_PROPERTY:
            return new PropertyOp();
        case OpType_BRANCH_AND:
        case OpType_BRANCH_OR:
        case OpType_BRANCH_NOT:
            return new BranchFilterEntryOp(type);
        case OpType_LOOP:
            return new LoopOp();
        case OpType_OPTIONAL_MATCH:
            return new OptionalMatchOp();
        default:
            CHECK(false) << "unrecognized op type:" << OpTypeStr[type];
            break;
        }
        return nullptr;
    }
};

}  // namespace AGE
