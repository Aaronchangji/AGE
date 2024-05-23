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
#include <string>
#include "base/type.h"

namespace AGE {

using std::string;

// Operator type
typedef enum : uint16_t {
    OpType_INIT = 0,
    OpType_EXPAND = 1,
    OpType_EXPAND_INTO = 2,
    OpType_PROJECT = 3,
    OpType_VERTEX_SCAN = 4,
    OpType_FILTER = 5,
    OpType_AGGREGATE = 6,
    OpType_END = 7,
    OpType_INDEX = 8,
    OpType_INDEX_SCAN = 9,
    OpType_EXPAND_NN = 10,
    OpType_EXPAND_NC = 11,
    OpType_EXPAND_CN = 12,
    OpType_EXPAND_CC = 13,
    OpType_EXPAND_UL = 14,
    OpType_COUNT = 15,
    OpType_PROPERTY = 16,
    OpType_BRANCH_AND = 17,
    OpType_BRANCH_OR = 18,
    OpType_BRANCH_NOT = 19,
    OpType_LOOP = 20,
    OpType_OPTIONAL_MATCH = 21,
    OpType_BARRIER_PROJECT = 22,
    OpType_AGGREGATE_LOCAL = 23,
    OpType_AGGREGATE_GLOBAL = 24,
    OpType_BARRIER_PROJECT_LOCAL = 25,
    OpType_BARRIER_PROJECT_GLOBAL = 26,
    COUNT = 27
} OpType;

constexpr char *OpTypeStr[] = {(char *)"Init",
                               (char *)"Expand",
                               (char *)"ExpandInto",
                               (char *)"Project",
                               (char *)"VertexScan",
                               (char *)"Filter",
                               (char *)"Aggregation",
                               (char *)"End",
                               (char *)"Index",
                               (char *)"IndexScan",
                               (char *)"ExpandNN",
                               (char *)"ExpandNC",
                               (char *)"ExpandCN",
                               (char *)"ExpandCC",
                               (char *)"ExpandUL",
                               (char *)"Count",
                               (char *)"Property",
                               (char *)"BranchAnd",
                               (char *)"BranchOr",
                               (char *)"BranchNot",
                               (char *)"Loop",
                               (char *)"OptionalMatch",
                               (char *)"BarrierProject",
                               (char *)"AGGREGATE_LOCAL",
                               (char *)"AGGREGATE_GLOBAL",
                               (char *)"BARRIER_PROJECT_LOCAL",
                               (char *)"BARRIER_PROJECT_GLOBAL"};

inline string OpType_DebugString(OpType t) { return OpTypeStr[t]; }
inline bool OpType_IsScan(OpType t) { return (t == OpType_VERTEX_SCAN || t == OpType_INDEX_SCAN); }
inline bool OpType_IsIndex(OpType t) { return (t == OpType_INDEX || t == OpType_INDEX_SCAN); }
inline bool OpType_IsAggregate(OpType t) {
    return (t == OpType_COUNT || t == OpType_AGGREGATE || t == OpType_AGGREGATE_GLOBAL || t == OpType_AGGREGATE_LOCAL);
}
inline bool OpType_IsBarrierProject(OpType t) {
    return (t == OpType_BARRIER_PROJECT || t == OpType_BARRIER_PROJECT_GLOBAL || t == OpType_BARRIER_PROJECT_LOCAL);
}
inline bool OpType_IsExpand(OpType t) {
    return (t == OpType_EXPAND || t == OpType_EXPAND_CC || t == OpType_EXPAND_CN || t == OpType_EXPAND_INTO ||
            t == OpType_EXPAND_NC || t == OpType_EXPAND_NN || t == OpType_EXPAND_UL);
}
inline bool OpType_IsProject(OpType t) {
    return t == OpType_PROJECT || OpType_IsAggregate(t) || OpType_IsBarrierProject(t);
}
inline bool OpType_IsBranchFilter(OpType t) {
    return (t == OpType_BRANCH_AND || t == OpType_BRANCH_OR || t == OpType_BRANCH_NOT);
}
inline bool OpType_IsSubquery(OpType t) {
    return (OpType_IsBranchFilter(t) || t == OpType_LOOP || t == OpType_OPTIONAL_MATCH);
}
inline bool OpType_IsBarrier(OpType t) {
    return t == OpType_END || OpType_IsAggregate(t) || OpType_IsBarrierProject(t) || OpType_IsSubquery(t);
}
inline bool OpType_NeedRequestData(OpType t) {
    return (OpType_IsExpand(t) || t == OpType_PROPERTY || OpType_IsScan(t) || OpType_IsIndex(t));
}

inline ClusterRole OpType_GetExecutionSide(OpType t) {
    switch (t) {
    case OpType_VERTEX_SCAN:
    case OpType_EXPAND:
    case OpType_EXPAND_INTO:
    case OpType_EXPAND_NN:
    case OpType_EXPAND_NC:
    case OpType_EXPAND_CN:
    case OpType_EXPAND_CC:
    case OpType_EXPAND_UL:
    case OpType_PROPERTY:
    case OpType_INDEX:
    case OpType_INDEX_SCAN:
    case OpType_FILTER:
    case OpType_AGGREGATE_LOCAL:
    case OpType_BARRIER_PROJECT_LOCAL:
        return ClusterRole::CACHE;
    case OpType_INIT:
    case OpType_PROJECT:
    case OpType_AGGREGATE:
    case OpType_AGGREGATE_GLOBAL:
    case OpType_END:
    case OpType_COUNT:
    case OpType_BRANCH_AND:
    case OpType_BRANCH_OR:
    case OpType_BRANCH_NOT:
    case OpType_LOOP:
    case OpType_OPTIONAL_MATCH:
    case OpType_BARRIER_PROJECT:
    case OpType_BARRIER_PROJECT_GLOBAL:
        return ClusterRole::COMPUTE;
    default:
        LOG(ERROR) << "Unexpected op type: " << OpTypeStr[t];
        CHECK(false);
    }
}

}  // namespace AGE
