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

#include <cstdint>

#include <iostream>
#include <string>
#include "base/graph_entity.h"

using std::string;

namespace AGE {
typedef enum : uint8_t { DirectionType_BOTH = 0, DirectionType_IN = 1, DirectionType_OUT = 2 } DirectionType;
constexpr char *DirTypeStr[] = {(char *)"Both", (char *)"In", (char *)"Out"};
inline string DirectionType_DebugString(DirectionType type) { return DirTypeStr[type]; }
inline DirectionType DirectionType_Reverse(DirectionType type) {
    if (type == DirectionType_BOTH) return type;
    return static_cast<DirectionType>(type ^ DirectionType_IN ^ DirectionType_OUT);
}

using i8 = int8_t;
using i16 = int16_t;
using i32 = int32_t;
using i64 = int64_t;
using u8 = uint8_t;
using u16 = uint16_t;
using u32 = uint32_t;
using u64 = uint64_t;
using f32 = float;
using f64 = double;
using QueryId = uint32_t;

enum ClusterRole : uint8_t { MASTER = 0, COMPUTE = 1, CACHE = 2 };
constexpr int INVALID_RANK = -1;
inline char *ClusterRoleString[] = {(char *)"Master", (char *)"Compute", (char *)"Cache"};
inline string RoleString(ClusterRole role) { return ClusterRoleString[role]; }

enum SchedulerPolicy : uint8_t { ROUNDROBIN = 0, LEASTLOAD = 1, POOLING = 2, INVALID = 255 };
inline char *SchedulerPolicyString[] = {(char *)"RoundRobin", (char *)"LeastLoad", (char *)"Pooling", (char *)INVALID};
inline string SchedulerPolicy_DebugString(SchedulerPolicy policy) { return SchedulerPolicyString[policy]; }
inline SchedulerPolicy ParseSchedulerPolicy(string &policy) {
    if (policy == "RoundRobin") {
        return SchedulerPolicy::ROUNDROBIN;
    } else if (policy == "LeastLoad") {
        return SchedulerPolicy::LEASTLOAD;
    } else if (policy == "Pooling") {
        return SchedulerPolicy::POOLING;
    } else {
        return SchedulerPolicy::INVALID;
    }
}

#ifdef _WIN32  // For vscode.

#define TEST(x, y) void func_##x_##y()
#define EXPECT_FALSE(x) assert(!(x))
#define EXPECT_TRUE(x) assert((x))
#define EXPECT_EQ(x, y) assert((x) == (y))
#define EXPECT_NE(x, y) assert((x) != (y))
#define EXPECT_DEATH(x, y)

#endif  // _WIN32

string Edge_DebugString(LabelId lid, DirectionType dir);
}  // namespace AGE
