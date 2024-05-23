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

#include <set>
#include <string>
#include <vector>
#include "base/expression.h"
#include "base/type.h"
#include "plan/type.h"

namespace AGE {
using std::string;
using std::vector;

class ColAssigner;

struct PhysicalOpParams {
   public:
    vector<ColIndex> cols;
    vector<Expression> exprs;
    string params;
    ColIndex compressDst;
    u32 compressBeginIdx;
    string DebugString() const {
        string ret = "";
        return ret;
    }
};

class LogicalOpParams {
   public:
    // Variables params exist before this op is processed
    vector<VarId> preVars;

    // Variables params exist after this op is processed
    vector<VarId> postVars;

    // Expression related to preVars
    vector<Expression> preExprs;

    // Expression related to postVars
    vector<Expression> postExprs;
    string params;

    // For now only effective on subquery_entry_op
    set<VarId> preservedVarSet;
    VarId LoopExpandSrc = kVarIdNone, LoopExpandDst = kVarIdNone;

    // Overall process of converting logicParam --> physicParam :
    //  1. Construct Physical::compressBeginIdx. Copy Logical::params --> Physical::params
    //  2. Map preVars --> ColIndex, append to Physical::cols in order
    //  3. Map EXP_VARIABLE in Logical::preExprs --> ColIndex, append exprs to Physical::exprs in order
    //  4. Simulate op process, call colAssigner.ProcessNextOp()
    //  5. Map postVars --> ColIndex, append to Physical::cols in order
    //  6. Map EXP_VARIABLE in Logical::postExprs --> ColIndex, append exprs to Physical::exprs in order
    PhysicalOpParams BuildPhysicalOpParams(ColAssigner& colAssigner) const;

    string DebugString() const {
        string ret = "preVars: {";
        for (VarId var : preVars) ret += std::to_string(var) + ", ";
        ret += "}, postVars: {";
        for (VarId var : postVars) ret += std::to_string(var) + ", ";
        ret += "}\n";
        // exprs
        ret += "Pre Expressions:\n";
        for (Expression expr : preExprs) ret += expr.DebugString() + "\n";
        ret += "Post Expressions:\n";
        for (Expression expr : postExprs) ret += expr.DebugString() + "\n";
        ret += "Params: " + params + "\n";

        return ret;
    }
};

}  // namespace AGE
