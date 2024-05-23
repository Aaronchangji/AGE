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

#include "plan/op_params.h"
#include <glog/logging.h>
#include "base/expr_util.h"
#include "plan/col_assigner.h"

namespace AGE {

// Overall process of converting logicParam --> physicParam :
//  1. Construct Physical::compressBeginIdx. Copy Logical::params --> Physical::params
//  2. Map preVars --> ColIndex, append to Physical::cols in order
//  3. Map EXP_VARIABLE in Logical::preExprs --> ColIndex, append exprs to Physical::exprs in order
//  4. Simulate op process, call colAssigner.ProcessNextOp()
//  5. Map postVars --> ColIndex, append to Physical::cols in order
//  6. Map EXP_VARIABLE in Logical::postExprs --> ColIndex, append exprs to Physical::exprs in order
PhysicalOpParams LogicalOpParams::BuildPhysicalOpParams(ColAssigner &colAssigner) const {
    // LOG(INFO) << DebugString();
    // LOG(INFO) << "Process preVars & preExprs";
    // LOG(INFO) << colAssigner.DebugString();
    PhysicalOpParams ret;
    ret.compressBeginIdx = colAssigner.GetCompressBeginIdx();
    ret.params = params;

    // Pre-process
    VarId preCompress = colAssigner.GetCompress();
    for (VarId var : preVars) {
        ColIndex col = colAssigner.GetCol(var);
        // Input variable can not be none
        assert(col != COLINDEX_NONE);
        ret.cols.emplace_back(col);
    }
    for (const Expression &preExpr : preExprs) {
        for (Expression *const var : ExprUtil::GetTypedExpr(EXP_VARIABLE, preExpr)) {
            VarId varId = var->varId;
            ColIndex colIdx = colAssigner.GetCol(varId);
            assert(colIdx != COLINDEX_NONE);
            var->colIdx = colIdx;
        }
        ret.exprs.emplace_back(preExpr);
    }

    // Op process
    // This will modify colAssigner, transform to next status in plan execution
    ret.compressDst = colAssigner.ProcessNextOp();
    // LOG(INFO) << "Process postVars & postExprs";
    // LOG(INFO) << colAssigner.DebugString();

    // Post-process
    VarId postCompress = colAssigner.GetCompress();
    for (VarId var : postVars) {
        ColIndex col = colAssigner.GetCol(var);
        // Output variable can be none means it is useless
        // assert(col != COLINDEX_NONE);
        ret.cols.emplace_back(col);
    }
    for (const Expression &postExpr : postExprs) {
        for (Expression *const var : ExprUtil::GetTypedExpr(EXP_VARIABLE, postExpr)) {
            VarId varId = var->varId;
            ColIndex colIdx = colAssigner.GetCol(varId);
            assert(colIdx != COLINDEX_NONE);
            var->colIdx = colIdx;
        }
        ret.exprs.emplace_back(postExpr);
    }

    // Compress destination
    if (preCompress != postCompress) {
        ColIndex col = colAssigner.GetCol(preCompress);
        ret.compressDst = (col == COLINDEX_NONE) ? COLINDEX_NONE : col;
    } else {
        ret.compressDst = COLINDEX_COMPRESS;
    }

    return ret;
}
}  // namespace AGE
