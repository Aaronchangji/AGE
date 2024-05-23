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

#include "operator/expand_op.h"
#include "base/serializer.h"
#include "operator/expand_cc_op.h"
#include "operator/expand_cn_op.h"
#include "operator/expand_nc_op.h"
#include "operator/expand_nn_op.h"
#include "operator/expand_ul_op.h"

namespace AGE {
ExpandOp* ExpandOp::FromPhysicalParams(const PhysicalOpParams& params) {
    ColIndex src = params.cols[0], dst = params.cols[1], compressDst = params.compressDst,
             compressBeginIdx = params.compressBeginIdx;

    size_t pos = 0;
    DirectionType dir;
    LabelId eLabel, dstVLabel;
    Serializer::readVar(params.params, pos, &dir);
    Serializer::readVar(params.params, pos, &eLabel);
    Serializer::readVar(params.params, pos, &dstVLabel);

    if (dst == COLINDEX_NONE) return new ExpandULOp(src, dst, compressDst, eLabel, dstVLabel, dir, compressBeginIdx);
    if (src == COLINDEX_COMPRESS && dst == COLINDEX_COMPRESS)
        return new ExpandCCOp(src, dst, compressDst, eLabel, dstVLabel, dir, compressBeginIdx);
    else if (src == COLINDEX_COMPRESS)
        return new ExpandCNOp(src, dst, compressDst, eLabel, dstVLabel, dir, compressBeginIdx);
    else if (dst == COLINDEX_COMPRESS)
        return new ExpandNCOp(src, dst, compressDst, eLabel, dstVLabel, dir, compressBeginIdx);
    else
        return new ExpandNNOp(src, dst, compressDst, eLabel, dstVLabel, dir, compressBeginIdx);
}
}  // namespace AGE
