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
// limitations under the License.#pragma once

#pragma once

#include <utility>
#include <vector>

#include "operator/barrier_projection_op.h"

namespace AGE {

class LocalBarrierProjectOp : public BarrierProjectOp {
   public:
    explicit LocalBarrierProjectOp(u32 compressBeginIdx = COMPRESS_BEGIN_IDX_UNDECIDED)
        : BarrierProjectOp(OpType_BARRIER_PROJECT_LOCAL, compressBeginIdx) {}
    LocalBarrierProjectOp(const vector<pair<Expression, ColIndex>> &exps, u32 compressBeginIdx,
                          u32 newCompressBeginIdx_, vector<pair<Expression, isASC>> *orderbyExps = nullptr,
                          bool hasDistinct = false, int limitNum_ = LimitUtil::WITHOUT_LIMIT)
        : BarrierProjectOp(OpType_BARRIER_PROJECT_LOCAL, exps, compressBeginIdx, newCompressBeginIdx_, orderbyExps,
                           hasDistinct, limitNum_) {}

    uint8_t get_end_history_size(Message &msg) override {
        uint8_t end_path_size = 0;
        end_path_size = msg.header.globalHistorySize;
        return end_path_size;
    }

    // void postprocess(Message &m, vector<Message> &output, bool isReady) override {
    //     if (isReady) {
    //         storageSet.getResult(m, limitNum);
    //     } else {
    //         m.data.clear();
    //     }
    //     output.emplace_back(std::move(m));
    // }
};

}  // namespace AGE
