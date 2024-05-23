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

#include <glog/logging.h>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "base/type.h"
#include "operator/abstract_op.h"
#include "operator/base_barrier_op.h"
#include "plan/op_params.h"

namespace AGE {
class EndOp : public BaseBarrierOp {
   public:
    explicit EndOp(int compressBeginIdx = COMPRESS_BEGIN_IDX_UNDECIDED) : BaseBarrierOp(OpType_END, compressBeginIdx) {}

   private:
    std::vector<Row> result;

    bool do_work(Message &msg, std::vector<Message> &output, bool isReady) {
        Tool::VecMoveAppend(msg.data, result);

        if (isReady) {
            msg.data.swap(result);
            output.emplace_back(std::move(msg));
            return true;
        }

        return false;
    }
};
}  // namespace AGE
