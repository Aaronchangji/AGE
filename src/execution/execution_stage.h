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

#include <glog/logging.h>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/expression.h"
#include "base/serializer.h"
#include "base/type.h"
#include "execution/execution_stage_type.h"
#include "execution/message.h"
#include "model/util.h"
#include "operator/abstract_op.h"
#include "util/tool.h"

using std::pair;
using std::string;
using std::vector;

namespace AGE {

class ExecutionStage {
   public:
    ExecutionStage() {}
    ~ExecutionStage() {
        for (auto& op : ops_) delete op;
    }
    constexpr static size_t npos = -1;

    inline void appendOp(AbstractOp* op) { ops_.emplace_back(op); }
    void addNextStage(int stageId) { next_stages_.emplace_back(stageId); }
    void setPrevStage(int stageId) { prev_stage_ = stageId; }
    void setExecutionSide();

    vector<int>& getNextStages() { return next_stages_; }
    int getPrevStage() const { return prev_stage_; }
    ClusterRole getExecutionSide() const { return execution_side_; }
    OpType getOpType(int op_idx) const;
    size_t size() const { return ops_.size(); }
    bool isFinished(int cur_op_idx) const { return cur_op_idx == static_cast<int>((ops_.size() - 1)); }
    bool isLastStage() const { return next_stages_.empty(); }
    bool isEndStage() const { return ops_.at(0)->type == OpType_END; }

    // Process current op
    bool processOp(Message& m, vector<Message>& output);

    void ToString(string* s) const;
    void FromString(const string& s, size_t& pos);
    string DebugString(int stageId) const;

    void set_stage_type();
    ExecutionStageType get_stage_type() { return stage_type_; }

    ExecutionStageType stage_type_;
    vector<AbstractOp*> ops_;              // ops in this stage
    vector<int> next_stages_;              // next stages
    int prev_stage_;                       // previous stage
    ClusterRole execution_side_ = MASTER;  // which side to execute this stage
};
}  // namespace AGE
