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

#include "execution/execution_stage.h"
#include "execution/message_header.h"
#include "operator/op_type.h"
#include "operator/ops.h"

namespace AGE {

void ExecutionStage::setExecutionSide() {
    CHECK_NE(ops_.size(), static_cast<size_t>(0));
    if (OpType_NeedRequestData(ops_[0]->type)) {
        execution_side_ = ClusterRole::CACHE;
    } else {
        execution_side_ = ClusterRole::COMPUTE;
    }
}

OpType ExecutionStage::getOpType(int idx) const {
    CHECK(idx < static_cast<int>(ops_.size())) << "Invalid op index: " << idx << " with size " << ops_.size();
    return ops_[idx]->type;
}

bool ExecutionStage::processOp(Message& m, vector<Message>& output) {
    CHECK(m.header.currentOpIdx < ops_.size()) << "Invalid op index";
    bool is_last_op = (m.header.currentOpIdx == ops_.size() - 1);
    OpType op_type = getOpType(m.header.currentOpIdx);

    // inner processing
    bool return_val = false;
    if (!m.plan->is_index_query() && Config::GetInstance()->op_profiler_) {
        return_val = ops_[m.header.currentOpIdx]->processWithProfiling(m, output, execution_side_);
    } else {
        return_val = ops_[m.header.currentOpIdx]->process(m, output);  // This line might move m to output
    }

    // Move op forward
    if (OpType_IsSubquery(op_type)) {  // SubQuery Op should be treated differently
        for (size_t i = 0; i < output.size(); i++) {
            Message& opt = output[i];
            switch (opt.header.type) {
            case MessageType::SPAWN:
                // into subquery: Optional Match & Loop guarantee to place SPAWN message at i == 0
                opt.header.forward(is_last_op, next_stages_[i]);
                break;
            case MessageType::SUBQUERY:
                // output from subquery (except loop)
                opt.header.forward(is_last_op, next_stages_.back());
                opt.header.type = MessageType::SPAWN;
                break;
            case MessageType::LOOPEND:
                // output from loop
                opt.header.forward(is_last_op, next_stages_.back());
                if (!opt.execution_strategy.get_is_dfs()) opt.header.type = MessageType::SPAWN;
                break;
            default:
                LOG(ERROR) << "Non-handled message type: " << opt.header.type << " during forwarding";
            }
        }
        return return_val;
    }

    if (op_type != OpType_END) {  // END op don't forward
        // normal op forward
        // LOG(INFO) << output.size() << std::flush;
        for (auto& opt : output) {
            if (next_stages_.size()) {
                opt.header.forward(is_last_op, next_stages_[0]);
            } else {
                opt.header.forward();
            }
        }
    }
    return return_val;
}

void ExecutionStage::ToString(string* s) const {
    Serializer::appendU16(s, ops_.size());
    for (const auto& op : ops_) {
        op->appendType(s);
        op->ToString(s);
    }
    Serializer::appendU16(s, next_stages_.size());
    for (const auto& stage : next_stages_) {
        Serializer::appendU16(s, stage);
    }
    Serializer::appendU8(s, static_cast<u8>(execution_side_));
}

void ExecutionStage::FromString(const string& s, size_t& pos) {
    uint16_t size = Serializer::readU16(s, pos);
    ops_.resize(size);

    for (size_t i = 0; i < size; i++) {
        OpType type;
        Serializer::readVar(s, pos, &type);
        ops_[i] = std::move(Ops::CreateOp(type));
        ops_[i]->FromString(s, pos);
    }

    size = Serializer::readU16(s, pos);
    next_stages_.resize(size);
    for (auto& stage : next_stages_) {
        stage = Serializer::readU16(s, pos);
    }
    execution_side_ = static_cast<ClusterRole>(Serializer::readU8(s, pos));
}

string ExecutionStage::DebugString(int stageId) const {
    string ret = "Stage " + std::to_string(stageId) + "\nOps: \n";
    for (auto& op : ops_) {
        ret += "\t" + op->DebugString() + "\n";
    }
    ret += stage_type_.DebugString() + "\n";
    ret += "\nNext Stages: [";
    for (auto& stage : next_stages_) {
        ret += std::to_string(stage) + " ";
    }
    ret += "]\nPrev Stage: " + std::to_string(prev_stage_) + "\n";
    ret += "Running on " + RoleString(execution_side_) + " cluster\n";
    return ret;
}

void ExecutionStage::set_stage_type() {
    for (auto op : ops_) {
        MLModel::OpModelType type = MLModel::OpType2OpModelType(op->type);
        if (type == MLModel::OpModelType_USELESS) {
            continue;
        }
        stage_type_.set_single(MLModel::OpType2OpModelType(op->type));
    }
}

}  // namespace AGE
