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

#include "execution/message.h"
#include <utility>
#include "base/node.h"
#include "execution/physical_plan.h"

namespace AGE {
u32 Message::GetCompressBeginIdx(ClusterRole role) const {
    CHECK(plan) << "plan can't be null";
    if (role == ClusterRole::COMPUTE) {
        // LOG(INFO) << "GetCompressBeginIdx: " << to_string(header.currentStageIdx) << " " <<
        // to_string(header.currentOpIdx) << std::flush;
        CHECK(header.currentStageIdx < plan->stages.size())
            << to_string(header.currentStageIdx) << " " << plan->stages.size() << std::flush;
        CHECK(header.currentOpIdx < plan->stages[header.currentStageIdx]->ops_.size())
            << to_string(header.currentOpIdx) << " " << plan->stages[header.currentStageIdx]->ops_.size() << std::flush;
        return plan->stages[header.currentStageIdx]->ops_[header.currentOpIdx]->compressBeginIdx;
    } else {
        CHECK(header.currentOpIdx < plan->stages[0]->ops_.size())
            << to_string(header.currentOpIdx) << " " << plan->stages[0]->ops_.size() << std::flush;
        return plan->stages[0]->ops_[header.currentOpIdx]->compressBeginIdx;
    }
}

Message Message::BuildEmptyMessage(QueryId qid) {
    Message msg;
    msg.header = MessageHeader::InitHeader(qid);
    msg.plan = std::make_shared<PhysicalPlan>(PhysicalPlan(qid));
    return msg;
}

Message Message::CreateInitMsg(MessageHeader&& header) {
    Message m(std::move(header));
    return m;
}

// Try to ensure the orderliness of data
void Message::SplitMessage(Message& msg, vector<Message>& ret, ClusterRole role) {
    u32 compressBeginIdx = msg.GetCompressBeginIdx(role);
    u32 originalReturnSize = ret.size();

    // Try to ensure the orderliness of data
    vector<Message> tempVec;
    do {
        tempVec.emplace_back(msg.split(compressBeginIdx));
    } while (msg.data.size() > 0);
    for (i64 i = tempVec.size() - 1; i >= 0; i--) ret.emplace_back(std::move(tempVec[i]));

    // Record split number
    u32 splitSize = ret.size() - originalReturnSize;
    if (splitSize <= 1) return;
    for (size_t i = originalReturnSize; i < ret.size(); i++) {
        ret[i].header.splitHistory += "\t" + std::to_string(splitSize);
        ret[i].header.opPerfInfos.emplace_back();
    }
}

void Message::EarlyStop(Message& msg, uint8_t reset_path_size) {
    msg.header.type = MessageType::EARLYSTOP;
    msg.header.splitHistory.resize(reset_path_size);
}
}  // namespace AGE
