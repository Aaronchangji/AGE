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

#include "execution/physical_plan.h"
#include <glog/logging.h>
#include <memory>
#include <string>
#include <vector>
#include "base/serializer.h"
#include "base/type.h"
#include "operator/abstract_op.h"
#include "operator/ops.h"

namespace AGE {
PhysicalPlan *PhysicalPlan::CreateFromString(const string &s, size_t &pos) {
    if (s.size() == 0) return nullptr;
    QueryId qid;
    Serializer::readVar(s, pos, &qid);

    u8 len = Serializer::readU8(s, pos);
    PhysicalPlan *plan = new PhysicalPlan(qid);

    plan->stages.resize(len);
    for (auto &stage : plan->stages) {
        stage = new ExecutionStage();
        stage->FromString(s, pos);
    }
    plan->is_heavy = Serializer::readBool(s, pos);

    // LOG(INFO) << plan->DebugString() << std::flush;
    return plan;
}

void PhysicalPlan::SingleStageToString(shared_ptr<PhysicalPlan> &plan, string *s, uint8_t stageIdx) {
    CHECK(stageIdx <= plan->stages.size()) << "stageIdx out of range";
    Serializer::appendVar(s, plan->qid);
    Serializer::appendU8(s, 1);
    plan->stages[stageIdx]->ToString(s);
    Serializer::appendBool(s, plan->is_heavy);
}
}  // namespace AGE
