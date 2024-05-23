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
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "base/serializer.h"
#include "base/type.h"
#include "execution/execution_stage.h"
#include "operator/op_type.h"

using std::string;

namespace AGE {
class Graph;

// Physical plan to execute operator
class PhysicalPlan {
   public:
    constexpr static size_t npos = -1;
    explicit PhysicalPlan(int qid = 0) : g(nullptr), qid(qid) {}
    std::vector<ExecutionStage *> stages;
    Graph *g;
    QueryId qid;
    uint32_t timeout;
    int query_template_idx;
    bool is_heavy{false};

    // TODO(ycli): memory pool for every plan.
    ~PhysicalPlan() {
        for (ExecutionStage *stage : stages) {
            delete stage;
        }
    }

    void AppendStage(ExecutionStage *stage, size_t prev = npos) {
        // By default, connect to last op of plan
        if (stages.size() > 0) {
            if (prev == npos) prev = stages.size() - 1;
            stages[prev]->addNextStage(stages.size());
            stage->setPrevStage(prev);
        }
        stages.emplace_back(stage);
    }

    // This function only used for subquery
    void ConnectStages(size_t prev, size_t nxt) {
        CHECK(prev < stages.size() && nxt < stages.size());
        stages[prev]->addNextStage(nxt);
    }

    inline size_t size() const { return stages.size(); }
    ExecutionStage *getStage(int stage_idx) const {
        CHECK(stage_idx < static_cast<int>(stages.size()))
            << "Out-of-range: stage_idx: " << stage_idx << ", size: " << stages.size() << "";
        return stages[stage_idx];
    }

    inline int TrackbackToPrevStage(int cur_stage_id) { return stages[cur_stage_id]->getPrevStage(); }

    void ToString(string *s) {
        Serializer::appendVar(s, qid);
        Serializer::appendU8(s, stages.size());
        for (ExecutionStage *stage : stages) {
            stage->ToString(s);
        }
        Serializer::appendBool(s, is_heavy);
    }

    static PhysicalPlan *CreateFromString(const string &s, size_t &pos);

    void set_finish() { isFinished = true; }
    inline bool is_finished() const { return isFinished; }
    inline bool is_index_query() const { return stages[0]->getOpType(0) == OpType_INDEX; }
    void set_is_heavy(bool is_heavy_) { is_heavy = is_heavy_; }

    string DebugString() {
        string ret = "Query " + std::to_string(qid) + ":\n";
        int stageId = 0;
        for (auto &stage : stages) {
            ret += stage->DebugString(stageId++);
        }
        return ret;
    }

    static void SingleStageToString(shared_ptr<PhysicalPlan> &plan, string *s, uint8_t stageIdx);

   private:
    bool isFinished = false;
};
}  // namespace AGE
