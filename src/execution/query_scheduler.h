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

#include <algorithm>
#include <memory>
#include "execution/execution_stage.h"
#include "execution/execution_strategy.h"
#include "execution/physical_plan.h"
#include "execution/qc_databuffer.h"
#include "operator/barrier_projection_op.h"
#include "operator/op_type.h"
#include "util/config.h"
namespace AGE {

class StrategyMaker {
   public:
    static void CheckEarlyStoppable(const std::shared_ptr<PhysicalPlan>& plan, QueryStrategy& strategy) {
        bool limit_set = false;
        for (ExecutionStage* stage : plan->stages) {
            for (auto& op : stage->ops_) {
                if (!OpType_IsBarrierProject(op->type)) continue;
                BarrierProjectOp* barrier_project_op = dynamic_cast<BarrierProjectOp*>(op);
                if (!barrier_project_op->is_early_stoppable()) continue;
                if (!limit_set) {
                    strategy.set_batch_size(barrier_project_op->get_limit_num());
                    limit_set = true;
                } else {
                    strategy.set_batch_size(
                        std::max(strategy.get_batch_size(), static_cast<u32>(barrier_project_op->get_limit_num())));
                }
            }
        }
    }
};

class QueryScheduler {
   public:
    enum SchedEvent {
        POP_BUFFER,         // pop out tuples from a stage buffer
        POP_BARRIER,        // pop out tuples from a global barrier operator
        TRACKBACK_BARRIER,  // pop out tuples the SECOND time from the buffer following the global barrier
    };

   public:
    void Schedule(SchedEvent event, QueryStrategy& strategy) {
        if (!strategy.get_adaptive_batch_size()) return;
        switch (event) {
        case POP_BUFFER:
            ScheduleBatchSize(strategy);
            break;
        case POP_BARRIER:
            StartSampledRun(strategy);
            break;
        case TRACKBACK_BARRIER:
            FinishSampledRun(strategy);
            break;
        default: {
        }
        }
    }

    void UpdateMaxNumInput(u64 num_input_tuples) {
        max_sampled_num_input_ = std::max(max_sampled_num_input_, static_cast<u64>(num_input_tuples));
    }

    u64 GetMaxNumInput() const { return max_sampled_num_input_; }

    void UpdateLastBatchSize(u64 num_output_tuples) { last_batch_size = num_output_tuples; }

    void UpdateAccumulatedAmpRatio(u64 num_input_tuples) {
        double amp_ratio = last_batch_size ? (static_cast<double>(num_input_tuples) / last_batch_size) : 1;
        accumulated_amp_ratio *= amp_ratio;
    }

   private:
    void ScheduleBatchSize(QueryStrategy& strategy) {
        // u32 curr_batch_size = strategy.get_batch_size();
        if (strategy.get_sampling_status() == 1) {
            strategy.set_batch_size(strategy.get_batch_size_sample());
        } else {
            strategy.set_batch_size(Config::GetInstance()->batch_size_);
        }
        // LOG(INFO) << "[Scheduler] Sched batch size from " << curr_batch_size << " to " << strategy.get_batch_size();
    }

    void InitInternalCounters() {
        max_sampled_num_input_ = 0;
        accumulated_amp_ratio = 1;
    }

    void StartSampledRun(QueryStrategy& strategy) {
        // LOG(INFO) << "[Scheduler] Sample run begins";
        strategy.set_sampling_status(1);
        strategy.set_is_heavy(false);
        strategy.set_batch_size(strategy.get_batch_size_sample());
        InitInternalCounters();
    }

    void FinishSampledRun(QueryStrategy& strategy) {
        strategy.set_sampling_status(2);
        // if (ByMaxNumSampledInput()) strategy.set_is_heavy(true);
        if (ByAccumulatedAmpRatio()) strategy.set_is_heavy(true);
    }

    bool ByMaxNumSampledInput() const {
        double max_amplification_ratio =
            static_cast<double>(max_sampled_num_input_) / Config::GetInstance()->batch_size_sample_;
        bool is_heavy = max_amplification_ratio >= Config::GetInstance()->heavy_threshold_;
        // LOG(INFO) << "[Scheduler] Sample run ends, max ratio: " << max_amplification_ratio
        //           << ", current pipeline classified as: " << (is_heavy ? "Heavy" : "Light");
        return is_heavy;
    }

    bool ByAccumulatedAmpRatio() const {
        bool is_heavy = accumulated_amp_ratio >= Config::GetInstance()->heavy_threshold_;
        // LOG(INFO) << "[Scheduler] Sample run ends, amp ratio: " << accumulated_amp_ratio
        //           << ", current pipeline classified as: " << (is_heavy ? "Heavy" : "Light");
        return is_heavy;
    }

    u64 max_sampled_num_input_ = 0;

    u64 last_batch_size = 0;
    double accumulated_amp_ratio = 1;
};

}  // namespace AGE
