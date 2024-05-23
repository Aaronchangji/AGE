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
#include <atomic>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "base/type.h"
#include "execution/message.h"
#include "util/config.h"
#include "util/tool.h"

namespace AGE {

using std::atomic;
using std::vector;

class Scheduler {
   public:
    explicit Scheduler(uint32_t num_threads, bool is_print,
                       SchedulerPolicy policy = Config::GetInstance()->scheduler_policy_)
        : num_threads_(num_threads),
          num_heavy_threads_(Config::GetInstance()->num_heavy_threads_),
          id_assigner_(0),
          heavy_id_assigner_(0),
          loads_(num_threads_),
          last_print_(Tool::getTimeMs()),
          policy_(policy),
          is_print_(is_print),
          rand_generator_(time(nullptr)) {}

    uint32_t Assign(size_t sz, int tid, uint32_t assigned_thread, bool is_heavy) {
        if (is_print_ && !tid) print_load();
        uint32_t assigned_tid = tid;
        if (policy_ == SchedulerPolicy::ROUNDROBIN) {
            // ROUNDROBIN overwrites msg's assigned_thread
            assigned_tid = AssignRoundRobin();
        } else if (assigned_thread < num_threads_) {
            assigned_tid = AssignBoundThread(assigned_thread);
        } else {
            switch (policy_) {
            case SchedulerPolicy::LEASTLOAD:
                assigned_tid = AssignLeastLoad();
                break;
            case SchedulerPolicy::POOLING:
                assigned_tid = AssignPooling(is_heavy);
                break;
            default:
                LOG(ERROR) << "Invalid schedule policy " << SchedulerPolicy_DebugString(policy_);
            }
        }
        AddLoad(assigned_tid, sz);
        // LOG(INFO) << "Assign thread " << assigned_tid;
        // LOG(INFO) << DebugString();
        return assigned_tid;
    }

    void AddLoad(uint32_t tid, size_t sz) { loads_[tid] += sz; }
    void SubLoad(uint32_t tid, size_t sz) { loads_[tid] -= sz; }
    uint32_t AssignBoundThread(uint32_t assigned_thread) {
        CHECK_LT(assigned_thread, num_threads_);
        uint32_t ret = assigned_thread;
        return ret;
    }

    uint32_t AssignRoundRobin() {
        uint32_t ret = id_assigner_++ % num_threads_;
        return ret;
    }

    uint32_t AssignLeastLoad() {
        // return AssignLeastLoadSequence();
        // return AssignLeastLoadDoubleSequence();
        return AssignLeastLoadBalance(0, num_threads_);
    }

    string DebugString() {
        string ret = "Current Load: ";
        for (uint32_t i = 0; i < num_threads_; i++) {
            ret += std::to_string(loads_[i].load()) + " ";
        }
        return ret;
    }

    uint32_t AssignPooling(bool is_heavy = false) {
        uint32_t ret;
        if (is_heavy) {
            // ret = num_threads_ - num_heavy_threads_ + heavy_id_assigner_++ % num_heavy_threads_;
            ret = AssignLeastLoadBalance(num_threads_ - num_heavy_threads_, num_heavy_threads_);
        } else {
            // ret = id_assigner_++ % (num_threads_ - num_heavy_threads_);
            ret = AssignLeastLoadBalance(0, num_threads_ - num_heavy_threads_);
        }
        return ret;
    }

   private:
    uint32_t num_threads_;
    uint32_t num_heavy_threads_;
    atomic<size_t> id_assigner_;
    atomic<size_t> heavy_id_assigner_;
    vector<atomic<size_t>> loads_;
    uint64_t last_print_;

    SchedulerPolicy policy_;
    bool is_print_ = false;

    std::default_random_engine rand_generator_;

    void print_load() {
        if (Tool::getTimeMs() - last_print_ > 1) {
            LOG(INFO) << DebugString();
            last_print_ = Tool::getTimeMs();
        }
    }

    uint32_t AssignLeastLoadSequence() {
        uint64_t min_load = INT_MAX;
        uint32_t ret = 0;
        for (uint32_t i = 0; i < num_threads_; i++) {
            uint64_t cur_l = loads_[i].load();
            if (cur_l < min_load) {
                min_load = cur_l;
                ret = i;
            }
        }
        return ret;
    }

    uint32_t AssignLeastLoadDoubleSequence() {
        uint32_t start_thread = rand_generator_() % 2;
        uint64_t min_load = INT_MAX;
        uint32_t ret = 0;
        for (uint32_t i = start_thread; i < num_threads_; i += 2) {
            uint64_t cur_l = loads_[i].load();
            if (cur_l < min_load) {
                min_load = cur_l;
                ret = i;
            }
        }
        return ret;
    }

    uint32_t AssignLeastLoadBalance(uint32_t start_idx, uint32_t range) {
        uint32_t max_idx = start_idx + range;
        CHECK_LE(max_idx, num_threads_);
        uint32_t candidate1 = rand_generator_() % range;
        uint32_t candidate2 = rand_generator_() % range;
        candidate1 = candidate1 == candidate2 ? (candidate1 + 1) % range : candidate1;

        candidate1 += start_idx;
        candidate2 += start_idx;
        uint64_t load1 = loads_[candidate1].load();
        uint64_t load2 = loads_[candidate2].load();
        return load1 < load2 ? candidate1 : candidate2;
    }
};

}  // namespace AGE
