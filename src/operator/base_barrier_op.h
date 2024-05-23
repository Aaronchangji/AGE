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
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "execution/barrier_util.h"
#include "execution/monitor.h"
#include "operator/abstract_op.h"
#include "tbb/concurrent_hash_map.h"

namespace AGE {

// Abstract base barrier operator
// For operators who need to ensure that
// all previous message is collected
// then process to next step
class BaseBarrierOp : public AbstractOp {
   public:
    bool process(Message &msg, std::vector<Message> &output) override {
        std::lock_guard<std::mutex> lk(mutex_);
        uint8_t end_size = get_end_history_size(msg);
        bool is_batch_ready = BarrierUtil::Sync(msg, counter_map_, monitor_, end_size);
        bool is_ready = is_batch_ready;
        if (msg.execution_strategy.get_is_dfs()) {
            bool is_barrier_end = (msg.header.type == MessageType::BARRIEREND);
            is_ready &= is_barrier_end;
        }

        // Barrier Earlystop (should work in dfs)
        if (is_finished_) {
            return false;
        }
        LOG_IF(INFO, Config::GetInstance()->verbose_ >= 2)
            << "Ready: " << (is_ready ? "True" : "False") << ", msg type: " << static_cast<u64>(msg.header.type);

        bool ret = do_work(msg, output, is_ready);
        if (msg.execution_strategy.get_is_dfs()) {
            if (msg.header.type == MessageType::BARRIEREND) {
                for (auto &m : output) m.header.type = MessageType::SPAWN;
            } else if (msg.header.type == MessageType::EARLYSTOP) {
                LOG_IF(INFO, Config::GetInstance()->verbose_) << "BarrierOp EARLYSTOP";
            } else {
                msg.header.type = is_batch_ready ? MessageType::BARRIERREADY : MessageType::SPAWN;
            }
        } else {
            for (auto &m : output) m.header.type = MessageType::SPAWN;
        }
        return ret;
    }

    void setMonitor(std::shared_ptr<OpMonitor> monitor) { monitor_ = monitor; }

   protected:
    explicit BaseBarrierOp(OpType type, u32 compressBeginIdx = COMPRESS_BEGIN_IDX_UNDECIDED)
        : AbstractOp(type, compressBeginIdx) {}
    virtual bool do_work(Message &msg, std::vector<Message> &output, bool isReady) = 0;
    void ack_finish() { is_finished_ = true; }

    virtual uint8_t get_end_history_size(Message &msg) {
        uint8_t end_path_size = 0;
        int subquery_depth = msg.header.subQueryInfos.size();
        if (subquery_depth > 0) {
            end_path_size = msg.header.subQueryInfos.back().parentSplitHistorySize;
        }
        return end_path_size;
    }

   private:
    std::mutex mutex_;
    bool is_finished_ = false;

    BarrierUtil::CounterMapType counter_map_;
    std::shared_ptr<OpMonitor> monitor_;
};
}  // namespace AGE
