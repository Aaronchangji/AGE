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

#include "execution/query_coordinator.h"
#include <algorithm>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include "execution/barrier_util.h"
#include "execution/execution_stage.h"
#include "execution/message_header.h"
#include "execution/monitor.h"
#include "execution/op_profiler.h"
#include "execution/qc_databuffer.h"
#include "execution/query_scheduler.h"
#include "glog/logging.h"
#include "operator/abstract_op.h"
#include "operator/base_barrier_op.h"
#include "operator/loop_op.h"
#include "operator/op_type.h"
#include "operator/subquery_entry_op.h"

namespace AGE {
ExecutionStrategy QueryCoordinator::prepare(const shared_ptr<PhysicalPlan>& plan) {
    const Config* config = Config::GetInstance();
    if (config->op_profiler_) setup_perf_map_for_barrier(plan);

    /**
     * Set the initial strategy
     */
    strategy_.set_is_dfs(config->use_dfs_);
    strategy_.set_split_by_thread(false);
    strategy_.set_available_num_threads(config->num_threads_);
    strategy_.set_adaptive_batch_size(config->adaptive_batch_size_);
    strategy_.set_batch_size_sample(config->batch_size_sample_);
    strategy_.set_is_set(true);
    if (plan->is_index_query()) {
        strategy_.set_is_dfs(false);
    }

    if (config->adaptive_batch_size_) StrategyMaker::CheckEarlyStoppable(plan, strategy_);

    return strategy_;
}

void QueryCoordinator::assign_execution_strategy(Message& msg) const {
    if (msg.execution_strategy.get_is_set()) return;
    msg.execution_strategy = strategy_;
}

bool QueryCoordinator::process(Message& msg, vector<Message>& output) {
    if (Config::GetInstance()->enable_timeout_ && check_timeout(msg, output)) return false;  // Timeout

    // -------Before Processing-------
    assign_execution_strategy(msg);

    // Check whether the op is the first of the stage (except for the first stage)
    if (msg.header.currentOpIdx == 0 && msg.header.currentStageIdx != 0) {
        bool continue_execute = false;
        if (strategy_.get_is_dfs()) {
            continue_execute = preprocess_dfs(msg, output);
        } else {
            continue_execute = preprocess_bfs(msg, output);
        }
        if (!continue_execute) return false;
    }

    // -------Processing--------------
    CHECK_NE(msg.header.type, MessageType::TRACKBACK) << "Trackback message should NOT be physically executed";
    ExecutionStage* cur_stage = msg.plan->getStage(msg.header.currentStageIdx);
    if (cur_stage->getExecutionSide() == ClusterRole::CACHE) {
        // Pack the msg and send them to the remote cache worker
        return prepare_remote_message(msg, output);
    } else {
        // Process here
        cur_stage->processOp(msg, output);
    }

    // -------After Processing--------
    if (strategy_.get_is_dfs()) {
        postprocess_dfs(msg, output);
    }

    return false;
}

bool QueryCoordinator::preprocess_dfs(Message& msg, vector<Message>& output) {
    /**
     * DFS Model:
     * 1. Before processing
     *  1.1. Put the data in the buffer anyway first
     *  1.2. Check whether the prev stage of this batch is finished, if so, we can choose from
     *       starting next stage or trackback.
     *   1.2.1. If choose to execute next stage, we need to pop a batch of data, and execute normally
     *   1.2.2. Otherwise, we need to create a trackback message with no data
     * 2. Processing
     * 3. After processing
     *  3.1. Check whether hits the last stage, if so, store the data into the buffer with key (lastStageIdx,
     * lastStageIdx + 1)
     *   3.1.1. Check whether the query is finished, if so, pop all result and return; if not, invoke
     * trackback
     */
    // LOG(INFO) << "[DFS] preprocess:\n" << msg.DebugString();
    if (msg.header.type == MessageType::BARRIEREND) return true;
    if (msg.header.type == MessageType::TRACKBACK && !check_trackback_validity(msg, output)) return false;

    if (is_subquery(msg)) {
        handle_return_subquery(msg);
        // Subquery but NOT LOOP, skip pre-processing
        if (!is_dfs_subquery(msg)) return true;
    }

    if (!sync_prev_batch(msg)) return false;
    if (MessageHeader::NeedToUpdatePrevBuffer(msg.header.type)) finish_prev_batch(msg);

    // Check early stop
    if (enforce_early_stop(msg)) return false;

    // Check whether it's the last stage, return here if:
    // 1) the query is done; 2) we move back to previous stages
    if (is_last_stage(msg) && finish_last_stage(msg, output)) return false;

    // Pop data and kick off current stage
    return launch_new_batch(msg, output);
}

void QueryCoordinator::postprocess_dfs(Message& msg, vector<Message>& output) {
    if (output.size() == 0) {
        // There is no output for last op due to barrier; Try to trackback
        OpType prev_op_type = msg.plan->getStage(msg.header.currentStageIdx)->getOpType(msg.header.currentOpIdx);
        if ((OpType_IsAggregate(prev_op_type) || OpType_IsBarrierProject(prev_op_type)) &&
            msg.header.type == MessageType::BARRIERREADY) {
            handle_ready_barrier(msg, output);
        }
    } else {
        Message& msg = output[0];
        if (msg.header.type == MessageType::EARLYSTOP) {
            handle_early_stop(msg);
        }
    }
}

bool QueryCoordinator::sync_prev_batch(Message& msg) {
    BufferKey buffer_key = get_curr_buffer_key(msg);
    BufferMapType::accessor ac;
    buffer_map_.insert(ac, buffer_key);

    // LOG(INFO) << "[DFS] Adding " << msg.TupleCnt() << " with " << buffer_key.DebugString();
    buffer_add_data(ac, msg);

    // Check whether this batch is finished
    if (!msg.header.historyIsEmpty() && !prev_batch_is_ready(msg, buffer_key)) return false;

    if (strategy_.get_sampling_status() == 1 && msg.header.type != MessageType::TRACKBACK) {
        // LOG(INFO) << "[DFS] Sampled input: " << ac->first.DebugString() << ", " << ac->second.get_num_input_tuples();
        scheduler_.UpdateAccumulatedAmpRatio(ac->second.get_num_input_tuples());
    }
    return true;
}

void QueryCoordinator::finish_prev_batch(Message& msg) {
    // LOG(INFO) << "[DFS] Finish batch:\n" << msg.DebugString();
    if (msg.header.type == MessageType::LOOPEND) {
        // LOG(INFO) << "[DFS] LOOPEND";
        msg.header.type = MessageType::SPAWN;
        if (msg.header.loopHistory == MessageHeader::kNonLoopOutput) {
            // Only call finish_batch() if loop history is set
            // (i.e., the last output message. There's NO iterative message to complete the batch)
            return;
        }
    }
    BufferKey prev_buffer_key = get_prev_buffer_key(msg), buffer_key = get_curr_buffer_key(msg);
    BufferMapType::accessor prev_ac;
    if (get_prev_buffer(msg, prev_ac)) {
        LOG_IF(INFO, Config::GetInstance()->verbose_) << "[DFS] Finish from " << buffer_key.DebugString() << " to " << prev_buffer_key.DebugString();
        prev_ac->second.finish_batch();

        if (prev_ac->second.is_finish() && watermark_ == prev_buffer_key.value()) {
            // move watermark to current buffer
            forward_watermark(buffer_key.value());
        }
    }
}

bool QueryCoordinator::is_last_stage(const Message& msg) const {
    return msg.header.type != MessageType::TRACKBACK && msg.plan->getStage(msg.header.currentStageIdx)->isEndStage();
}

bool QueryCoordinator::finish_last_stage(Message& msg, vector<Message>& output) {
    // LOG(INFO) << "[DFS] Check last stage; watermark: " << BufferKey::DebugString(watermark_);
    // Whether need to trackback
    if (watermark_ == get_curr_buffer_key(msg).value()) {
        // LOG(INFO) << "[DFS] Watermark Hit";
        if (finish_query(msg, output)) {
            CHECK(output.size()) << "Empty output after finishing the query";
            if (Config::GetInstance()->inter_data_level_ == 1) get_inter_op_data(output.front());
            op_monitor_->DumpCurrentSplitPerfStat(output.front());
            return true;
        } else {
            return false;
        }
    } else {
        // LOG(INFO) << "[DFS] Msg hits the end of pipeline";
        trackback(msg, output);
        return true;
    }
}

void QueryCoordinator::get_inter_op_data(const Message& msg) {
    const auto& collected_stat = msg.header.opPerfInfos.back();

    for (u64 i = 0; i < collected_stat.size(); i++) {
        const OpPerfInfo& info = collected_stat[i];
        OpId id = info.op_id_;
        SingleProcStat stat = info.op_proc_stat_;

        if (id.stage_id_ == 0) {
            if (id.in_stage_op_id_ == 0) {
                // First op in the first stage
                continue;
            }
        }

        AbstractOp* op = msg.plan->getStage(id.stage_id_)->ops_[id.in_stage_op_id_];
        MLModel::OpModelType cur_op_type = MLModel::OpType2OpModelType(op->type);
        if (cur_op_type == MLModel::OpModelType_USELESS) continue;
        MLModel::OpModelType prev_op_type;
        if (id.in_stage_op_id_ == 0) {
            // check previous stage
            int prev_stage_id = msg.plan->TrackbackToPrevStage(id.stage_id_);
            ExecutionStage* prev_stage = msg.plan->getStage(prev_stage_id);
            op = prev_stage->ops_[prev_stage->size() - 1];
            prev_op_type = MLModel::OpType2OpModelType(op->type);
        } else {
            // check prev op
            op = msg.plan->getStage(id.stage_id_)->ops_[id.in_stage_op_id_ - 1];
            prev_op_type = MLModel::OpType2OpModelType(op->type);
        }
        ExecutionStageType _cur_op_type, _prev_op_type;  // execution stage type has same structure with the op type for model
        _cur_op_type.set_single(cur_op_type);
        _prev_op_type.set_single(prev_op_type);
        InterExecutionStageType inter_op_type(_prev_op_type, _cur_op_type);

        InterOpDataMapT::accessor ac;
        inter_op_data_map_.insert(ac, inter_op_type);
        ac->second += stat.logical_tuple_size_;
    }
}

bool QueryCoordinator::finish_query(Message& msg, vector<Message>& output) {
    // LOG(INFO) << "[DFS] Check if query is done ";
    BufferMapType::accessor prev_ac, ac;
    if (get_prev_buffer(msg, prev_ac) && prev_ac->second.is_finish() && get_curr_buffer(msg, ac)) {
        prev_ac.release();
        buffer_pop_data(ac, msg, false);
        output.emplace_back(std::move(msg));
        return true;
    }
    return false;
}

void QueryCoordinator::schedule_batch_strategy(Message& msg) {
    QueryScheduler::SchedEvent event = QueryScheduler::POP_BUFFER;
    if (!msg.plan->is_heavy) {  // If it's already heavy, no need to check
        // Check if we are popping from the pipeline
        BufferKey key = get_curr_buffer_key(msg);
        if (pipeline_watermark_ == key.value() && strategy_.get_sampling_status() == 0) {
            // Start the sample run
            event = QueryScheduler::POP_BARRIER;
        // } else if (watermark_ == key.value() && strategy_.get_sampling_status() == 1 && msg.header.type == TRACKBACK) {
        } else if (strategy_.get_sampling_status() == 1 && msg.header.type == TRACKBACK) {
            // In sampling mode, the first msg trackbacks to watermark triggers the event
            // End (and analyze) the sample run
            event = QueryScheduler::TRACKBACK_BARRIER;
        }
    }
    scheduler_.Schedule(event, strategy_);
    bool cur_is_heavy = strategy_.get_is_heavy();
    if (msg.plan->is_heavy ^ cur_is_heavy) {
        // LOG(INFO) << "record a heavy query";
        msg.plan->set_is_heavy(cur_is_heavy);
        thpt_input_data_t idata(cur_is_heavy);
        thpt_input_monitor_->record_single_data(idata);
    }
}

bool QueryCoordinator::launch_new_batch(Message& msg, vector<Message>& output) {
    CHECK_EQ(msg.data.size(), 0);
    BufferKey buffer_key = get_curr_buffer_key(msg), watermark_key(watermark_);
    BufferMapType::accessor ac;
    CHECK(get_curr_buffer(msg, ac)) << "Failed to acquire buffer for " << buffer_key.DebugString();

    schedule_batch_strategy(msg);
    // LOG(INFO) << "[DFS] Launch batch from " << buffer_key.DebugString() << "\n" << ac->second.DebugString();
    int batch_idx = buffer_pop_data(ac, msg, true, strategy_.get_batch_size());
    if (batch_idx != -1) {
        try_release_trackback_slot(msg);
        msg.header.batchIdx = batch_idx;
        return true;
    }

    // No data for current buffer. Check whether watermark is hit
    if (watermark_ >= buffer_key.value()) {
        if (msg.header.type == MessageType::TRACKBACK) {
            // LOG(INFO) << "[DFS] Unsuccessful Trackback: " << buffer_key.DebugString() << " .vs "
            //           << watermark_key.DebugString();
            try_release_trackback_slot(msg);
            return false;
        } else {
            // The only (though empty) batch that is running
            // TODO(cjli): How to impl early stop if there is no data
            // LOG(INFO) << "[DFS] Continue execution";
            ac->second.launch_batch();
            return true;
        }
    }

    // LOG(INFO) << "[DFS] Trackback";
    if (msg.header.type != MessageType::TRACKBACK && InterDataBuffer::is_loop_buffer(msg)) {
        // Don't allow messages in loop to trigger trackback when there is no data
        return false;
    }

    trackback(msg, output);
    return false;
}

void QueryCoordinator::handle_ready_barrier(Message& msg, vector<Message>& output) {
    BufferKey buf_key = get_curr_buffer_key(msg);
    BufferMapType::accessor ac;
    if (get_curr_buffer(msg, ac)) {
        ac->second.finish_batch();
        if (ac->second.is_finish() && watermark_ == buf_key.value()) {
            // Barrier is finished. Launch a batch that carries all data inside the barrier
            msg.header.type = MessageType::BARRIEREND;
            ac->second.launch_batch();
            output.emplace_back(msg, false);

            // Move watermark to current buffer
            forward_watermark(get_next_buffer_key(msg).value(), true);
        } else {
            // LOG(INFO) << "[DFS] Trackback without modification to message header";
            msg.data.clear();
            trackback(msg, output, false);
        }
    }
}

void QueryCoordinator::handle_early_stop(Message& msg) {
    // This query can be early stopped
    // Move watermark to buffer of next stage
    forward_watermark(get_curr_buffer_key(msg).value());
    msg.header.type = MessageType::SPAWN;
}

bool QueryCoordinator::enforce_early_stop(Message& msg) {
    // Check if the message falls behind the watermark
    if (get_curr_buffer_key(msg).value() >= watermark_) return false;
    // LOG(INFO) << "[DFS] Earlystop triggered " << get_curr_buffer_key(msg).DebugString() << " vs. "
    //           << BufferKey::DebugString(watermark_);
    return true;
}

bool QueryCoordinator::check_trackback_validity(const Message& msg, vector<Message>& output) const {
    BufferKey minimum_key(watermark_), target_key = get_curr_buffer_key(msg);
    if (target_key.value() >= minimum_key.value()) return true;

    // LOG(INFO) << "[DFS] Forward Trackback target from: " << target_key.DebugString()
    //           << " to watermark: " << minimum_key.DebugString();
    output.emplace_back(msg);
    Message& new_msg = output.back();
    new_msg.header.prevStageIdx = minimum_key.prev_stage_idx;
    new_msg.header.currentStageIdx = minimum_key.curr_stage_idx;
    if (InterDataBuffer::is_loop_buffer(new_msg)) {
        // Watermark now inside a loop
        if (new_msg.header.subQueryInfos.empty()) new_msg.header.subQueryInfos.emplace_back();
        new_msg.header.subQueryInfos.back().loop_idx = minimum_key.loop_iter_num;
        new_msg.header.subQueryInfos.back().subquery_entry_pos = new_msg.header.prevStageIdx;
    } else if (InterDataBuffer::is_loop_buffer(msg)) {
        // Trackback message inside a loop BUT the Watermark is NOT
        CHECK_GT(new_msg.header.subQueryInfos.size(), 0);
        new_msg.header.subQueryInfos.pop_back();
    }
    return false;
}

void QueryCoordinator::trackback(Message& msg, vector<Message>& output, bool with_backward) {
    if (!try_hold_trackback_slot(msg)) return;

    int loop_idx = msg.header.subQueryInfos.size() ? msg.header.subQueryInfos.back().loop_idx : 0;
    // LOG(INFO) << "Trackback from (" << to_string(msg.header.prevStageIdx) << ", "
    //           << to_string(msg.header.currentStageIdx) << ": " << loop_idx << ")";
    if (with_backward) {
        u8 prev_buffer_key_type = 0;
        BufferKey prev_buffer_key = get_prev_buffer_key(msg, &prev_buffer_key_type);
        switch (prev_buffer_key_type) {
        case 0:  // from inside the loop to outside
            msg.header.subQueryInfos.pop_back();
        case 1:  // outside the loop
            break;
        case 2:  // from outside the loop to inside
            msg.header.subQueryInfos.emplace_back();
        case 3:  // inside the loop
            CHECK(msg.header.subQueryInfos.size()) << "Loop with empty subquery info";
            msg.header.subQueryInfos.back().loop_idx = loop_idx = prev_buffer_key.loop_iter_num;
            msg.header.subQueryInfos.back().subquery_entry_pos = msg.header.prevStageIdx;
        }
        msg.header.prevStageIdx = prev_buffer_key.prev_stage_idx;
        msg.header.currentStageIdx = prev_buffer_key.curr_stage_idx;
    }
    msg.header.loopHistory = MessageHeader::kNonLoopOutput;
    msg.header.currentOpIdx = 0;
    msg.header.type = MessageType::TRACKBACK;
    // LOG(INFO) << "Trackback to   (" << to_string(msg.header.prevStageIdx) << ", "
    //           << to_string(msg.header.currentStageIdx) << ": " << loop_idx << ")";

    output.emplace_back(std::move(msg));
}

void QueryCoordinator::try_release_trackback_slot(Message& msg) {
    std::lock_guard guard(slot_mutex_);
    // Msg releases the slot or didn't take the slot
    if (msg.header.type == MessageType::TRACKBACK) {
        // LOG(INFO) << "[DFS] Trackback available now" << msg.DebugString();
        CHECK_EQ(trackback_msg_slots_, 0);
        trackback_msg_slots_++;
    }
    msg.header.type = MessageType::SPAWN;
}

bool QueryCoordinator::try_hold_trackback_slot(Message& msg) {
    std::lock_guard guard(slot_mutex_);
    if (!trackback_msg_slots_ && msg.header.type != MessageType::TRACKBACK) {
        // LOG(INFO) << "[DFS] Trackback unavailable";
        return false;
    }
    // Msg takes the slot or is already holding the slot
    if (msg.header.type != MessageType::TRACKBACK) {
        // LOG(INFO) << "[DFS] Trackback unavailable now" << msg.DebugString();
        CHECK_EQ(trackback_msg_slots_, 1);
        trackback_msg_slots_--;
    }
    msg.header.type = MessageType::TRACKBACK;
    return true;
}

bool QueryCoordinator::preprocess_bfs(Message& msg, vector<Message>& output) {
    /**
     * BFS Model:
     * 1. Before processing
     *  1.1. Whether the op is the first op of the stage, if so, we need to check whether
     *      the previous stage is finished.
     *          If so, we can strat the processing. (pop data from the buffer)
     *          Otherwise, we need to wait for all messages. (store data into the buffer)
     *  1.2. Then we should check whether the stage is executed on cache worker, if so, we
     *      need to pack the data and send them to the remote cache worker
     * 2. Processing
     */
    BufferKey buffer_key = get_curr_buffer_key(msg);
    BufferMapType::accessor ac;
    buffer_map_.insert(ac, buffer_key);

    if (!msg.header.historyIsEmpty()) {
        if (prev_batch_is_ready(msg, buffer_key)) {
            // Pop data from the buffer and add back to message
            msg.header.batchIdx = buffer_pop_data(ac, msg, false);
        } else {
            // There is history to be resolved
            // LOG(INFO) << "Adding " << msg.data.size() << " with key " << buffer_key.DebugString() << endl;
            buffer_add_data(ac, msg);
            return false;
        }
    }

    // Check whether the message is sending back for subquery op
    // Change message type to SUBQUERY if so
    if (is_subquery(msg)) handle_return_subquery(msg);

    // Then, we can check whether there is data in the message. If not, we can directly finish this query
    if (msg.data.size() == 0 && msg.header.type != MessageType::SUBQUERY && !strategy_.get_is_dfs()) {
        msg.header.finish(msg.plan->stages.size() - 1, msg.plan->stages.back()->size() - 1);
        output.emplace_back(std::move(msg));
        return false;
    }

    // TODO(cjli) [OPT]: skip ENDop

    if (msg.header.currentStageIdx != 0) {
        LOG_IF(INFO, Config::GetInstance()->verbose_)
            << "Stage " << std::to_string(msg.header.currentStageIdx - 1) << " finished" << std::flush;
    }

    return true;
}

bool QueryCoordinator::prev_batch_is_ready(Message& msg, const BufferKey& key) {
    CounterMapType::accessor ac;
    BatchKey batch_key(key, msg.header.batchIdx);
    counter_map_.insert(ac, batch_key);
    if (Config::GetInstance()->inter_data_level_ == 1) get_inter_op_data(msg);
    bool is_ready = BarrierUtil::Sync(msg, ac->second, op_monitor_, 0);
    // LOG(INFO) << "[DFS] " << batch_key.DebugString() << " ready: " << (is_ready ? "true" : "false");
    return is_ready;
}

bool QueryCoordinator::prepare_remote_message(Message& msg, vector<Message>& output) const {
    // LOG(INFO) << "Prepare remote message: " << msg.header.msg_signature() << std::flush;
    if (msg.data.empty() && strategy_.get_is_dfs() && msg.header.currentStageIdx != 0) {
        // There is no data for this message, no need to send remotely
        msg.header.forward(true, msg.plan->getStage(msg.header.currentStageIdx)->getNextStages().back());
        output.emplace_back(std::move(msg));
        return false;
    }
    output.emplace_back(std::move(msg));
    return true;
}

bool QueryCoordinator::is_subquery(const Message& msg) const { return msg.header.subQueryInfos.size(); }

void QueryCoordinator::handle_return_subquery(Message& msg) const {
    CHECK(is_subquery(msg)) << "Input msg NOT in a subquery";
    if (msg.header.subQueryInfos.back().subquery_entry_pos == msg.header.currentStageIdx) {
        // subquery send back
        msg.header.type = MessageType::SUBQUERY;
    }
}

bool QueryCoordinator::is_dfs_subquery(const Message& msg) const {
    if (!is_subquery(msg)) return false;
    if (msg.plan->getStage(msg.header.subQueryInfos.back().subquery_entry_pos)->getOpType(0) != OpType_LOOP) {
        // NO dfs for branch and optional match subqueries
        return false;
    } else if (msg.plan->getStage(msg.header.currentStageIdx)->getOpType(0) == OpType_LOOP) {
        // This is a collection message going to loop op; skip pre-processing
        return false;
    }
    // loop message going to expand op: needs dfs
    return true;
}

bool QueryCoordinator::buffer_is_full(BufferMapType::accessor& ac, bool with_compress) const {
    return ac->second.full(with_compress);
}

void QueryCoordinator::buffer_add_data(BufferMapType::accessor& ac, Message& msg) {
    all_inter_data_size_ += msg.TupleCnt();
    LOG_IF(INFO, msg.TupleCnt() != 0 && !query_meta_.is_thpt_test_) << "Inter Data Size: " << all_inter_data_size_;
    if (!ac->second.append(msg)) return;
}

int QueryCoordinator::buffer_pop_data(BufferMapType::accessor& ac, Message& msg, bool with_compress, u64 size) {
    if (ac->second.empty()) return strategy_.get_is_dfs() ? -1 : 0;

    int batch_idx = ac->second.pop(msg, with_compress, size);
    all_inter_data_size_ -= msg.TupleCnt();
    LOG_IF(INFO, msg.TupleCnt() != 0 && !query_meta_.is_thpt_test_) << "Inter Data Size: " << all_inter_data_size_;
    scheduler_.UpdateLastBatchSize(msg.TupleCnt());

    return strategy_.get_is_dfs() ? batch_idx : 0;
}

BufferKey QueryCoordinator::get_prev_buffer_key(const Message& msg, u8* buffer_key_type) const {
    AbstractOp* pre_op = msg.plan->getStage(msg.header.prevStageIdx)->ops_[0];
    if (buffer_key_type != nullptr) *buffer_key_type = 1;
    if (pre_op->type == OpType_LOOP) {
        u32 next_stage_idx = msg.plan->getStage(msg.header.currentStageIdx)->getNextStages().front();
        if (next_stage_idx == msg.header.prevStageIdx) {
            // Trackback inside a loop.
            u32 loop_idx = msg.header.subQueryInfos.back().loop_idx;
            if (loop_idx) {
                // Inside a loop with loop_idx > 0
                if (buffer_key_type != nullptr) *buffer_key_type = 3;
                return BufferKey(msg.header.prevStageIdx, msg.header.currentStageIdx, loop_idx - 1);
            } else {
                // Inside a loop but loop_idx == 0: exiting the loop
                if (buffer_key_type != nullptr) *buffer_key_type = 0;
            }
        } else {
            // Trackback into a loop.
            u8 loop_stage_idx = static_cast<u8>(msg.plan->getStage(msg.header.prevStageIdx)->getNextStages().front());
            if (msg.header.loopHistory == MessageHeader::kNonLoopOutput) {
                // Pull from the last iteration of the loop
                if (buffer_key_type != nullptr) *buffer_key_type = 2;
                return BufferKey(msg.header.prevStageIdx, loop_stage_idx,
                                 static_cast<LoopOp*>(pre_op)->get_end_iteration_number() - 1);
            } else {
                // Pull from the last iteration reached
                if (buffer_key_type != nullptr) *buffer_key_type = 2;
                return BufferKey(msg.header.prevStageIdx, loop_stage_idx, msg.header.loopHistory);
            }
        }
    }
    // There is no loop inside another loop. Return BufferKey with loop_num = 0
    u8 prev_prev_stage_idx = static_cast<u8>(msg.plan->getStage(msg.header.prevStageIdx)->getPrevStage());
    return BufferKey(prev_prev_stage_idx, msg.header.prevStageIdx, 0);
}

BufferKey QueryCoordinator::get_curr_buffer_key(const Message& msg) const {
    if (msg.header.subQueryInfos.size()) {
        return BufferKey(msg.header.prevStageIdx, msg.header.currentStageIdx, msg.header.subQueryInfos.back().loop_idx);
    }
    return BufferKey(msg.header.prevStageIdx, msg.header.currentStageIdx, 0);
}

BufferKey QueryCoordinator::get_next_buffer_key(const Message& msg) const {
    u8 next_stage_idx = static_cast<u8>(msg.plan->getStage(msg.header.currentStageIdx)->getNextStages().front());
    return BufferKey(msg.header.currentStageIdx, next_stage_idx);
}

bool QueryCoordinator::get_prev_buffer(const Message& msg, BufferMapType::accessor& prev_ac) {
    BufferKey prev_buffer_key = get_prev_buffer_key(msg);
    if (!buffer_map_.find(prev_ac, prev_buffer_key)) {
        LOG_IF(ERROR, msg.header.prevStageIdx != 0)
            << "Cannot find the buffer for prev stage: " << prev_buffer_key.DebugString();
        return false;
    }
    return true;
}

bool QueryCoordinator::get_curr_buffer(const Message& msg, BufferMapType::accessor& ac) {
    BufferKey buffer_key = get_curr_buffer_key(msg);
    if (!buffer_map_.find(ac, buffer_key)) {
        LOG(ERROR) << "Cannot find the buffer for current stage: " << buffer_key.DebugString();
        return false;
    }
    return true;
}

void QueryCoordinator::forward_watermark(u32 target_watermark_value, bool is_barrier) {
    std::lock_guard guard(watermark_mutex_);
    // LOG(INFO) << "[DFS] Move watermark: " << BufferKey::DebugString(watermark_) << " -> "
    //           << BufferKey::DebugString(target_watermark_value);
    // Make sure that the watermark_ only moves forward
    if (watermark_ >= target_watermark_value) return;
    watermark_ = target_watermark_value;
    if (is_barrier) {
        // Forward for the pipeline and reset the sampling batch
        pipeline_watermark_ = target_watermark_value;
        strategy_.set_sampling_status(0);
    }
}

void QueryCoordinator::setup_perf_map_for_barrier(const shared_ptr<PhysicalPlan>& plan) {
    for (auto& stage : plan->stages) {
        for (auto op : stage->ops_) {
            if (!OpType_IsBarrier(op->type)) continue;
            LOG_IF(INFO, Config::GetInstance()->verbose_ >= 2) << "Setup perf map for:\n" << op->DebugString();
            if (OpType_IsSubquery(op->type)) {
                static_cast<SubqueryEntryOp<>*>(op)->setMonitor(op_monitor_);
            } else {
                static_cast<BaseBarrierOp*>(op)->setMonitor(op_monitor_);
            }
        }
    }
}

QueryMeta QueryCoordinator::dump_meta(const shared_ptr<PhysicalPlan>& plan, std::shared_ptr<Monitor<inter_data_info_t>>& inter_data_monitor) {
    ExecutionStageType whole_query;
    for (BufferMapType::const_iterator i = buffer_map_.begin(); i != buffer_map_.end(); i++) {
        u32 bw = i->second.get_num_input_tuples();
        query_meta_.batchwidth_ = std::max(query_meta_.batchwidth_, static_cast<u64>(bw));

        if (Config::GetInstance()->inter_data_level_ == 0) {  // subquery level (stage)
            int parent_stage = i->first.prev_stage_idx;
            int cur_stage = i->first.curr_stage_idx;
            ExecutionStageType parent_stage_type = plan->stages[parent_stage]->get_stage_type();
            ExecutionStageType cur_stage_type = plan->stages[cur_stage]->get_stage_type();

            if (cur_stage_type.is_empty() || parent_stage_type.is_empty()) {
                continue;
            }

            InterExecutionStageType inter_stage_type(parent_stage_type, cur_stage_type);

            inter_data_info_t data(inter_stage_type.value(), bw);
            inter_data_monitor->record_single_data(data);
        }
    }

    if (Config::GetInstance()->inter_data_level_ == 1) {  // operator level
        // Check inter_op_data_map_;
        tbb::parallel_for(inter_op_data_map_.range(), [&](const InterOpDataMapT::range_type& r) {
            int max = 0;
            for (InterOpDataMapT::iterator i = r.begin(); i != r.end(); i++) {
                inter_data_info_t data(i->first.value(), i->second);
                inter_data_monitor->record_single_data(data);
                // if (i->second > max) {
                //     max = i->second;
                // }
            }
            // inter_data_info_t data(0, max);
            // inter_data_monitor->record_single_data(data);
        });
    }

    if (Config::GetInstance()->inter_data_level_ == 2) {  // query level
        // How to model the query?
        // inter_data_info_t data(query_meta_.batchwidth_);
        // inter_data_monitor->record_single_data(data); 
    }

    return std::move(query_meta_);
}

bool QueryCoordinator::check_timeout(Message& msg, vector<Message>& output) const {
    if (msg.plan->is_index_query()) return false;
    if (msg.plan->is_finished()) return true;
    auto cur_time = Tool::getTimeMs();
    auto duration = cur_time - query_meta_.proc_time_ / 1000.0;
    if (duration > msg.plan->timeout) {
        msg.header.finish(msg.plan->stages.size() - 1, msg.plan->stages.back()->size() - 1);
        msg.header.type = MessageType::TIMEOUT;
        msg.data.clear();
        msg.plan->set_finish();
        output.emplace_back(std::move(msg));
        return true;
    }

    return false;
}

}  // namespace AGE
