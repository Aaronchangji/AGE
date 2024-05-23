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
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "tbb/parallel_for.h"

#include "base/row.h"
#include "base/type.h"
#include "execution/barrier_util.h"
#include "execution/execution_strategy.h"
#include "execution/monitor.h"
#include "execution/op_profiler.h"
#include "execution/physical_plan.h"
#include "execution/qc_databuffer.h"
#include "execution/query_meta.h"
#include "execution/query_scheduler.h"
#include "operator/aggregate_op.h"
#include "operator/op_type.h"
#include "util/tool.h"

namespace AGE {

struct BatchKey {
    BufferKey buffer_key;
    u32 batch_idx;

    BatchKey() : buffer_key(), batch_idx(0) {}
    BatchKey(const BufferKey& buffer_key, u32 batch_idx) : buffer_key(buffer_key), batch_idx(batch_idx) {}
    BatchKey(u8 prev_stage_idx, u8 curr_stage_idx, u32 batch_idx)
        : BatchKey(BufferKey(prev_stage_idx, curr_stage_idx), batch_idx) {}
    BatchKey(u8 prev_stage_idx, u8 curr_stage_idx, u16 loop_iter_num, u32 batch_idx)
        : BatchKey(BufferKey(prev_stage_idx, curr_stage_idx, loop_iter_num), batch_idx) {}
    BatchKey(const BatchKey& key) : buffer_key(key.buffer_key), batch_idx(key.batch_idx) {}
    explicit BatchKey(u64 key) {
        buffer_key = BufferKey(key & ((static_cast<u64>(1) << 32) - 1));
        batch_idx = (key >> 32) & ((static_cast<u64>(1) << 32) - 1);
    }

    inline bool operator==(const BatchKey& rhs) const {
        return (buffer_key == rhs.buffer_key) && (batch_idx == rhs.batch_idx);
    }

    u64 value() const {
        u64 ret = 0;
        ret |= batch_idx;
        ret <<= 32;
        ret |= buffer_key.value();
        return ret;
    }

    string DebugString() const {
        string ret = "BatchKey: [";
        ret += buffer_key.DebugString();
        ret += ", ";
        ret += std::to_string(batch_idx);
        ret += "]";
        return ret;
    }
};

struct BatchKeyCompare {
    static size_t hash(const BatchKey& key) {
        u64 value = key.value();
        return Math::MurmurHash64_x64(&value, sizeof(BatchKey));
    }

    static bool equal(const BatchKey& lhs, const BatchKey& rhs) { return lhs == rhs; }
};

/**
 * Consider an execution graph where the node is the operator, and directed edge is the execution order.
 * QueryCoordinator(QC) only cares the data transferred on edge, which means it doesn't intervene the execution
 * inside the operator.
 */
class QueryCoordinator {
   public:
    const u32 DEFAULT_WATERMARK = BufferKey(0, 1).value();

    explicit QueryCoordinator(QueryId qid, std::shared_ptr<OpMonitor> op_monitor,
                              std::shared_ptr<Monitor<thpt_input_data_t>> thpt_input_monitor)
        : qid_(qid),
          op_monitor_(op_monitor),
          thpt_input_monitor_(thpt_input_monitor),
          trackback_msg_slots_(1),
          pipeline_watermark_(DEFAULT_WATERMARK),
          watermark_(DEFAULT_WATERMARK),
          stage_timer_(Tool::getTimeNs()) {}
    ~QueryCoordinator() {
        if (Config::GetInstance()->verbose_) {
            tbb::parallel_for(buffer_map_.range(), [](const BufferMapType::range_type& r) {
                for (BufferMapType::iterator i = r.begin(); i != r.end(); i++) {
                    LOG(INFO) << i->first.DebugString() << ":" << i->second.collect_info() << std::flush;
                }
            });
        }
    }

    // Key: <prev_stage_idx, curr_stage_idx>, Value: buffer
    typedef tbb::concurrent_hash_map<BufferKey, InterDataBuffer, BufferKeyCompare> BufferMapType;
    typedef tbb::concurrent_hash_map<BatchKey, unordered_map<string, int>, BatchKeyCompare> CounterMapType;

    // prepare the info when receving physical execution plan
    ExecutionStrategy prepare(const shared_ptr<PhysicalPlan>& plan);

    /**
     * @return true: current message should be send to cache worker for processing
     * @return false: current message can be calculated locally and when the function
     *                returns, the op is already processed
     */
    bool process(Message& msg, vector<Message>& output);

    /* Query Meta related functions */
    QueryMeta dump_meta(const shared_ptr<PhysicalPlan>& plan, std::shared_ptr<Monitor<inter_data_info_t>>& inter_data_monitor);
    void insert_meta(QueryMeta&& meta) { query_meta_ = std::move(meta); }
    QueryMeta* get_meta() { return &query_meta_; }
    /* Query Meta related functions ENDS */

   private:
    bool preprocess_dfs(Message& msg, vector<Message>& output);
    void postprocess_dfs(Message& msg, vector<Message>& output);
    bool preprocess_bfs(Message& msg, vector<Message>& output);

    void assign_execution_strategy(Message& msg) const;

    /**
     * check if the trackback message is valid.
     * Otherwise, generate a new, valid trackback message for a re-try
     * @return true: the trackback message is valid
     * @return false: otherwise
     */
    bool check_trackback_validity(const Message& msg, vector<Message>& output) const;
    /**
     * trackback to fetch data from prev stage(s) when:
     *   1. msg hits a barrier op and current stage still has data (stored in buffer or being processed)
     *   2. msg hits END op and watermark < end
     *   3. watermark < current stage BUT current stage's buffer has NO data
     *   4. LOOP subquery
     */
    void trackback(Message& msg, vector<Message>& output, bool with_backward = true);
    void try_release_trackback_slot(Message& msg);
    bool try_hold_trackback_slot(Message& msg);

    bool prev_batch_is_ready(Message& msg, const BufferKey& key);
    /**
     * @return true: the message will be sent to remote
     * @return false: otherwise
     */
    bool prepare_remote_message(Message& msg, vector<Message>& output) const;

    void handle_return_subquery(Message& msg) const;
    bool is_subquery(const Message& msg) const;
    /**
     * @return true: the subquery message needs dfs (namely LOOP)
     * @return false: otherwise
     */
    bool is_dfs_subquery(const Message& msg) const;
    /**
     * Consume a message: put its data into the buffer and check if the batch can be processed,
     * (i.e., query not early-stopped, all messages of that batch have been collected)
     * @return true: the message passes the barrier and can be processed
     * @return false: otherwise
     */
    bool sync_prev_batch(Message& msg);
    /**
     * Finish a batch: notify prev buffer and alter msg type,
     * move watermark if prev stage is done (i.e., buffer is empty & NO ongoing batch)
     */
    void finish_prev_batch(Message& msg);
    bool is_last_stage(const Message& msg) const;
    /**
     * Handle messages hitting the last stage: finish the query if possible, trackback for more data otherwise
     * @return true: complete pre-processing without spawning a new batch
     * @return false: kick off a new batch for later processing
     */
    bool finish_last_stage(Message& msg, vector<Message>& output);
    /**
     * Pop all data when the query is finished
     * @return true: current query is finished
     * @return false: otherwise
     */
    bool finish_query(Message& msg, vector<Message>& output);
    /**
     * Try to fetch a new batch with data for later processing
     * @return true: some data has been popped out from the current buffer to msg for processing
     * @return false: the current buffer has NO data for processing. Init a trackback to prev buffers
     */
    bool launch_new_batch(Message& msg, vector<Message>& output);
    void schedule_batch_strategy(Message& msg);
    /**
     * Process BARRIERREADY -> BARRIEREND when barrier gets all the data,
     * the barrier then takes BARRIEREND and projects data as output
     */
    void handle_ready_barrier(Message& msg, vector<Message>& output);
    /**
     * finish collection early (now only barrier projection)
     */
    void handle_early_stop(Message& msg);
    bool enforce_early_stop(Message& msg);

    /* inter_data_buffer related functions */
    bool buffer_is_full(BufferMapType::accessor& ac, bool with_compress = true) const;
    void buffer_add_data(BufferMapType::accessor& ac, Message& msg);
    int buffer_pop_data(BufferMapType::accessor& ac, Message& msg, bool with_compress, u64 size = MAX_BUFFER_SIZE);
    /* inter_data_buffer related functions ENDS */

    BufferKey get_prev_buffer_key(const Message& msg, u8* prev_buffer_key_type = nullptr) const;
    BufferKey get_curr_buffer_key(const Message& msg) const;
    BufferKey get_next_buffer_key(const Message& msg) const;
    bool get_prev_buffer(const Message& msg, BufferMapType::accessor& prev_ac);
    bool get_curr_buffer(const Message& msg, BufferMapType::accessor& ac);
    void forward_watermark(u32 target_watermark_value, bool is_barrier = false);

    bool check_timeout(Message& msg, vector<Message>& output) const;

    /* profiling relate functions*/
    void setup_perf_map_for_barrier(const shared_ptr<PhysicalPlan>& plan);
    void collect_perf_stats(Message& msg);
    void summarize_perf_stats(const shared_ptr<PhysicalPlan>& plan, const QueryMeta& meta);
    void move_perf_stats_to_meta(const shared_ptr<PhysicalPlan>& plan, QueryMeta& meta);
    /* profiling relate functions ENDS*/

    void get_inter_op_data(const Message& msg);

   private:
    QueryId qid_;  // Necessary redundancy for reversed search for execution plan
    QueryMeta query_meta_;
    QueryStrategy strategy_;
    QueryScheduler scheduler_;

    std::shared_ptr<OpMonitor> op_monitor_;
    std::shared_ptr<Monitor<thpt_input_data_t>> thpt_input_monitor_;

    // Intermediate Data Buffer
    BufferMapType buffer_map_;
    CounterMapType counter_map_;

    i32 trackback_msg_slots_;  // Indicates the number of available trackback msgs
    std::mutex slot_mutex_;    // Mutex protecting the test/set procedure

    u32 pipeline_watermark_;      // Indicates the begining of a pipeline (often a global barrier)
    u32 watermark_;               // Indicates the lowest key that contains data in the buffer, not protected yet
    std::mutex watermark_mutex_;  // Mutex protecting the watermark moving procedure

    typedef tbb::concurrent_hash_map<InterExecutionStageType, int, InterExecutionStageTypeCompare> InterOpDataMapT;
    InterOpDataMapT inter_op_data_map_;
    std::atomic<int> all_inter_data_size_{0};   // across all stages

    uint64_t stage_timer_;  // Used to record the stage finish time for calculating the tuple latency, ns
    vector<float> tuple_latency_;  // Used to record the tuple latency for each stage
};
}  // namespace AGE
