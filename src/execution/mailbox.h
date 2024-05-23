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
#include <cassert>
#include <cstdint>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "base/node.h"
#include "base/type.h"
#include "brpc/channel.h"
#include "execution/mailbox_scheduler.h"
#include "execution/message.h"
#include "execution/message_header.h"
#include "execution/numeric_monitor.h"
#include "execution/physical_plan.h"
#include "execution/rdma_mailbox.h"
#include "proto/cache_worker.pb.h"
#include "proto/compute_worker.pb.h"
#include "util/brpc_helper.h"
#include "util/concurrentqueue/blockingconcurrentqueue.h"
#include "util/concurrentqueue/concurrentqueue.h"
#include "util/config.h"
#include "util/tool.h"

namespace AGE {

using moodycamel::BlockingConcurrentQueue;
using std::string;

struct InMsgT {
   public:
    InMsgT() {}
    explicit InMsgT(Message&& msg) : is_raw_data_(false), msg_(std::move(msg)), enqueue_ts_(Tool::getTimeNs()) {}
    InMsgT(const string& msg_data, const string& header_data, u32 assigned_thread)
        : is_raw_data_(true),
          msg_data_(msg_data),
          header_data_(header_data),
          assigned_thread_(assigned_thread),
          enqueue_ts_(Tool::getTimeNs()) {}
    InMsgT(const string& msg_data, const string& header_data, const string& plan_data, u32 sender_node,
           u32 assigned_thread)
        : is_raw_data_(true),
          sender_node_(sender_node),
          msg_data_(msg_data),
          header_data_(header_data),
          plan_data_(plan_data),
          assigned_thread_(assigned_thread),
          enqueue_ts_(Tool::getTimeNs()) {}
    // For rdma
    InMsgT(const string& msg, u32 assigned_thread)
        : is_raw_data_(true), msg_data_(msg), assigned_thread_(assigned_thread), enqueue_ts_(Tool::getTimeNs()) {}
    InMsgT(const string& msg, u32 sender_node, u32 assigned_thread)
        : is_raw_data_(true),
          msg_data_(msg),
          sender_node_(sender_node),
          assigned_thread_(assigned_thread),
          enqueue_ts_(Tool::getTimeNs()) {}

    // For Standalone and Compute servers
    bool GetComputeMessage(Message& msg) {
        u64 dequeue_ts = Tool::getTimeNs();
        bool from_raw_data = true;
        if (is_raw_data_) {
            u64 st = Tool::getTimeMs();
            u64 msg_size = msg_data_.size();
            if (Config::GetInstance()->use_rdma_mailbox_) {
                size_t pos = 0;
                msg = Message::CreateFromString(msg_data_, ClusterRole::COMPUTE, pos);
            } else {
                msg = Message::CreateFromString(header_data_, msg_data_, ClusterRole::COMPUTE);
            }
            msg.header.assignedThread = assigned_thread_;
            LOG_IF(INFO, Config::GetInstance()->verbose_ >= 2)
                << "Msg of size " << msg_size << " deserialize time: " << Tool::getTimeMs() - st << "ms";
        } else {
            msg = std::move(msg_);
            from_raw_data = false;
        }

        if (Config::GetInstance()->op_profiler_ && MessageHeader::NeedToCollectPerfStat(msg.header.type)) {
            auto& perf_stat = msg.GetCurrentSplitPerfStat();
            perf_stat.setNetEndTime(Tool::getDateNs());
            perf_stat.setQueStartTime(enqueue_ts_);
            perf_stat.setQueEndTime(dequeue_ts);
        }
        return from_raw_data;
    }
    // For Cache servers
    void GetCacheMessage(Message& msg, Graph& g) {
        u64 dequeue_ts = Tool::getTimeNs();
        if (is_raw_data_) {
            u64 pos = 0, msg_size = msg_data_.size(), st = Tool::getTimeMs();
            if (Config::GetInstance()->use_rdma_mailbox_) {
                msg = Message::CreateFromString(msg_data_, ClusterRole::CACHE, pos);
                msg.plan = shared_ptr<PhysicalPlan>(PhysicalPlan::CreateFromString(msg_data_, pos));
            } else {
                msg = Message::CreateFromString(header_data_, msg_data_, ClusterRole::CACHE);
                msg.plan = shared_ptr<PhysicalPlan>(PhysicalPlan::CreateFromString(plan_data_, pos));
            }
            msg.plan->g = &g;
            msg.header.globalHistorySize = msg.header.splitHistory.size();
            msg.header.senderNode = sender_node_;
            msg.header.assignedThread = assigned_thread_;
            msg.execution_strategy.set_is_dfs(false);  // CacheWorker does not support DFS by default
            LOG_IF(INFO, Config::GetInstance()->verbose_ >= 2)
                << "Msg of size " << msg_size << " deserialize time: " << Tool::getTimeMs() - st << "ms";
        } else {
            msg = std::move(msg_);
        }

        if (Config::GetInstance()->op_profiler_ && MessageHeader::NeedToCollectPerfStat(msg.header.type)) {
            auto& perf_stat = msg.GetCurrentSplitPerfStat();
            perf_stat.setNetEndTime(Tool::getDateNs());
            perf_stat.setQueStartTime(enqueue_ts_);
            perf_stat.setQueEndTime(dequeue_ts);
        }
    }

    bool is_raw_data_;
    u32 sender_node_;
    string msg_data_, header_data_, plan_data_;
    Message msg_;
    u32 assigned_thread_;
    u64 enqueue_ts_;
};

struct OutMsgT {
   public:
    OutMsgT() {}
    OutMsgT(ClusterRole target_cluster, Message&& msg) : target_cluster_(target_cluster), msg_(std::move(msg)) {}

    ClusterRole target_cluster_;
    Message msg_;
};

struct RawQuery {
   public:
    RawQuery() {}
    RawQuery(string&& query, string&& client_host_port, uint64_t start_time, uint32_t timeout, bool is_thpt,
             int query_template_idx)
        : query_(std::move(query)),
          client_host_port_(std::move(client_host_port)),
          start_time_(start_time),
          timeout_(timeout),
          is_thpt_(is_thpt),
          query_template_idx_(query_template_idx) {}

    string query_;
    string client_host_port_;
    uint64_t start_time_;
    uint32_t timeout_;
    bool is_thpt_;
    int query_template_idx_;
};

// Mailbox to send/recv message
class Mailbox {
   public:
    explicit Mailbox(u32 num_exe_threads, u32 num_mailbox_threads,
                     std::shared_ptr<NetworkMonitors> network_monitors = nullptr)
        : num_exe_threads_(num_exe_threads),
          num_mailbox_threads_(num_mailbox_threads),
          rand_generator_(time(nullptr)),
          in_msg_queue_scheduler_(num_exe_threads, /*print load*/ false),
          out_msg_queue_scheduler_(num_mailbox_threads, /*do not print*/ false, SchedulerPolicy::ROUNDROBIN),
          query_queue_scheduler_(num_exe_threads, /*do not print*/ false, SchedulerPolicy::ROUNDROBIN),
          network_monitors_(network_monitors),
          rdma_mailbox_(nullptr) {
        in_msg_queues_ = new BlockingConcurrentQueue<InMsgT>[num_exe_threads_];
        out_msg_queues_ = new BlockingConcurrentQueue<OutMsgT>[num_mailbox_threads_];
        query_queues_ = new BlockingConcurrentQueue<RawQuery>[num_exe_threads_];
        last_print = Tool::getTimeNs();
        if (network_monitors_ == nullptr) network_monitors_ = std::make_shared<NetworkMonitors>();
    }
    ~Mailbox() {
        delete[] in_msg_queues_;
        delete[] out_msg_queues_;
        delete[] query_queues_;
    }

    void set_rdma_mailbox(std::shared_ptr<RdmaMailbox> rdma_mailbox) { rdma_mailbox_ = rdma_mailbox; }

    void send_remote(Message&& msg, ClusterRole target_cluster) {
        if (Config::GetInstance()->op_profiler_ && MessageHeader::NeedToCollectPerfStat(msg.header.type)) {
            msg.header.PreparePerfStatForCurrOp();
            msg.GetCurrentSplitPerfStat().setNetStartTime(Tool::getDateNs());
        }
        out_msg_queues_[out_msg_queue_scheduler_.Assign(msg.data.size(), 0, num_mailbox_threads_, false)].enqueue(
            OutMsgT(target_cluster, std::move(msg)));
    }
    // Zero-copy send called by local executors
    void send_local(Message&& msg, int cur_tid = 0) {
        if (Config::GetInstance()->op_profiler_ && MessageHeader::NeedToCollectPerfStat(msg.header.type))
            msg.header.PreparePerfStatForCurrOp();
        uint32_t tid = in_msg_queue_scheduler_.Assign(msg.data.size(), cur_tid, msg.header.assignedThread,
                                                      msg.plan ? msg.plan->is_heavy  // re-schedule to one pool
                                                               : false);
        in_msg_queues_[tid].enqueue(InMsgT(std::move(msg)));
    }
    // CacheWorker --> ComputeWorker
    void send_local(const string& msg_data, const string& msg_header) {
        uint32_t tid = in_msg_queue_scheduler_.Assign(/*constant workload*/ 1, /*no print load*/ 1, num_exe_threads_,
                                                      /*deserialization is light*/ false);
        in_msg_queues_[tid].enqueue(InMsgT(msg_data, msg_header, num_exe_threads_));
    }
    void send_local(const string& msg_data) {
        uint32_t tid = in_msg_queue_scheduler_.Assign(/*constant workload*/ 1, /*no print load*/ 1, num_exe_threads_,
                                                      /*deserialization is light*/ false);
        in_msg_queues_[tid].enqueue(InMsgT(msg_data, num_exe_threads_));
    }
    // ComputeWorker --> CacheWorker
    void send_local(const string& msg_data, const string& msg_header, const string& plan_data, u32 sender_node) {
        size_t pos = 0;
        uint32_t len = Serializer::readU32(msg_data, pos);
        // Try to get the is_heavy from plan_data
        bool is_heavy = plan_data.back() & 0x1;
        uint32_t tid = in_msg_queue_scheduler_.Assign(len, /*no print load*/ 1, num_exe_threads_, is_heavy);
        in_msg_queues_[tid].enqueue(InMsgT(msg_data, msg_header, plan_data, sender_node, tid));
    }
    void send_local(const string& msg_data, u32 sender_node) {
        uint64_t len = msg_data.size();
        uint32_t tid = in_msg_queue_scheduler_.Assign(len, /*no print load*/ 1, num_exe_threads_, false);
        in_msg_queues_[tid].enqueue(InMsgT(msg_data, sender_node, tid));
    }

    void add_query(string&& query, string&& client_host_port, uint64_t start_time, uint32_t timeout, bool is_thpt,
                   int query_templated_idx) {
        query_queues_[query_queue_scheduler_.Assign(/*constant workload*/ 1, 0, num_exe_threads_, false)].enqueue(
            RawQuery(std::move(query), std::move(client_host_port), start_time, timeout, is_thpt, query_templated_idx));
    }

    // tid: thread id of receiver thread.
    // try_send pops multiple elements from the output
    bool compute_try_recv(int tid, Message& msg);
    bool compute_try_steal(int tid, Message& msg);
    bool cache_try_recv(int tid, Message& msg, Graph& g);
    bool cache_try_steal(int tid, Message& msg, Graph& g);
    void try_send(int tid);
    bool try_get_query(int tid, RawQuery& raw_query) { return query_queues_[tid].wait_dequeue_timed(raw_query, 100); }
    void send_single_msg(Message&& msg, ClusterRole target_cluster);
    void rdma_send_single_msg(int tid, Message&& msg, ClusterRole target_cluster);
    bool rdma_try_recv(int tid, ClusterRole target_cluster, bool placeholder = false);

    void init_channel(vector<string>& addrs, int32_t timeout, int retry) {
        for (auto& addr : addrs) {
            channel_map_[addr] = BRPCHelper::build_channel(timeout, retry, addr);
        }
        LOG(INFO) << "Mailbox init channel done" << std::flush;
    }
    brpc::Channel* get_channel(string& addr) { return channel_map_[addr].get(); }

    std::shared_ptr<NetworkMonitors> get_network_monitors() { return network_monitors_; }

   private:
    u32 num_exe_threads_;
    u32 num_mailbox_threads_;
    std::default_random_engine rand_generator_;

    Scheduler in_msg_queue_scheduler_;
    Scheduler out_msg_queue_scheduler_;
    Scheduler query_queue_scheduler_;

    BlockingConcurrentQueue<InMsgT>* in_msg_queues_;
    BlockingConcurrentQueue<OutMsgT>* out_msg_queues_;
    BlockingConcurrentQueue<RawQuery>* query_queues_;

    std::unordered_map<string, std::shared_ptr<brpc::Channel>> channel_map_;

    std::shared_ptr<NetworkMonitors> network_monitors_;
    u64 last_print;

    std::shared_ptr<RdmaMailbox> rdma_mailbox_;
};
}  // namespace AGE
