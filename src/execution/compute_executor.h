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

#include <emmintrin.h>
#include <glog/logging.h>
#include <sys/time.h>
#include <tbb/concurrent_hash_map.h>
#include <algorithm>
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/node.h"
#include "base/type.h"
#include "execution/cpu_affinity.h"
#include "execution/mailbox.h"
#include "execution/message.h"
#include "execution/message_header.h"
#include "execution/monitor.h"
#include "execution/msg_dispatcher.h"
#include "execution/physical_plan.h"
#include "execution/query_coordinator.h"
#include "execution/query_meta.h"
#include "execution/query_scheduler.h"
#include "execution/result_collector.h"
#include "model/opt_target.h"
#include "operator/abstract_op.h"
#include "operator/op_type.h"
#include "plan/planner.h"
#include "proto/master.pb.h"
#include "util/concurrentqueue/blockingconcurrentqueue.h"
#include "util/tool.h"

namespace AGE {

using moodycamel::BlockingConcurrentQueue;

typedef tbb::concurrent_hash_map<QueryId, shared_ptr<PhysicalPlan>> PlanMap;
typedef tbb::concurrent_hash_map<QueryId, QueryCoordinator*> QueryCoordMap;

class ComputeExecutor {
   public:
    ComputeExecutor(Mailbox* mailbox, ResultCollector* rc, MsgDispatcher* msg_dispatcher, Planner* planner,
                    shared_ptr<Monitor<thpt_result_data_t>> thpt_monitor,
                    shared_ptr<Monitor<thpt_result_data_t>> thpt_timeout_monitor,
                    shared_ptr<Monitor<thpt_input_data_t>> thpt_input_monitor, shared_ptr<OpMonitor> op_monitor,
                    shared_ptr<Monitor<inter_data_info_t>> inter_stage_monitor_map)
        : mailbox_(mailbox),
          rc_(rc),
          msg_dispatcher_(msg_dispatcher),
          shutdown_(false),
          planner_(planner),
          thpt_monitor_(thpt_monitor),
          thpt_timeout_monitor_(thpt_timeout_monitor),
          thpt_input_monitor_(thpt_input_monitor),
          op_monitor_(op_monitor),
          inter_stage_monitor_map_(inter_stage_monitor_map),
          opt_target_(0, Config::GetInstance()->timeout_ms_, 1, 1),
          local_monitor_(1E9) {}
    ~ComputeExecutor() {
        stop();
        join();
    }

    void Init() {}

    void start() {
        Init();
        plan_cleaner_ = std::thread(&ComputeExecutor::PlanCleaner, this);
        // timer_ = std::thread(&ComputeExecutor::Timer, this);
        Config* config = Config::GetInstance();

        for (int i = 0; i < config->num_threads_; i++) {
            execution_thread_pool_.emplace_back(&ComputeExecutor::ThreadExecutor, this, i);
        }

        for (int i = 0; i < config->num_mailbox_threads_; i++) {
            sender_thread_pool_.emplace_back(&ComputeExecutor::ThreadSender, this, i);
            if (config->use_rdma_mailbox_) recver_thread_pool_.emplace_back(&ComputeExecutor::ThreadRecver, this, i);
        }
    }

    void join() {
        for (auto& th : execution_thread_pool_) th.join();
        for (auto& th : sender_thread_pool_) th.join();
        for (auto& th : recver_thread_pool_) th.join();
        plan_cleaner_.join();
    }

    void stop() { shutdown_ = true; }
    void execute(int tid, Message& msg) {
        std::vector<Message> result;
        QueryId cur_qid = msg.header.qid;
        OpType cur_op_type = msg.plan->getStage(msg.header.currentStageIdx)->getOpType(msg.header.currentOpIdx);
        // LOG(INFO) << "Executing query " << cur_qid << " op " << OpType_DebugString(cur_op_type) << " stage "
        //           << to_string(msg.header.currentStageIdx) << " opIdx " << to_string(msg.header.currentOpIdx)
        //           << " tid " << tid;
        // LOG(INFO) << msg.header.DebugString() << std::flush;

        // Query Timeout allows that qc can be deleted during execution
        QueryCoordinator* qc = GetQueryCoordinator(cur_qid);
        if (qc == nullptr) return;
        bool send_remote = qc->process(msg, result);

        if (result.size() == 0) return;
        if (result.size() == 1) {
            Message& msg = result[0];

            if (msg.header.type == MessageType::TIMEOUT) {
                try_catch_timeout_query(qc, msg);
                return;
            }

            OpType next_op_type = msg.plan->getStage(msg.header.currentStageIdx)->getOpType(msg.header.currentOpIdx);
            if (cur_op_type == OpType_END && next_op_type == OpType_END) {
                try_catch_compelete_query(tid, qc, rc_, msg);
                return;
            }
        }

        // TODO(cjli): decide when should dispatch be invoked
        // For compute worker op: it's better to invoke dispatch when the msg size is too big
        //      but for barriers, in most case, dispatch shouldn't be invoked to avoid high contention
        //      However, in some cases, dispatch is also necessary if the contantion is low, for example,
        //      the groupBy operation, which is mostly local contention on the row prefix.
        // For cache worker op: dispatch should be invoked
        if (send_remote) {
            msg_dispatcher_->dispatch(result);
        } else {
            // msg_dispatcher_->split_messages(result);
        }

        LOG_IF(INFO, send_remote && Config::GetInstance()->verbose_ >= 2)
            << "Sending " << result.size() << " to remote";
        for (size_t i = 0; i < result.size(); i++) {
            send_remote ? mailbox_->send_remote(std::move(result[i]), ClusterRole::CACHE)
                        : mailbox_->send_local(std::move(result[i]), tid);
        }
    }

    void ThreadExecutor(int tid) {
        if (Config::GetInstance()->core_bind_) cpu_affinity.BindToCore(tid);
        bool enable_steal = Config::GetInstance()->enable_steal_;
        while (!shutdown_) {
            Message msg;
            if (try_parse_query(tid, msg) ||             // New query
                mailbox_->compute_try_recv(tid, msg) ||  // Receive or steal a msg
                (enable_steal && mailbox_->compute_try_steal(tid, msg))) {
                if (msg.plan == nullptr) {
                    if (!FindPlan(msg)) continue;
                    if (Config::GetInstance()->scheduler_policy_ == SchedulerPolicy::POOLING) {
                        // do not execute, send back to queue to decide the execute tid with heavy/light
                        mailbox_->send_local(std::move(msg), tid);
                        continue;
                    }
                }
                execute(tid, msg);
            }
        }
    }

    void ThreadSender(int tid) {
        if (Config::GetInstance()->core_bind_) {
            int bind_id = tid + Config::GetInstance()->num_threads_;
            LOG(INFO) << "Sender " << tid << " try to bind " << bind_id;
            cpu_affinity.BindToCore(bind_id);
        }
        while (!shutdown_) {
            mailbox_->try_send(tid);
        }
    }

    void ThreadRecver(int tid) {
        // Only when using rdma mailbox, this thread is needed
        if (Config::GetInstance()->core_bind_) {
            int bind_id = tid + Config::GetInstance()->num_threads_ + Config::GetInstance()->num_mailbox_threads_;
            LOG(INFO) << "Recver " << tid << " try to bind " << bind_id;
            cpu_affinity.BindToCore(bind_id);
        }
        while (!shutdown_) {
            mailbox_->rdma_try_recv(tid, ClusterRole::COMPUTE);
        }
    }

    void PlanCleaner() {
        while (!shutdown_) {
            QueryId qid;
            finished_plan_queue_.wait_dequeue(qid);
            if (ErasePlan(qid)) {
                EraseQueryCoordinator(qid);
            }

            if (Config::GetInstance()->op_profiler_ > 1) PrintNumActiveQueries();
        }
    }

    void Timer() {
        // Used for debugging
        while (!shutdown_) {
            std::chrono::seconds dur(1);
            std::this_thread::sleep_for(dur);
        }
    }

    // only for query parent(i.e. coordinator)
    // we need to make sure that the plan is inserted after creating coordinator
    // since plan existence can be used for checking whether qc is inited
    ExecutionStrategy InitQuery(shared_ptr<PhysicalPlan>&& plan, QueryMeta&& qm) {
        ExecutionStrategy str = CreateQueryCoordinator(plan, std::move(qm));
        InsertPlan(std::move(plan));
        return str;
    }

    void InsertPlan(shared_ptr<PhysicalPlan>&& plan) {
        // Insert execution plan
        PlanMap::accessor ac;
        plan_map_.insert(ac, plan->qid);
        ac->second = std::move(plan);
    }

    bool FindPlan(Message& msg) {
        // all messages except init message has a plan
        PlanMap::const_accessor ac;
        if (!plan_map_.find(ac, msg.header.qid)) {
            // plan not found, might be deleted or inserted failed
            // currently, just ignore it and TODO(TBD) add error code for future
            // LOG(ERROR) << "Cannot find query plan of query " << msg.header.qid;
            return false;
        }
        msg.plan = ac->second;
        return true;
    }

    bool ErasePlan(QueryId qid) {
        PlanMap::accessor pm_ac;
        if (!plan_map_.find(pm_ac, qid)) return false;
        if (pm_ac->second.use_count() != 1) {
            // only this thread is using the plan, cleanable
            finished_plan_queue_.enqueue(qid);  // Re-try
            return false;
        }
        plan_map_.erase(pm_ac);
        return true;
    }

    ExecutionStrategy CreateQueryCoordinator(const shared_ptr<PhysicalPlan>& plan, QueryMeta&& qm = QueryMeta()) {
        QueryCoordMap::accessor ac;
        if (!query_coordinator_map_.insert(ac, plan->qid)) {
            CHECK(false) << "Query coordinator of query " << to_string(plan->qid)
                         << " already exists during creation\n";
        }
        ac->second = new QueryCoordinator(plan->qid, op_monitor_, thpt_input_monitor_);
        ac->second->insert_meta(std::move(qm));
        return ac->second->prepare(plan);  // using plan to prepare the execution procedure
    }

    QueryCoordinator* GetQueryCoordinator(QueryId qid) {
        QueryCoordMap::accessor ac;
        if (!query_coordinator_map_.find(ac, qid)) return nullptr;
        return ac->second;
    }

    void EraseQueryCoordinator(QueryId qid) {
        QueryCoordMap::accessor qc_ac;
        if (query_coordinator_map_.find(qc_ac, qid)) {
            delete qc_ac->second;
            query_coordinator_map_.erase(qc_ac);
        } else {
            LOG(ERROR) << "Failed to erase Query Coordinator for query: " << qid;
        }
    }

    void PrintNumActiveQueries() {
        static u64 last_check_time = 0;
        u64 cur_time = Tool::getTimeMs();
        if (cur_time - last_check_time > 100) {
            last_check_time = cur_time;
            // LOG(INFO) << "# active query: " << plan_map_.size() - finished_plan_queue_.size_approx();
            LOG(INFO) << "plan_map size: " << plan_map_.size();
        }
    }

   private:
    // map from qid to ExecutionPlan.
    // tbb::concurrent_hash_map<QueryId, PhysicalPlan*> plan_map_;
    PlanMap plan_map_;

    // each query only have one query coordinator on parent worker
    // tbb::concurrent_hash_map<QueryId, QueryCoordinator*> QueryCoordMap;
    // QueryCoordinator might be created and looked up
    QueryCoordMap query_coordinator_map_;

    // Thread pool.
    std::vector<std::thread> execution_thread_pool_;
    std::vector<std::thread> sender_thread_pool_;
    std::vector<std::thread> recver_thread_pool_;
    std::thread plan_cleaner_;
    std::thread timer_;
    BlockingConcurrentQueue<QueryId> finished_plan_queue_;

    // Mailbox.
    Mailbox* mailbox_;

    // Result collector.
    ResultCollector* rc_;

    // Message Dispatcher
    MsgDispatcher* msg_dispatcher_;

    // CPU Affinity
    CPUAffinity cpu_affinity;

    // Shutdown flag;
    bool shutdown_;

    // Planner
    Planner* planner_;

    // Monitor
    shared_ptr<Monitor<thpt_result_data_t>> thpt_monitor_;
    shared_ptr<Monitor<thpt_result_data_t>> thpt_timeout_monitor_;
    shared_ptr<Monitor<thpt_input_data_t>> thpt_input_monitor_;
    shared_ptr<OpMonitor> op_monitor_;
    shared_ptr<Monitor<inter_data_info_t>> inter_stage_monitor_map_;
    MLModel::OptTarget opt_target_;

    // local monitor
    Monitor<uint64_t> local_monitor_;

    void try_catch_timeout_query(QueryCoordinator* qc, Message& msg) {
        uint64_t exec_time = Tool::getTimeNs() / 1000.0 - qc->get_meta()->proc_time_;
        if (qc->get_meta()->is_thpt_test_) {
            thpt_result_data_t data(msg.plan->query_template_idx, exec_time);
            thpt_timeout_monitor_->record_single_data(data);
            return;
        }
        LOG(INFO) << "Query Timeout with " << exec_time / 1000.0 << " ms" << std::flush;
        string error = "Reached Timeout";
        string clientHostPorts = qc->get_meta()->client_host_port_;
        rc_->insert(error, std::move(clientHostPorts));
        finished_plan_queue_.enqueue(msg.header.qid);
    }

    void try_catch_compelete_query(int tid, QueryCoordinator* qc, ResultCollector* rc, Message& msg) {
        LOG_IF(INFO, Config::GetInstance()->verbose_) << "Query Result:\n" << msg.DebugString();
        QueryId qid = msg.header.qid;
        QueryMeta qm = qc->dump_meta(msg.plan, inter_stage_monitor_map_);
        if (qm.is_thpt_test_) {
            u64 exec_time = Tool::getTimeNs() / 1000.0 - qm.proc_time_;
            thpt_result_data_t data(msg.plan->query_template_idx, exec_time, qm.batchwidth_);
            thpt_monitor_->record_single_data(data);
        } else {
            // query process done. Send result back to collector.
            rc_->insert(qid, std::move(msg.data), std::move(qm));
        }
        msg.plan->set_finish();
        finished_plan_queue_.enqueue(qid);
    }

    bool try_parse_query(int tid, Message& msg) {
        RawQuery raw_query;
        if (!mailbox_->try_get_query(tid, raw_query)) {
            return false;
        }

        string error = "";
        auto [returnStrs, raw_plan] = planner_->process(raw_query.query_, &error);

        raw_plan->timeout = raw_query.timeout_ == 0 ? Config::GetInstance()->timeout_ms_ : raw_query.timeout_;
        raw_plan->query_template_idx = raw_query.query_template_idx_;
        raw_plan->is_heavy = false;  // false by default

        for (auto stage : raw_plan->stages) {
            stage->set_stage_type();
        }

        shared_ptr<PhysicalPlan> plan(raw_plan);  // make plan shared

        // Print plan
        if (plan != nullptr) {
            LOG_IF(INFO, Config::GetInstance()->verbose_ >= 2) << "Plan of Query " << plan->qid << " is generated:\n"
                                                               << plan->DebugString();
            if (returnStrs.size() == 1 && returnStrs[0].find("INDEX") != string::npos) {
                boardcast_index(returnStrs[0]);
                returnStrs.clear();
            }
        } else {
            // Error
            rc_->insert(error, raw_query.client_host_port_);
            LOG(ERROR) << "Query " << raw_query.query_ << " encounter error: " << error << ", size: " << error.size()
                       << flush;
            return false;
        }

        QueryId qid = plan->qid;
        QueryMeta qm(raw_query, std::move(returnStrs));
        ExecutionStrategy init_strategy = InitQuery(std::move(plan), std::move(qm));

        // Create init message and enqueue into local queue for query coordinator to further process the message
        MessageHeader header = MessageHeader::InitHeader(qid);
        msg = Message::CreateInitMsg(std::move(header));
        msg.execution_strategy = init_strategy;

        return true;
    }

    void boardcast_index(string& params) {
        Config* config = Config::GetInstance();
        std::shared_ptr<brpc::Channel> channel =
            BRPCHelper::build_channel(1000, 3, config->master_ip_address_, config->master_listen_port_);
        ProtoBuf::Master::Service_Stub stub(channel.get());
        ProtoBuf::Master::SyncIndexReq req;
        ProtoBuf::Master::SyncIndexResp resp;
        brpc::Controller cntl;

        vector<string> tokens = Tool::split(params, ",");
        // tokens[0] == INDEX
        req.set_label(stoi(tokens[1]));
        req.set_prop(stoi(tokens[2]));
        req.set_iscreate(tokens[3] == "1" ? true : false);

        stub.SyncIndex(&cntl, &req, &resp, nullptr);

        if (cntl.Failed()) {
            LOG(ERROR) << cntl.ErrorText() << std::flush;
            return;
        }

        if (!resp.success()) {
            LOG(ERROR) << "Boardcast failed" << std::flush;
            return;
        }
    }
};
}  // namespace AGE
