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
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/type.h"
#include "execution/cpu_affinity.h"
#include "execution/mailbox.h"
#include "execution/message.h"
#include "execution/message_header.h"
#include "execution/msg_dispatcher.h"
#include "execution/physical_plan.h"
#include "execution/query_meta.h"
#include "execution/result_collector.h"
#include "operator/abstract_op.h"
#include "util/config.h"
#include "util/tool.h"

namespace AGE {

using PlanId = uint64_t;

typedef tbb::concurrent_hash_map<QueryId, PhysicalPlan*> PlanMap;

// class::OperatorExecutor handles a thread pool, in which each thread acts as the worker to process all operators.
// Each query should register their id and execution plan in this class such that the worker thread can be aware of the
// metadata (e.g. parameters) for each operator.
class CacheExecutor {
   public:
    CacheExecutor(Graph& graph, Mailbox& mailbox, bool build_with_index)
        : graph(graph), mailbox_(mailbox), shutdown(false), build_with_index(build_with_index) {}
    ~CacheExecutor() {
        Stop();
        Join();
    }

    void Start() {
        LOG(INFO) << "build_with_index:" << build_with_index << std::flush;
        if (build_with_index) BuildIndexIfNeeded();
        Config* config = Config::GetInstance();
        for (int i = 0; i < config->num_threads_; i++) {
            execution_thread_pool_.emplace_back(&CacheExecutor::ThreadExecutor, this, i);
        }
        for (int i = 0; i < config->num_mailbox_threads_; i++) {
            sender_thread_pool_.emplace_back(&CacheExecutor::ThreadSender, this, i);
        }

        if (config->use_rdma_mailbox_) {
            for (int i = 0; i < config->num_mailbox_threads_; i++) {
                recver_thread_pool_.emplace_back(&CacheExecutor::ThreadRecver, this, i);
            }
        }
    }

    void Join() {
        for (auto& th : execution_thread_pool_) th.join();
        for (auto& th : sender_thread_pool_) th.join();
        for (auto& th : recver_thread_pool_) th.join();
    }

    void Stop() { shutdown = true; }

    void execute(int tid, Message& msg) {
        // LOG(INFO) << "Execute by thread " << tid << ": " << msg.DebugString();
        std::vector<Message> result;

        // Process the message
        int cur_op_idx = msg.header.currentOpIdx;  // store the idx to avoid being changed
        // LOG(INFO) << "operator : " << OpTypeStr[msg.plan->getStage(0)->getOpType(cur_op_idx)];
        if (!msg.plan->getStage(0)->processOp(msg, result)) return;

        // Check whether need to send back to compute server
        if (result[0].plan->getStage(0)->isFinished(cur_op_idx)) {
            for (auto& m : result) {
                // senderNode & senderPort is set in CacheWorker::RpcServer::SendMsg()
                m.header.recverNode = m.header.senderNode;
                m.header.recverPort = m.header.senderPort;
                mailbox_.send_remote(std::move(m), ClusterRole::COMPUTE);
            }
        } else {
            // send local
            vector<Message> new_results;
            for (auto& m : result) {
                Message::SplitMessage(m, new_results, ClusterRole::CACHE);
            }

            for (auto& m : new_results) {
                mailbox_.send_local(std::move(m), tid);
            }
        }
    }

    void ThreadExecutor(int tid) {
        if (Config::GetInstance()->core_bind_) cpu_affinity.BindToCore(tid);
        bool enable_steal = Config::GetInstance()->enable_steal_;
        while (!shutdown) {
            Message msg;
            if (mailbox_.cache_try_recv(tid, msg, graph) ||
                (enable_steal && mailbox_.cache_try_steal(tid, msg, graph))) {
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
        while (!shutdown) {
            mailbox_.try_send(tid);
        }
    }

    void ThreadRecver(int tid) {
        if (Config::GetInstance()->core_bind_) {
            int bind_id = tid + Config::GetInstance()->num_threads_ + Config::GetInstance()->num_mailbox_threads_;
            LOG(INFO) << "Recver " << tid << " try to bind " << bind_id;
            cpu_affinity.BindToCore(bind_id);
        }
        while (!shutdown) {
            mailbox_.rdma_try_recv(tid, ClusterRole::CACHE);
        }
    }

   private:
    // Graph
    Graph& graph;

    // Thread pool.
    std::vector<std::thread> execution_thread_pool_;
    std::vector<std::thread> sender_thread_pool_;
    std::vector<std::thread> recver_thread_pool_;

    // Mailbox.
    Mailbox& mailbox_;

    // Shutdown flag;
    bool shutdown;

    // CPU Affinity
    CPUAffinity cpu_affinity;

    bool build_with_index;

    /* This function is hard-coded for auto thpt testing */
    void BuildIndexIfNeeded() {
        LOG(INFO) << "Start Building the Index";
        // Create index operator
        vector<string> ldbc_label_strs{"person", "person", "person", "person", "comment"};
        vector<string> ldbc_prop_strs{"id", "firstName", "lastName", "birthday", "id"};

        vector<string> amazon_label_strs{"product", "review", "product", "user"};
        vector<string> amazon_prop_strs{"id", "id", "brand", "id"};

        // first test dataset
        vector<string>* label_strs;
        vector<string>* prop_strs;
        if (graph.getLabelId(ldbc_label_strs[0]) != INVALID_LABEL) {
            label_strs = &ldbc_label_strs;
            prop_strs = &ldbc_prop_strs;
        } else if (graph.getLabelId(amazon_label_strs[0]) != INVALID_LABEL) {
            label_strs = &amazon_label_strs;
            prop_strs = &amazon_prop_strs;
        } else {
            CHECK(false) << "No valid dataset found";
            return;
        }

        for (int i = 0; i < label_strs->size(); i++) {
            LabelId lid;
            PropId pid;
            Message m;
            m.plan = shared_ptr<PhysicalPlan>(new PhysicalPlan());
            m.plan->g = &graph;
            vector<Message> opt;

            // build for person:id
            lid = graph.getLabelId((*label_strs)[i]);
            pid = graph.getPropId((*prop_strs)[i]);
            IndexOp person_op(lid, pid, true);
            person_op.process(m, opt);
            LOG(INFO) << "Build " << (*label_strs)[i] << ":" << (*prop_strs)[i] << " DONE";
        }
    }
};
}  // namespace AGE
