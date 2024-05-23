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

#include "execution/cpu_affinity.h"
#include "execution/mailbox.h"
#include "execution/message.h"
#include "execution/physical_plan.h"
#include "execution/query_meta.h"
#include "execution/result_collector.h"
#include "operator/abstract_op.h"
#include "operator/op_type.h"
#include "util/config.h"
#include "util/tool.h"

namespace AGE {
// Return us = 10^-3 ms.

inline void __OperatorExecutor_cpu_relax(int t) {
    t *= 166;
    while (t-- > 0) {
        _mm_pause();
    }
}

typedef tbb::concurrent_hash_map<QueryId, shared_ptr<PhysicalPlan>> PlanMap;
typedef tbb::concurrent_hash_map<QueryId, QueryMeta> QueryMetaMap;

// class::OperatorExecutor handles a thread pool, in which each thread acts as the worker to process all operators.
// Each query should register their id and execution plan in this class such that the worker thread can be aware of the
// metadata (e.g. parameters) for each operator.
class StandaloneExecutor {
   public:
    StandaloneExecutor(Nodes& nodes, Graph& graph, Mailbox& mailbox, ResultCollector& rc, int num_thread)
        : nodes(nodes), graph(graph), mailbox_(mailbox), rc_(rc), shutdown(false), numThread(num_thread) {
        assert(nodes.getWorldSize() == 1);
    }
    ~StandaloneExecutor() {
        stop();
        join();
    }

    void Init() {}

    void start() {
        Init();
        for (int i = 0; i < numThread; i++) {
            thread_pool_.emplace_back(&StandaloneExecutor::ThreadExecutor, this, i);
        }
    }

    void join() {
        for (auto& th : thread_pool_) {
            th.join();
        }
    }

    void stop() { shutdown = true; }

    void execute(int tid, Message& msg) {
        ExecutionStage* cur_stage = msg.plan->getStage(msg.header.currentStageIdx);
        OpType operator_type = cur_stage->getOpType(msg.header.currentOpIdx);
        if (OpType_IsSubquery(operator_type)) {
            check_subquery_status(msg);
        }
        // LOG(INFO) << "exec: " << tid << " " << msg.DebugString();
        std::vector<Message> result;
        bool ready_to_send = cur_stage->processOp(msg, result);
        // LOG(INFO) << "operator : " << OpTypeStr[operator_type] << ", readyToSend: " << ready_to_send;

        if (!ready_to_send) return;
        if (operator_type == OpType_END) {
            // Only worker that owns the query has its queryMeta,
            // so we can erase inside operator executor instead of
            // erasing in worker like the process erasing plan
            QueryMeta qm = eraseQueryMeta(msg.header.qid);

            // query process done.
            // Send to result collector.
            rc_.insert(msg.header.qid, std::move(result[0].data), std::move(qm));
            return;
        }

        OpType next_op_type =
            result[0].plan->getStage(result[0].header.currentStageIdx)->getOpType(result[0].header.currentOpIdx);
        if (!OpType_IsBarrier(next_op_type)) {
            // If next step is END step, do not split.
            vector<Message> tmp_res;
            for (auto& m : result) Message::SplitMessage(m, tmp_res);
            result.swap(tmp_res);
        }

        for (size_t i = 0; i < result.size(); i++) {
            mailbox_.send_local(std::move(result[i]));
        }
    }

    void ThreadExecutor(int tid) {
        if (Config::GetInstance()->core_bind_) cpu_affinity.BindToCore(tid);
        while (!shutdown) {
            Message msg;
            if (!mailbox_.compute_try_recv(tid, msg)) continue;
            if (msg.plan == nullptr && !findPlan(msg)) continue;
            execute(tid, msg);
        }
    }

    void insertPlan(shared_ptr<PhysicalPlan>&& plan) {
        // Insert execution plan
        PlanMap::accessor ac;
        planMap.insert(ac, plan->qid);
        plan->g = &graph;
        ac->second = std::move(plan);
    }

    bool findPlan(Message& msg) {
        // If this message is sent from remote and losses plan pointer, pick up it.
        PlanMap::const_accessor ac;
        if (!planMap.find(ac, msg.header.qid)) {
            // First message of this query to this machine has not been received
            // Resend back to mailbox
            LOG(INFO) << "Resend plan-less message of query " << msg.header.qid;
            mailbox_.send_local(std::move(msg));
            return false;
        }
        msg.plan = ac->second;
        return true;
    }

    void insertQueryMeta(QueryId qid, QueryMeta&& qm) {
        QueryMetaMap::accessor ac;
        queryMetaMap.insert(ac, qid);
        ac->second = std::move(qm);
    }

    void erasePlan(QueryId qid) {
        PlanMap::accessor ac;

        // Find return false means no such key
        // This will happen only when that qid is a invalid query with PlannerException
        if (planMap.find(ac, qid) == false) return;

        // Free PhysicalPlan.
        // delete ac->second;

        // Erase qid-plan pair in planMap.
        planMap.erase(ac);
    }

    QueryMeta eraseQueryMeta(QueryId qid) {
        QueryMetaMap::accessor ac;
        queryMetaMap.find(ac, qid);
        QueryMeta qm = ac->second;
        queryMetaMap.erase(ac);

        return qm;
    }

    PlanMap* getPlanMap() { return &planMap; }

    void check_subquery_status(Message& msg) {
        if (msg.header.subQueryInfos.size() &&
            msg.header.subQueryInfos.back().subquery_entry_pos == msg.header.currentStageIdx) {
            msg.header.type = MessageType::SUBQUERY;
        }
    }

   private:
    // cluster
    Nodes& nodes;

    // map from qid to PhysicalPlan.
    // tbb::concurrent_hash_map<QueryId, PhysicalPlan*> planMap;
    PlanMap planMap;

    // Graph
    Graph& graph;

    // Thread pool.
    std::vector<std::thread> thread_pool_;

    // Query meta data map.
    QueryMetaMap queryMetaMap;

    // Mailbox.
    Mailbox& mailbox_;

    // Result collector.
    ResultCollector& rc_;

    // Shutdown flag;
    bool shutdown;

    // # of thread
    int numThread;

    // CPU Affinity
    CPUAffinity cpu_affinity;
};
}  // namespace AGE
