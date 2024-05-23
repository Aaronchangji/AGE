// Copyright 2022 HDL
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

#include <brpc/channel.h>
#include <brpc/server.h>
#include <glog/logging.h>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "base/node.h"
#include "base/type.h"
#include "execution/thpt_handler.h"
#include "model/rl_action.h"
#include "proto/cache_worker.pb.h"
#include "proto/compute_worker.pb.h"
#include "proto/master.pb.h"
#include "storage/str_map.h"
#include "util/brpc_helper.h"
#include "util/thpt_test_helper.h"

using std::deque;
using std::mutex;
using std::string;
using std::vector;

namespace AGE {
namespace ProtoBuf {
namespace Master {

class MasterRpcServer : public Service {
   public:
    explicit MasterRpcServer(Nodes &compute_nodes_, Nodes &cache_nodes_,
                             vector<shared_ptr<brpc::Channel>>& compute_node_channels,
                             vector<shared_ptr<brpc::Channel>>& cache_node_channels,
                             StrMap &strMap, std::shared_ptr<MasterThptHandler> thpt_handler)
        : compute_nodes(compute_nodes_),
          cache_nodes(cache_nodes_),
          compute_node_channels(compute_node_channels),
          cache_node_channels(cache_node_channels),
          strMap(strMap),
          thpt_handler(thpt_handler),
          compute_worker_ready_meta(compute_nodes_.getWorldSize()),
          cache_worker_ready_meta(cache_nodes_.getWorldSize()) {}

    ~MasterRpcServer() {}

    void Ready(google::protobuf::RpcController *cntlBase, const ReadyRequest *req, ReadyResponse *resp,
               google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);

        ClusterRole role = static_cast<ClusterRole>(req->role());
        ReadyMeta *ready_meta;
        if (role == ClusterRole::COMPUTE) {
            ready_meta = &compute_worker_ready_meta;
        } else if (role == ClusterRole::CACHE) {
            ready_meta = &cache_worker_ready_meta;
        } else {
            LOG(ERROR) << "Unexpected role from Ready RPC Call" << std::flush;
            resp->set_success(false);
            return;
        }

        u32 rk = req->rank();

        ready_meta->readyMutex.lock();
        if (!ready_meta->readyStatus[rk]) ready_meta->readyCount++;
        LOG(INFO) << "Received ready with rank: " << rk << ", readyCount:" << ready_meta->readyCount << " from "
                  << RoleString(role) << " worker";
        ready_meta->readyStatus[rk] = true;
        if (ready_meta->readyCount == ready_meta->readyStatus.size()) {
            LOG(INFO) << "All " << RoleString(role) << " workers are ready...";
            ready_meta->allReady = true;
        }
        ready_meta->readyMutex.unlock();

        resp->set_success(true);
    }

    void RequestTopo(google::protobuf::RpcController *cntlBase, const RequestTopoReq *req, RequestTopoResp *resp,
                     google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        DLOG_IF(INFO, Config::GetInstance()->verbose_) << cache_nodes.DebugString() << std::flush;
        ClusterRole role = static_cast<ClusterRole>(req->role());
        if (role == ClusterRole::COMPUTE) {
            compute_nodes.ToString(resp->mutable_data());
        } else if (role == ClusterRole::CACHE) {
            cache_nodes.ToString(resp->mutable_data());
        } else {
            LOG(ERROR) << "Unexpected role from Ready RPC Call" << std::flush;
            resp->set_success(false);
            return;
        }
        resp->set_success(true);
    }

    void RequestStrMap(google::protobuf::RpcController *cntlBase, const RequestStrMapReq *req, RequestStrMapResp *resp,
                       google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        DLOG_IF(INFO, Config::GetInstance()->verbose_) << strMap.DebugString() << std::flush;
        strMap.ToString(resp->mutable_data());
        resp->set_success(true);
    }

    void GetRandomQueries(google::protobuf::RpcController *cntlBase, const RandomQueriesReq *req,
                          RandomQueriesResp *resp, google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        vector<string> queries;
        for (size_t i = 0; i < cache_nodes.getWorldSize(); i++) {
            const Node &cache_node = cache_nodes.get(i);
            std::shared_ptr<brpc::Channel> channel =
                BRPCHelper::build_channel(3000, 3, cache_node.host, cache_node.port);
            CacheWorker::Service_Stub stub(channel.get());
            CacheWorker::ThptTestQueryReq get_query_req;
            CacheWorker::ThptTestQueryResp get_query_resp;
            brpc::Controller cntl;

            get_query_req.set_config(req->config());
            stub.GetThptTestQueries(&cntl, &get_query_req, &get_query_resp, nullptr);
            if (!cntl.Failed()) {
                LOG(INFO) << "Successfully send query template to CacheWorker [" << i << "]; " << std::flush;
            } else {
                LOG(WARNING) << cntl.ErrorText();
                continue;
            }

            for (int i = 0; i < get_query_resp.seeds_size(); i++) {
                string q = get_query_resp.seeds(i);
                resp->add_seeds(q);
            }
        }
        resp->set_success(true);
    }

    // send throughput parameter to all workers
    void RequestWorker(google::protobuf::RpcController *cntlBase, const RequestWorkerReq *req, RequestWorkerResp *resp,
                       google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        DLOG(INFO) << "Receved request from client";
        if (!clusters_is_ready()) {
            resp->set_success(false);
            return;
        }

        // Decides which worker node as parent
        u8 parentRank = getQueryParent();
        const Node &parentNode = compute_nodes.get(parentRank);

        resp->set_success(true);
        resp->set_host(parentNode.host);
        resp->set_port(parentNode.port);
    }

    void RequestAllWorkers(google::protobuf::RpcController *cntlBase, const RequestAllWorkerReq *req,
                           RequestAllWorkerResp *resp, google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        if (!clusters_is_ready()) {
            resp->set_success(false);
            return;
        }

        for (size_t i = 0; i < compute_nodes.getWorldSize(); i++) {
            resp->add_hosts(compute_nodes.get(i).host);
            resp->add_ports(compute_nodes.get(i).port);
        }
        resp->set_success(true);
    }

    void SyncIndex(google::protobuf::RpcController *cntlBase, const SyncIndexReq *req, SyncIndexResp *resp,
                   google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        brpc::Controller *cntl = static_cast<brpc::Controller *>(cntlBase);
        uint32_t ipaddr = ntohl(cntl->remote_side().ip.s_addr);
        string sender_host;
        Tool::ip2string(ipaddr, sender_host);

        for (size_t i = 0; i < compute_nodes.getWorldSize(); i++) {
            const Node &compute_node = compute_nodes.get(i);
            if (compute_node.host == sender_host) {
                continue;
            }
            std::shared_ptr<brpc::Channel> channel =
                BRPCHelper::build_channel(300, 3, compute_node.host, compute_node.port);
            ComputeWorker::Service_Stub stub(channel.get());
            ComputeWorker::SyncIndexReq sync_index_req;
            ComputeWorker::SyncIndexResp sync_index_resp;
            brpc::Controller cntl;

            sync_index_req.set_label(req->label());
            sync_index_req.set_prop(req->prop());
            sync_index_req.set_iscreate(req->iscreate());

            stub.SyncIndex(&cntl, &sync_index_req, &sync_index_resp, nullptr);
            if (!cntl.Failed()) {
                LOG(INFO) << "Sync index to worker [" << i << "]; " << std::flush;
            } else {
                LOG(WARNING) << cntl.ErrorText();
                continue;
            }
        }

        resp->set_success(true);
    }

    void CheckReady(google::protobuf::RpcController *cntlBase, const CheckReadyReq *req, CheckReadyResp *resp,
                    google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);

        ClusterRole role = static_cast<ClusterRole>(req->role());
        ReadyMeta *ready_meta;
        if (role == ClusterRole::COMPUTE) {
            ready_meta = &compute_worker_ready_meta;
        } else if (role == ClusterRole::CACHE) {
            ready_meta = &cache_worker_ready_meta;
        } else {
            LOG(ERROR) << "Unexpected role from Ready RPC Call" << std::flush;
            resp->set_success(false);
            return;
        }

        if (ready_meta->allReady) {
            resp->set_success(true);
            return;
        }
        resp->set_success(false);
    }

    void ThptStart(google::protobuf::RpcController *cntlBase, const ThptStartReq *req, AnnounceResp *resp,
                   google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        LOG(INFO) << "Start thpt test with wait threshold: " << req->wait_threshold() << " seconds";
        thpt_handler->thpt_start();
        uint64_t wait_thres = req->wait_threshold();
        thpt_handler->set_wait_threshold(wait_thres);
        string prefix = req->prefix() + "." + to_string(Tool::getTimeNs()) + ".worker";
        thpt_handler->set_prefix(prefix);
        resp->set_success(true);
    }

    void ThptSendEnd(google::protobuf::RpcController *cntlBase, const ThptSendEndReq *req, AnnounceResp *resp,
                     google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        LOG(INFO) << "Thpt Send End";
        size_t num_workers = req->num_sent_queries_size();
        if (num_workers != compute_nodes.getWorldSize()) {
            // Received wrong number of workers
            resp->set_success(false);
            return;
        }
        vector<int> num_queries;
        int wid = 0;
        for (auto &nq : req->num_sent_queries()) {
            num_queries.emplace_back(nq);
            LOG(INFO) << "Worker " << wid++ << " received " << nq << " queries";
        }
        thpt_handler->thpt_send_end(num_queries);
        thpt_handler->set_send_duration(req->send_duration());
        resp->set_success(true);
    }

    void ClearThptMonitor(google::protobuf::RpcController *cntlBase, const AnnounceReq *req, AnnounceResp *resp,
                          google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);
        thpt_handler->reset();
        resp->set_success(true);
    }

    void Config(google::protobuf::RpcController *cntlBase, const ConfigReq *req, AnnounceResp *resp,
                google::protobuf::Closure *done) {
        brpc::ClosureGuard doneGuard(done);

        MLModel::RLAction action(req->batch_size(), req->message_size());

        // Compute Worker States
        LOG(INFO) << "Sending action to workers" << std::flush;
        for (size_t i = 0; i < compute_nodes.getWorldSize(); i++) {
            shared_ptr<brpc::Channel> ch = compute_node_channels[i];
            ProtoBuf::ComputeWorker::Service_Stub stub(ch.get());
            ProtoBuf::ComputeWorker::ApplyRLActionReq req;
            ProtoBuf::ComputeWorker::ApplyRLActionResp resp;
            brpc::Controller cntl;
            action.ToString(req.mutable_action());
            stub.ApplyRLAction(&cntl, &req, &resp, nullptr);

            if (cntl.Failed()) {
                LOG(WARNING) << cntl.ErrorText();
                continue;
            }
        }

        // Cache Worker States
        for (size_t i = 0; i < cache_nodes.getWorldSize(); i++) {
            shared_ptr<brpc::Channel> ch = cache_node_channels[i];
            ProtoBuf::CacheWorker::Service_Stub stub(ch.get());
            ProtoBuf::CacheWorker::ApplyRLActionReq req;
            ProtoBuf::CacheWorker::ApplyRLActionResp resp;
            brpc::Controller cntl;
            action.ToString(req.mutable_action());
            stub.ApplyRLAction(&cntl, &req, &resp, nullptr);

            if (cntl.Failed()) {
                LOG(WARNING) << cntl.ErrorText();
                continue;
            }
        }

        resp->set_success(true);
    }
    inline bool ClusterIsReady() { return clusters_is_ready(); }

   private:
    Nodes &compute_nodes;
    Nodes &cache_nodes;
    vector<shared_ptr<brpc::Channel>>& compute_node_channels;
    vector<shared_ptr<brpc::Channel>>& cache_node_channels;
    u8 queryParent;
    StrMap &strMap;

    std::shared_ptr<MasterThptHandler> thpt_handler;

    // Worker ready metada
    struct ReadyMeta {
        explicit ReadyMeta(size_t cluster_size) : readyStatus(cluster_size, false), readyCount(0), allReady(false) {}

        mutex readyMutex;
        deque<bool> readyStatus;
        u32 readyCount;
        bool allReady;
    };

    ReadyMeta compute_worker_ready_meta;
    ReadyMeta cache_worker_ready_meta;

    inline u8 getQueryParent() { return queryParent++ % compute_nodes.getWorldSize(); }
    inline bool clusters_is_ready() { return (compute_worker_ready_meta.allReady && cache_worker_ready_meta.allReady); }
};
}  // namespace Master
}  // namespace ProtoBuf
}  // namespace AGE
