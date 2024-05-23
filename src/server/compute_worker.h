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
#include <stdio.h>
#include <unistd.h>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "base/graph_entity.h"
#include "base/node.h"
#include "base/type.h"
#include "brpc/controller.h"
#include "execution/compute_executor.h"
#include "execution/execution_stage_type.h"
#include "execution/mailbox.h"
#include "execution/numeric_monitor.h"
#include "execution/query_scheduler.h"
#include "execution/rdma_mailbox.h"
#include "execution/result_collector.h"
#include "execution/thpt_handler.h"
#include "plan/planner.h"
#include "proto/client.pb.h"
#include "proto/master.pb.h"
#include "server/compute_worker_rpc_server.h"
#include "server/console_util.h"
#include "storage/general_histogram.h"
#include "storage/meta_store.h"
#include "storage/unique_namer.h"
#include "util/brpc_helper.h"
#include "util/config.h"

namespace AGE {
using std::flush;

// AGE worker node.
class ComputeWorker {
   public:
    // Input config file path & nodes file path.
    ComputeWorker(const Node &self_node_, const string schema_file_, bool build_with_index)
        : config(Config::GetInstance()),
          schema_file(schema_file_),
          self_node(self_node_),
          shutdown(false),
          build_with_index(build_with_index),
          msg_dispatcher(nullptr),
          exe(nullptr),
          network_monitors_(new NetworkMonitors()),
          mb(config->num_threads_, config->num_mailbox_threads_, network_monitors_),
          thpt_result_monitor_(new Monitor<thpt_result_data_t>(1E9)),
          thpt_result_timeout_monitor_(new Monitor<thpt_result_data_t>(1E9)),
          thpt_input_monitor_(new Monitor<thpt_input_data_t>(1E9)),
          op_monitor_(new OpMonitor(1E9)),
          inter_data_monitor_(new Monitor<inter_data_info_t>(1E9)) {}

    ~ComputeWorker() {
        stop();
        join();
        delete msg_dispatcher;
        delete exe;
        delete planner;
        delete meta_store;
    }

    void start() {
        RequestStrMap();
        LOG(INFO) << "Got StrMap from Meta Server";
        requestCacheWorkerTopo();

        if (Config::GetInstance()->use_rdma_mailbox_) {
            rdma_mailbox = std::make_shared<RdmaMailbox>(self_node.rank, cache_nodes);
            rdma_mailbox->init();
            mb.set_rdma_mailbox(rdma_mailbox);
        }

        LOG(INFO) << "Got CacheWorker topology from Meta Server";
        PrepareMetaStore();

        msg_dispatcher = new MsgDispatcher(self_node, cache_nodes);
        planner = new Planner(*strMap, self_node.rank);
        if (build_with_index) EnableIndexIfNeeded();
        exe = new ComputeExecutor(&mb, &rc, msg_dispatcher, planner, thpt_result_monitor_, thpt_result_timeout_monitor_,
                                  thpt_input_monitor_, op_monitor_, inter_data_monitor_);
        thpt_handler_ = std::make_shared<WorkerThptHandler>(op_monitor_, thpt_input_monitor_, thpt_result_monitor_,
                                                            thpt_result_timeout_monitor_, inter_data_monitor_, network_monitors_);
        rpc_server = new ProtoBuf::ComputeWorker::RpcServer(self_node, cache_nodes, mb, *exe, rc, planner,
                                                            thpt_handler_, thpt_result_monitor_,
                                                            thpt_result_timeout_monitor_, thpt_input_monitor_);
        if (server.AddService(rpc_server, brpc::SERVER_OWNS_SERVICE) != 0) {
            LOG(ERROR) << "Faild to add service" << flush;
            exit(-1);
        }

        // Start operator executor
        exe->start();

        // Start worker rpc server
        string addr = self_node.host + ":" + std::to_string(self_node.port);
        brpc::ServerOptions options;
        options.use_rdma = Config::GetInstance()->brpc_use_rdma_;
        if (server.Start(addr.c_str(), &options) != 0) {
            LOG(ERROR) << "Fail to start ComputeWorker::RPCServer" << flush;
            exit(-1);
        }

        reportReady();

        while (!shutdown) {
            Result r = rc.pop();
            LOG_IF(INFO, Config::GetInstance()->op_profiler_ == 2) << r.DebugString(false);
            sendResult(r);
        }
    }

    void join() {
        exe->join();
        server.Join();
    }

    void stop() {
        shutdown = true;
        exe->stop();
        server.Stop(0);
    }

   private:
    Config *config;
    const string schema_file;

    // Cluster Info
    Nodes cache_nodes;
    const Node &self_node;

    // Other Components
    bool shutdown;
    bool build_with_index;
    ResultCollector rc;
    MsgDispatcher *msg_dispatcher;
    Planner *planner;
    ComputeExecutor *exe;
    StrMap *strMap;
    MetaStore *meta_store;

    // Network monitor
    std::shared_ptr<NetworkMonitors> network_monitors_;

    // Communication
    Mailbox mb;
    std::shared_ptr<RdmaMailbox> rdma_mailbox;
    brpc::Server server;
    ProtoBuf::ComputeWorker::RpcServer *rpc_server;

    // Monitors
    std::shared_ptr<Monitor<thpt_result_data_t>> thpt_result_monitor_;
    std::shared_ptr<Monitor<thpt_result_data_t>> thpt_result_timeout_monitor_;
    std::shared_ptr<Monitor<thpt_input_data_t>> thpt_input_monitor_;
    std::shared_ptr<OpMonitor> op_monitor_;
    std::shared_ptr<Monitor<inter_data_info_t>> inter_data_monitor_;

    std::shared_ptr<WorkerThptHandler> thpt_handler_;

    void sendResult(Result &result) {
        std::shared_ptr<brpc::Channel> channel = BRPCHelper::build_channel(100, 3, result.client_host_port, false);
        ProtoBuf::Client::Service_Stub stub(channel.get());
        ProtoBuf::Client::SendResultRequest req;
        ProtoBuf::Client::SendResultResponse resp;
        brpc::Controller cntl;

        result.ToString(req.mutable_result());

        stub.SendResult(&cntl, &req, &resp, nullptr);
        BRPCHelper::LogRpcCallStatus(cntl, false);
    }

    void requestCacheWorkerTopo() {
        std::shared_ptr<brpc::Channel> channel =
            BRPCHelper::build_channel(100, 3, config->master_ip_address_, config->master_listen_port_);
        ProtoBuf::Master::Service_Stub stub(channel.get());
        ProtoBuf::Master::RequestTopoReq req;
        ProtoBuf::Master::RequestTopoResp resp;
        req.set_role(ClusterRole::CACHE);

        bool success = false;
        while (!success) {
            brpc::Controller cntl;
            stub.RequestTopo(&cntl, &req, &resp, nullptr);
            BRPCHelper::LogRpcCallStatus(cntl);
            success |= !cntl.Failed();
            usleep(1000 * 1000 * 0.5);
        }

        size_t pos = 0;
        cache_nodes.FromString(resp.data(), pos);

        // init channels from compute to cache
        vector<string> addrs;
        for (size_t i = 0; i < cache_nodes.getWorldSize(); i++) {
            addrs.emplace_back(cache_nodes.get(i).host + ":" + std::to_string(cache_nodes.get(i).port));
        }
        mb.init_channel(addrs, 1000, 3);
    }

    void reportReady() {
        std::shared_ptr<brpc::Channel> channel =
            BRPCHelper::build_channel(100, 3, config->master_ip_address_, config->master_listen_port_);
        ProtoBuf::Master::Service_Stub stub(channel.get());

        bool success = false;
        while (!success) {
            ProtoBuf::Master::ReadyRequest req;
            ProtoBuf::Master::ReadyResponse resp;
            brpc::Controller cntl;

            req.set_rank(self_node.rank);
            req.set_role(ClusterRole::COMPUTE);

            stub.Ready(&cntl, &req, &resp, nullptr);
            BRPCHelper::LogRpcCallStatus(cntl);
            success |= !cntl.Failed();

            usleep(1000 * 1000 * 0.5);
        }

        LOG(INFO) << "ComputeWorker is ready to receive query." << std::flush;
    }

    void RequestStrMap() {
        std::shared_ptr<brpc::Channel> channel =
            BRPCHelper::build_channel(1000, 3, config->master_ip_address_, config->master_listen_port_);
        ProtoBuf::Master::Service_Stub stub(channel.get());

        bool success = false;
        ProtoBuf::Master::RequestStrMapReq req;
        ProtoBuf::Master::RequestStrMapResp resp;
        while (!success) {
            brpc::Controller cntl;

            stub.RequestStrMap(&cntl, &req, &resp, nullptr);

            success |= !cntl.Failed();
            BRPCHelper::LogRpcCallStatus(cntl);

            usleep(1000 * 1000 * 0.5);
        }

        strMap = new StrMap();
        size_t pos = 0;
        strMap->FromString(resp.data(), pos);
    }

    void PrepareMetaStore() {
        meta_store = new MetaStore(*strMap, schema_file);
        {
            std::shared_ptr<brpc::Channel> channel =
                BRPCHelper::build_channel(1000, 3, config->master_ip_address_, config->master_listen_port_);
            ProtoBuf::Master::Service_Stub stub(channel.get());

            bool success = false;
            ProtoBuf::Master::CheckReadyReq req;
            ProtoBuf::Master::CheckReadyResp resp;
            req.set_role(ClusterRole::CACHE);

            while (!success) {
                brpc::Controller cntl;

                stub.CheckReady(&cntl, &req, &resp, nullptr);

                success |= resp.success();
                BRPCHelper::LogRpcCallStatus(cntl);

                usleep(1000 * 1000 * 1);
            }
        }

        // Request Histogram
        if (!config->build_histogram_) return;
        for (size_t i = 0; i < cache_nodes.getWorldSize(); i++) {
            std::shared_ptr<brpc::Channel> channel =
                BRPCHelper::build_channel(1000, 3, cache_nodes.get(i).host, cache_nodes.get(i).port);
            ProtoBuf::CacheWorker::Service_Stub stub(channel.get());
            ProtoBuf::CacheWorker::GetHistogramReq req;
            ProtoBuf::CacheWorker::GetHistogramResp resp;

            req.set_granularity(config->histogram_granularity_);
            const Config *config = Config::GetInstance();
            for (const auto &pair : config->histogram_targets_) {
                const string &label = pair.first;
                req.set_label(label);
                for (const auto &prop_or_degree : pair.second) {
                    string prop, elabel;
                    DirectionType dir;
                    MetaStore::MetaStoreKey key;
                    if ((dir = static_cast<DirectionType>(prop_or_degree.second)) == 255) {
                        prop = prop_or_degree.first;
                        req.set_prop(prop);
                        req.set_direction(dir);
                        key = MetaStore::MetaStoreKey(strMap->GetLabelId(label), strMap->GetPropId(prop));
                    } else {
                        elabel = prop_or_degree.first;
                        req.set_prop(elabel);
                        req.set_direction(dir);
                        key = MetaStore::MetaStoreKey(strMap->GetLabelId(label), strMap->GetLabelId(elabel), dir);
                    }

                    LOG(INFO) << "Requesting histograms for key: " << key.DebugString() << " from "
                              << cache_nodes.get(i).DebugString();

                    brpc::Controller cntl;
                    stub.GetHistogram(&cntl, &req, &resp, nullptr);
                    BRPCHelper::LogRpcCallStatus(cntl);
                    if (cntl.Failed()) {
                        LOG(ERROR) << "GetHistogram failed: " << cntl.ErrorText() << std::flush;
                        exit(-1);
                    }

                    GeneralHistogram ghist;
                    size_t pos = 0;
                    ghist.FromString(resp.hist(), pos);
                    meta_store->AddHistogram(key, std::move(ghist));
                }
            }
        }
    }

    void EnableIndexIfNeeded() {
        vector<string> ldbc_label_strs{"person", "person", "person", "person", "comment"};
        vector<string> ldbc_prop_strs{"id", "firstName", "lastName", "birthday", "id"};

        vector<string> amazon_label_strs{"product", "review", "product", "user"};
        vector<string> amazon_prop_strs{"id", "id", "brand", "id"};

        // first test dataset
        vector<string>* label_strs;
        vector<string>* prop_strs;
        if (strMap->GetLabelId(ldbc_label_strs[0]) != INVALID_LABEL) {
            label_strs = &ldbc_label_strs;
            prop_strs = &ldbc_prop_strs;
        } else if (strMap->GetLabelId(amazon_label_strs[0]) != INVALID_LABEL) {
            label_strs = &amazon_label_strs;
            prop_strs = &amazon_prop_strs;
        } else {
            CHECK(false) << "No valid dataset found";
            return;
        }

        for (int i = 0; i < label_strs->size(); i++) {
            LabelId lid = strMap->GetLabelId((*label_strs)[i]);
            PropId pid = strMap->GetPropId((*prop_strs)[i]);
            planner->UpdateIndexEnableSet(lid, pid, true);
            LOG(INFO) << "Enable index for " << (*label_strs)[i] << ":" << (*prop_strs)[i];
        }
    }
};

}  // namespace AGE
