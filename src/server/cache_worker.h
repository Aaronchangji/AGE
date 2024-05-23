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
#include <vector>
#include "base/node.h"
#include "execution/cache_executor.h"
#include "execution/mailbox.h"
#include "execution/numeric_monitor.h"
#include "execution/rdma_mailbox.h"
#include "execution/result_collector.h"
#include "plan/planner.h"
#include "proto/master.pb.h"
#include "server/cache_worker_rpc_server.h"
#include "server/console_util.h"
#include "storage/graph.h"
#include "storage/meta_store.h"
#include "storage/unique_namer.h"
#include "util/brpc_helper.h"
#include "util/config.h"

namespace AGE {
using std::flush;

// AGE worker node.
class CacheWorker {
   public:
    // Input config file path & nodes file path.
    explicit CacheWorker(const Nodes &cacheNodes, const string schema_file, bool build_with_index)
        : rank(cacheNodes.getLocalRank()),
          port(cacheNodes.getLocalNode().port),
          schema_file(schema_file),
          config(Config::GetInstance()),
          graph(nullptr),
          executor(nullptr),
          network_monitors(new NetworkMonitors()),
          mb(config->num_threads_, config->num_mailbox_threads_, network_monitors),
          strMap(nullptr),
          workerRpcServer(nullptr),
          build_with_index(build_with_index),
          shutdown(false) {}

    ~CacheWorker() {
        delete strMap;
        delete graph;
        delete executor;
        Stop();
        Join();
    }

    void Start() {
        RequestStrMapAndComputeTopo();
        InitChannels();

        // Load graph
        graph = new Graph(UniqueNamer(config->graph_name_).getGraphPartitionDir(config->graph_dir_, rank));
        CHECK(graph->load()) << "Graph load failed!";

        // Meta Store
        metaStore = new MetaStore(*strMap, schema_file);
        if (Config::GetInstance()->build_histogram_) {
            metaStore->BuildHistogram(graph);
        }

        if (Config::GetInstance()->use_rdma_mailbox_) {
            rdma_mailbox = std::make_shared<RdmaMailbox>(rank, compute_nodes);
            rdma_mailbox->init();
            mb.set_rdma_mailbox(rdma_mailbox);
        }

        // Start operator executor
        executor = new CacheExecutor(*graph, mb, build_with_index);
        executor->Start();

        // Start worker rpc server
        workerRpcServer = new ProtoBuf::CacheWorker::RpcServer(rank, mb, *executor, *graph, metaStore);
        if (server.AddService(workerRpcServer, brpc::SERVER_OWNS_SERVICE) != 0) {
            LOG(ERROR) << "Faild to add service" << flush;
            exit(-1);
        }

        brpc::ServerOptions options;
        options.use_rdma = Config::GetInstance()->brpc_use_rdma_;
        if (server.Start(port, &options) != 0) {
            LOG(ERROR) << "Fail to start CacheWorkerRpcServer on port" << to_string(port) << flush;
            exit(-1);
        }

        ReportReady();

        server.RunUntilAskedToQuit();
    }

    void Join() {
        executor->Join();
        server.Join();
    }

    void Stop() {
        shutdown = true;
        executor->Stop();
        server.Stop(0);
    }

    Graph &getGraph() { return *graph; }

   private:
    u8 rank;
    u16 port;
    const string schema_file;
    Config *config;
    Graph *graph;
    CacheExecutor *executor;
    std::shared_ptr<NetworkMonitors> network_monitors;
    Mailbox mb;
    std::shared_ptr<RdmaMailbox> rdma_mailbox;
    StrMap *strMap;
    brpc::Server server;
    ProtoBuf::CacheWorker::RpcServer *workerRpcServer;
    MetaStore *metaStore;
    Nodes compute_nodes;
    bool build_with_index;
    bool shutdown;

    void ReportReady() {
        std::shared_ptr<brpc::Channel> ch =
            BRPCHelper::build_channel(1000, 3, config->master_ip_address_, config->master_listen_port_);
        ProtoBuf::Master::Service_Stub stub(ch.get());

        bool success = false;
        while (!success) {
            ProtoBuf::Master::ReadyRequest req;
            ProtoBuf::Master::ReadyResponse resp;
            brpc::Controller cntl;

            req.set_rank(rank);
            req.set_role(ClusterRole::CACHE);

            stub.Ready(&cntl, &req, &resp, nullptr);

            success |= !cntl.Failed();
            BRPCHelper::LogRpcCallStatus(cntl);

            usleep(1000 * 1000 * 0.5);
        }

        LOG(INFO) << "CacheWorker is ready to receive request." << std::flush;
    }

    void RequestStrMapAndComputeTopo() {
        std::shared_ptr<brpc::Channel> ch =
            BRPCHelper::build_channel(1000, 3, config->master_ip_address_, config->master_listen_port_);
        ProtoBuf::Master::Service_Stub stub(ch.get());

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

        ProtoBuf::Master::RequestTopoReq topo_req;
        ProtoBuf::Master::RequestTopoResp topo_resp;
        topo_req.set_role(ClusterRole::COMPUTE);

        success = false;
        while (!success) {
            brpc::Controller cntl;
            stub.RequestTopo(&cntl, &topo_req, &topo_resp, nullptr);
            BRPCHelper::LogRpcCallStatus(cntl);
            success |= !cntl.Failed();
            usleep(1000 * 1000 * 0.5);
        }

        pos = 0;
        compute_nodes.FromString(topo_resp.data(), pos);
    }

    void InitChannels() {
        std::shared_ptr<brpc::Channel> ch =
            BRPCHelper::build_channel(1000, 3, config->master_ip_address_, config->master_listen_port_);
        ProtoBuf::Master::Service_Stub stub(ch.get());

        bool success = false;
        ProtoBuf::Master::RequestTopoReq req;
        ProtoBuf::Master::RequestTopoResp resp;
        req.set_role(ClusterRole::COMPUTE);

        while (!success) {
            brpc::Controller cntl;
            stub.RequestTopo(&cntl, &req, &resp, nullptr);
            success |= !cntl.Failed();
            BRPCHelper::LogRpcCallStatus(cntl);
            usleep(1000 * 1000 * 0.5);
        }

        size_t pos = 0;
        Nodes compute_nodes;
        compute_nodes.FromString(resp.data(), pos);

        // init channels from compute to cache
        vector<string> addrs;
        for (size_t i = 0; i < compute_nodes.getWorldSize(); i++) {
            addrs.emplace_back(compute_nodes.get(i).host + ":" + std::to_string(compute_nodes.get(i).port));
        }
        mb.init_channel(addrs, 1000, 3);
    }
};

}  // namespace AGE
