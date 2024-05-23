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
#include <stdio.h>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include "base/node.h"
#include "execution/mailbox.h"
#include "execution/result_collector.h"
#include "execution/thpt_handler.h"
#include "model/rl_action.h"
#include "model/rl_state.h"
#include "model/simple_buffer.h"
#include "plan/planner.h"
#include "server/console_util.h"
#include "server/master_rpc_server.h"
#include "storage/graph.h"
#include "storage/unique_namer.h"
#include "util/config.h"

using std::flush;
using std::vector;

namespace AGE {

using MLModel::RLAction;
using MLModel::RLState;

// AGE master node.
class Master {
   public:
    // Input config file path & nodes file path.
    Master(Nodes& compute_nodes_, Nodes& cache_nodes_, Nodes& master_nodes_, bool with_rl_model, int thpt_mode)
        : config(Config::GetInstance()),
          compute_nodes(compute_nodes_),
          cache_nodes(cache_nodes_),
          master_nodes(master_nodes_),
          with_rl_model_(with_rl_model),
          thpt_mode_(thpt_mode),
          buffer_(new MLModel::SimpleBuffer(1024 * 1024, config->shared_memory_prefix_)) {
        strMap.load(_STR_MAP_FILE_(UniqueNamer(config->graph_name_).getSchemaDir(config->graph_dir_)));
    }
    ~Master() {
        stop();
        join();
        delete masterRpcServer;
        delete buffer_;
    }

    void start() {
        const Node& localNode = master_nodes.getLocalNode();
        string addr = localNode.host + ":" + std::to_string(localNode.port);
        LOG(INFO) << "Local Address: " << addr << std::flush;

        init_channels();
        thpt_handler_ = std::make_shared<MasterThptHandler>(compute_node_channels, cache_node_channels);
        masterRpcServer = new ProtoBuf::Master::MasterRpcServer(compute_nodes, cache_nodes, compute_node_channels, cache_node_channels, strMap, thpt_handler_);
        if (server.AddService(masterRpcServer, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(ERROR) << "Fail to add service" << flush;
            exit(-1);
        }

        brpc::ServerOptions options;
        options.use_rdma = Config::GetInstance()->brpc_use_rdma_;
        if (server.Start(addr.c_str(), &options) != 0) {
            LOG(ERROR) << "Fail to start MasterRpcServer" << flush;
            exit(-1);
        }

        if (thpt_mode_ == 0) {
            thpt_handler_thread_ = std::thread(&Master::ThptHandler, this);
        } else if (thpt_mode_ == 1) {
            // Currently we don't use stream mode anymore
            thpt_handler_thread_ = std::thread(&Master::StreamRLModelHandler, this);
        }

        server.RunUntilAskedToQuit();
    }

    void stop() { server.Stop(0); }
    void join() {
        server.Join();
        thpt_handler_thread_.join();
    }

    void StreamRLModelHandler() {
        /**
         * 1. The RLModel will generate request to the current state of the system
         * 2. When accepting the request, the master will request the state of all workers.
         * 3. Then the master will aggregate all infos from all workers and generate the state
         * 4. Send back to model waiting for the action.
         * 5. After receiving the action, the master will send the action to all workers for apply.
         * 6. Master will wait for a period to obtain a stable state.
         * 7. Then master request the states form all workers and send back to the model as the next state.
         * 8. Back to step 4.
         */
        while (true) {
            std::chrono::seconds dur(1);
            std::this_thread::sleep_for(dur);
            if (masterRpcServer->ClusterIsReady()) break;
        }

        if (with_rl_model_) notify_model();
        vector<string> compute_states;
        if (with_rl_model_) wait_for_rl_model();

        while (true) {
            while (true) {
                Tool::thread_sleep_sec(1);
                if (thpt_handler_->thpt_is_start()) break;
            }

            while (true) {
                wait_for_stable_state();
                if (!thpt_handler_->get_thpt_metrics(with_rl_model_, true)) {
                    exit(1);
                }

                if (with_rl_model_) {
                    // Send back the state to model
                    string state;
                    thpt_handler_->generate_state_for_rl_model(&state);
                    buffer_->write(state.c_str(), state.size());
                }

                RLAction action;
                if (with_rl_model_) {
                    wait_for_action(action);
                    send_action_to_workers(action);
                }
            }
        }
    }

    // Thpt is tested with batch mode
    void ThptHandler() {
        while (true) {
            Tool::thread_sleep_sec(1);
            if (masterRpcServer->ClusterIsReady()) break;
        }

        if (with_rl_model_) {
            notify_model();

            // Wait for action
            RLAction action;
            if (wait_for_action(action)) {
                // apply action
                send_action_to_workers(action);
            }
            notify_model();
        }

        while (true) {
            // Wait for thpt starts
            while (true) {
                Tool::thread_sleep_sec(1);
                if (thpt_handler_->thpt_is_start()) break;
            }

            LOG(INFO) << "Thpt test starts";

            // Wait for thpt send ends
            while (true) {
                Tool::thread_sleep_ms(100);
                if (thpt_handler_->thpt_is_send_end()) break;
            }

            LOG(INFO) << "Thpt send ends";

            // Try to get the thpt metrics from the compute worker
            thpt_handler_->check_thpt_finish();
            if (!thpt_handler_->get_thpt_metrics(true)) {
                if (with_rl_model_) {
                    string state;
                    thpt_handler_->set_retry();
                    thpt_handler_->generate_state_for_rl_model(&state);
                    buffer_->write(state.c_str(), state.size());
                }
                thpt_handler_->clear_monitors();
                continue;
            }

            if (with_rl_model_) {
                // Send back the state to model
                string state;
                thpt_handler_->generate_state_for_rl_model(&state);
                thpt_handler_->clear_monitors();  // must clear the monitor before the state write back
                buffer_->write(state.c_str(), state.size());

                RLAction action;
                if (wait_for_action(action)) {
                    // apply action
                    send_action_to_workers(action);
                }
                notify_model();

                continue;
            }

            thpt_handler_->clear_monitors();
        }
    }

   private:
    Config* config;
    Nodes& compute_nodes;
    Nodes& cache_nodes;
    Nodes& master_nodes;  // Reserved for upgrading to meta server
    brpc::Server server;
    StrMap strMap;
    ProtoBuf::Master::MasterRpcServer* masterRpcServer;
    vector<shared_ptr<brpc::Channel>> compute_node_channels;
    vector<shared_ptr<brpc::Channel>> cache_node_channels;

    // Thread used to handle RL model request
    bool with_rl_model_;
    int32_t thpt_mode_;  // 0: batch, 1: stream
    MLModel::SimpleBuffer* buffer_;

    std::thread thpt_handler_thread_;
    std::shared_ptr<MasterThptHandler> thpt_handler_;

    void init_channels() {
        LOG(INFO) << "Initializing channels" << std::flush;
        for (size_t i = 0; i < compute_nodes.getWorldSize(); i++) {
            string addr = compute_nodes.get(i).host + ":" + std::to_string(compute_nodes.get(i).port);
            compute_node_channels.emplace_back(BRPCHelper::build_channel(5000, 3, addr));
        }

        for (size_t i = 0; i < cache_nodes.getWorldSize(); i++) {
            string addr = cache_nodes.get(i).host + ":" + std::to_string(cache_nodes.get(i).port);
            cache_node_channels.emplace_back(BRPCHelper::build_channel(5000, 3, addr));
        }
    }

    void notify_model() { buffer_->system_ready(); }

    void wait_for_rl_model() {
        LOG(INFO) << "Waiting for RL model" << std::flush;
        buffer_->notified();
    }

    void request_states(vector<string>& compute_states) {
        while (true) {
            compute_states.clear();
            // Compute Worker States
            for (size_t i = 0; i < compute_nodes.getWorldSize(); i++) {
                shared_ptr<brpc::Channel> ch = compute_node_channels[i];
                ProtoBuf::ComputeWorker::Service_Stub stub(ch.get());
                ProtoBuf::ComputeWorker::GetRLStateReq req;
                ProtoBuf::ComputeWorker::GetRLStateResp resp;
                brpc::Controller cntl;
                // LOG(INFO) << "Requesting state from compute worker " << i << std::flush;
                stub.GetRLState(&cntl, &req, &resp, nullptr);

                if (cntl.Failed()) {
                    LOG(WARNING) << cntl.ErrorText();
                    continue;
                }

                if (resp.success()) {
                    compute_states.emplace_back(resp.state());
                }
            }

            if (compute_states.size() == compute_nodes.getWorldSize()) break;

            // TODO(cjli): Cache Worker States
        }
    }

    bool generate_states(const vector<string>& compute_states, RLState& final_state) {
        LOG(INFO) << "Generating states with " << compute_states.size() << " states" << std::flush;
        final_state.reset();
        if (!compute_states.size()) return false;
        string delimiter = "|";
        for (size_t i = 0; i < compute_states.size(); i++) {
            RLState state;
            size_t pos = 0;
            state.FromString(compute_states[i], pos);
            final_state.merge(state);
        }
        final_state.settle_down(compute_states.size());
        // LOG(INFO) << final_state.print(delimiter) << std::flush;
        return true;
    }

    void send_states_to_model(RLState& state) {
        LOG(INFO) << "Sending states to model";
        string del = "|";
        LOG(INFO) << state.print(del) << std::flush;
        string data;
        state.ToString(&data);
        buffer_->write(data.c_str(), data.size());
    }

    bool wait_for_action(RLAction& rl_action) {
        LOG(INFO) << "Waiting for action" << std::flush;
        char action_data[16];
        buffer_->read(action_data);
        uint64_t bs;
        uint64_t ms;
        memcpy(&bs, action_data, 8);
        memcpy(&ms, action_data + 8, 8);
        LOG(INFO) << "Got batch size: " << bs << " and message size " << ms << std::flush;
        if (bs == 0) {
            // The first step, use default config
            LOG(INFO) << "Got batch size: " << bs << " for first step" << std::flush;
            return false;
        }
        rl_action.set_batch_size(bs);
        rl_action.set_message_size(ms);
        return true;
    }

    void send_action_to_workers(RLAction& action) {
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
    }

    void wait_for_stable_state() {
        LOG(INFO) << "Wait for the stable state" << std::flush;
        std::chrono::seconds dur(Config::GetInstance()->model_data_collection_duration_sec_);
        std::this_thread::sleep_for(dur);
    }
};

}  // namespace AGE
