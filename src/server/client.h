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

#include <brpc/channel.h>
#include <brpc/server.h>
#include <time.h>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <boost/coroutine2/all.hpp>

#include "base/node.h"
#include "execution/result_collector.h"
#include "proto/compute_worker.pb.h"
#include "proto/master.pb.h"
#include "server/client_rpc_server.h"
#include "server/console_util.h"
#include "util/brpc_helper.h"
#include "util/config.h"
#include "util/thpt_test_helper.h"
#include "util/tool.h"

using std::cerr;
using std::cout;
using std::endl;
using std::flush;
using std::map;
using std::string;

namespace AGE {
class Client {
   public:
    explicit Client(int port, const Node &master)
        : port(port),
          console(ConsoleUtil::GetInstance()),
          config(Config::GetInstance()),
          master(master),
          result(nullptr),
          shutdown(false) {
        LOG(INFO) << "Listening on port:" << port << std::flush;
        initServer();
        initChannel();
    }

    ~Client() {
        stop();
        join();
    }

    void start() {
        console->SetConsoleHistory("console_history.log");
        console->SetOnQuitWrite("console_history.log");
        bool output_to_file = false;
        while (!shutdown) {
            string query = console->TryConsoleInput(">>> ");
            if (!run_single_query(query, output_to_file)) {
                continue;
            }
            if (output_to_file) {
                std::ofstream output("output.txt", std::ios::trunc);
                output << result->DebugString(true) << std::endl;
                output.close();
                output_to_file = false;
                printf("\n>>> result dumped to file output.txt\n");
            } else {
                printf("\n>>> %s\n", result->DebugString(true).c_str());
            }
            delete result;
        }
    }

    bool run_single_query(string &query, bool &output_to_file) {
        query = transQuery(query, output_to_file);
        if (!query.length()) return false;

        result = nullptr;
        string worker_host;
        uint32_t worker_port;
        if (!request_worker(worker_host, worker_port)) {
            return false;
        }

        // Call worker::SendQuery
        {
            brpc::ChannelOptions worker_channel_options;
            worker_channel_options.timeout_ms = 100;
            worker_channel_options.max_retry = 3;

            brpc::Channel worker_channel;
            if (worker_channel.Init(worker_host.c_str(), worker_port, &worker_channel_options) != 0) {
                LOG(ERROR) << "Fail to initialize channel with ComputeWorker " << worker_host << ":" << worker_port
                           << flush;
                return false;
            }
            ProtoBuf::ComputeWorker::Service_Stub stub(&worker_channel);

            // Prepare rpc call input
            ProtoBuf::ComputeWorker::SendQueryRequest sendQueryReq;
            sendQueryReq.set_query(query);
            sendQueryReq.set_client_port(port);

            // Rpc call
            brpc::Controller sendQueryCntl;
            ProtoBuf::ComputeWorker::SendQueryResponse sendQueryResp;
            stub.SendQuery(&sendQueryCntl, &sendQueryReq, &sendQueryResp, nullptr);
            if (sendQueryCntl.Failed()) {
                LOG(WARNING) << sendQueryCntl.ErrorText();
                return false;
            }

            if (!sendQueryResp.success()) {
                LOG(ERROR) << "Sending query to compute worker failed";
                return false;
            }
        }

        while (result == nullptr) {
            usleep(1000);
        }

        return true;
    }

    void stop() {
        shutdown = true;
        server.Stop(0);
    }

    void join() { server.Join(); }

    Result *get_result() { return result; }

    void start_thpt_test(boost::coroutines2::coroutine<int>::push_type &sink, ThptTestHelper *helper, int rank,
                         vector<std::shared_ptr<brpc::Channel>> channels) {
        const size_t num_compute_workers = helper->get_nodes().size();

        // Start the thpt test
        const ThptTestConfig &test_config = helper->get_config();

        int compute_worker_idx_shift = rank % 2 == 0 ? 0 : 1;

        // Warm Up
        // int query_cnt = 0;
        // uint64_t startTime = Tool::getTimeNs();
        // uint64_t endTime = Tool::getTimeNs();
        /*
        while (((endTime - startTime)) < test_config.warmupDuration * 1E9) {
            int compute_worker_idx = query_cnt % compute_workers.size();
            ProtoBuf::ComputeWorker::Service_Stub stub(channels.at(compute_worker_idx));
            ProtoBuf::ComputeWorker::SendQueryRequest query_req;
            ProtoBuf::ComputeWorker::SendQueryResponse query_resp;
            brpc::Controller query_cntl;

            string query;
            helper->get_random_query(rank, query, query_cnt);
            query_req.set_query(query);
            query_req.set_client_port(port);
            query_req.set_is_throughput_test(true);

            stub.SendQuery(&query_cntl, &query_req, &query_resp, nullptr);
            if (!query_resp.success()) continue;
            sink(1);
            result = nullptr;

            // while (result == nullptr) {
            //     usleep(1000);
            // }
            endTime = Tool::getTimeNs();
            query_cnt++;
        }
        */

        // helper->report_ready();
        // sink(1);
        // while (!helper->all_client_ready()) {
        //     usleep(100);
        // }

        // True Test
        int query_cnt = 0;
        int amp_cnt = 0;
        uint64_t startTime = Tool::getTimeNs();
        uint64_t endTime = Tool::getTimeNs();
        LOG(INFO) << "Start thpt test for client " << rank << std::flush;
        string failed_query;
        int failed_qt_idx = 0;
        bool rerun = false;
        int retry_times = 0;
        while (((endTime - startTime)) < test_config.testDuration * 1E9) {
            uint64_t single_query_timer = Tool::getTimeNs();
            int compute_worker_idx = (query_cnt + compute_worker_idx_shift) % num_compute_workers;
            ProtoBuf::ComputeWorker::Service_Stub stub(channels.at(compute_worker_idx).get());
            ProtoBuf::ComputeWorker::SendQueryRequest query_req;
            ProtoBuf::ComputeWorker::SendQueryResponse query_resp;
            brpc::Controller query_cntl;

            string query;
            int qt_idx;
            if (rerun && retry_times < 2) {
                query = std::move(failed_query);
                qt_idx = failed_qt_idx;
                rerun = false;
                retry_times++;
            } else {
                qt_idx = helper->get_random_query(rank, query, query_cnt);
                retry_times = 0;
            }
            // LOG(INFO) << "Run query: " << query << std::flush;
            query_req.set_query(query);
            query_req.set_client_port(port);
            if (test_config.queryTemplates[qt_idx].timeout) {
                query_req.set_timeout(test_config.queryTemplates[qt_idx].timeout);
            }
            query_req.set_is_throughput_test(true);

            stub.SendQuery(&query_cntl, &query_req, &query_resp, nullptr);
            if (!query_resp.success()) continue;
            sink(1);

            // while (result == nullptr) {
            //     usleep(100);
            // }

            uint64_t query_lat = result->proc_time;
            if (result->IsSuccess()) {
                helper->record_query(query, rank, result->proc_time);
                if (result->IsThroughputTest()) {
                    helper->single_query_end(result->proc_time, rank, qt_idx);
                }
            } else {
                // LOG(INFO) << "QUERY TIMEOUT";
                helper->single_query_timeout(rank, qt_idx, query);
                if (helper->get_config().rerunOnFailure) {
                    failed_query = std::move(query);
                    failed_qt_idx = qt_idx;
                    rerun = true;
                }
            }

            single_query_timer = Tool::getTimeNs() - single_query_timer;
            double amp = (double)single_query_timer / 1000.0 / query_lat;
            if (amp > 2) {
                amp_cnt++;
                // LOG_IF(INFO, rank == 0) << "Query " << query_cnt << " latency: " << query_lat << " us, overall: " <<
                // (double) single_query_timer / 1000.0
                //         << " us, amp: " << amp << ", amplification rate: " << (double) amp_cnt / query_cnt <<
                //         std::flush;
            }

            delete result;
            result = nullptr;
            endTime = Tool::getTimeNs();
            query_cnt++;
        }
        LOG(INFO) << "Client " << rank << "'s amplification rate: " << (double)amp_cnt / query_cnt << std::flush;
        helper->report_end(endTime - startTime, rank);
        return;
    }

   private:
    int port;
    ConsoleUtil *console;
    Config *config;
    const Node &master;
    Result *result;
    brpc::Channel channel;
    brpc::Server server;
    ProtoBuf::Client::ClientRpcServer *clientRpcServer;
    bool shutdown;

    void initServer() {
        clientRpcServer = new ProtoBuf::Client::ClientRpcServer(&result);
        if (server.AddService(clientRpcServer, brpc::SERVER_OWNS_SERVICE) != 0) {
            LOG(ERROR) << "Failed to add service" << flush;
            exit(-1);
        }

        butil::EndPoint point(butil::IP_ANY, port);
        brpc::ServerOptions options;
        if (server.Start(point, &options) != 0) {
            LOG(ERROR) << "Fail to start client rpc sever" << flush;
            exit(-1);
        }
    }

    void initChannel() {
        brpc::ChannelOptions options;
        options.timeout_ms = 1000;
        options.max_retry = 3;

        if (channel.Init(master.host.c_str(), config->master_listen_port_, &options) != 0) {
            LOG(ERROR) << "Fail to initialize channel" << flush;
            exit(-1);
        }
    }

    /* 1. fetch query from file if "source filepath"
       2. replace special and uppercase chars with ' ' and lowercase chars
    */
    string transQuery(string query, bool &output_to_file) {
        std::set<char> s = {' ', '\b', '\n', '\t', '\r'};

        string q = Tool::replace(query, s, ' ');
        vector<string> vec = Tool::split(q, " ");
        Tool::remove(vec, "");

        string command = Tool::toLower(vec[0]);
        if (command == "source" || command == "dump") {
            string filepath = vec[1];
            std::ifstream input(filepath.c_str());
            std::stringstream buffer;
            if (input.good()) {
                buffer << input.rdbuf();
                q = buffer.str();
                q = Tool::replace(q, s, ' ');
            } else {
                q = "";
            }
            output_to_file = command == "dump";
        } else if(command == "config") {
            uint32_t bs = std::stoi(vec[1]);
            uint32_t ms = std::stoi(vec[2]);

            // Call master::RequestWorker()
            ProtoBuf::Master::Service_Stub stub(&channel);
            ProtoBuf::Master::ConfigReq req;
            ProtoBuf::AnnounceResp resp;
            brpc::Controller cntl;

            req.set_batch_size(bs);
            req.set_message_size(ms);

            stub.Config(&cntl, &req, &resp, nullptr);

            if (cntl.Failed()) {
                LOG(ERROR) << cntl.ErrorText() << flush;
            }
            q = "";
        } else if (command == "help") {
            string s = "\nFlavius Client Usage:\n";
            s += "\tQUERY\t\tDirectly input the query.\n";
            s += "\thelp\t\tPrint help messages.\n";
            s += "\tsource FILE\tProcess the query within the file, only one query is allowed.\n";
            s += "\tdump FILE\tProcess the query within the file and dump the results into file output.txt.\n";
            s += "\tconfig BATCH_SIZE TASK_SIZE\tAdjust the batch and task size into BATCH_SIZE and TASK_SIZE.\n";
            printf("\n>>> %s\n", s.c_str());
            q = "";
        }
        return q;
    }

    bool request_worker(string &worker_host, uint32_t &worker_port) {
        // Call master::RequestWorker()
        ProtoBuf::Master::Service_Stub stub(&channel);
        ProtoBuf::Master::RequestWorkerReq req;
        ProtoBuf::Master::RequestWorkerResp resp;
        brpc::Controller cntl;

        req.set_client_port(port);
        stub.RequestWorker(&cntl, &req, &resp, nullptr);

        if (cntl.Failed()) {
            LOG(ERROR) << cntl.ErrorText() << flush;
            return false;
        }

        if (!resp.success()) {
            return false;
        }

        worker_host = resp.host();
        worker_port = resp.port();
        return true;
    }
};
}  // namespace AGE
