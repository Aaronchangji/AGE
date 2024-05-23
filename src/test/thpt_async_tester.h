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
#include <cmath>
#include <fstream>
#include <iostream>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/node.h"
#include "execution/cpu_affinity.h"
#include "proto/compute_worker.pb.h"
#include "proto/master.pb.h"
#include "util/brpc_helper.h"
#include "util/thpt_test_helper.h"

namespace AGE {

using std::ifstream;
using std::thread;
using std::tuple;
using std::vector;

class ThptAsyncTester {
   public:
    ThptAsyncTester(ThptTestHelper& helper, const Node& master, int base_port, string output_prefix)
        : helper(helper), master(master), output_prefix(output_prefix) {}
    ~ThptAsyncTester() { Join(); }

    void Start() {
        prepare_thpt_test();
        // Should clear the monitor to avoid last time some unexpected behavior of system
        clear_monitor();
        announce_start();

        int num_threads = helper.get_config().kClient;
        real_input_qps.resize(num_threads);
        for (int i = 0; i < num_threads; i++) {
            thread_pool.emplace_back(&ThptAsyncTester::throughput_test, this, i);
        }

        while (!helper.all_client_end()) {
            sleep(1);
        }

        announce_send_end();
    }

    void Join() {
        for (auto& th : thread_pool) {
            th.join();
        }
    }

   private:
    ThptTestHelper& helper;
    const Node& master;
    std::shared_ptr<brpc::Channel> master_channel;
    vector<thread> thread_pool;
    vector<std::shared_ptr<brpc::Channel>> channels;
    string output_prefix;
    vector<vector<int>> real_input_qps;

    CPUAffinity cpu_affinity;

    void throughput_test(int rank) {
        cpu_affinity.BindToCore(rank);
        LOG(INFO) << "Client " << rank << " start thpt testing";
        vector<int>& real_qps = real_input_qps.at(rank);
        const size_t num_compute_workers = helper.get_nodes().size();
        ThptTestConfig& config = helper.get_config();
        int query_cnt = 0, period_query_cnt = 0, sleep_cnt = 0;
        auto startT = Tool::getTimeNs(), printT = Tool::getTimeNs();
        vector<tuple<std::chrono::nanoseconds, int, string>>& queries = helper.get_timed_queries(rank);
        auto ts = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < queries.size(); i++) {
            auto& p = queries[i];
            ts += std::get<0>(p);
            int compute_worker_idx = query_cnt % num_compute_workers;
            helper.record_sent_queries(rank, compute_worker_idx);
            ProtoBuf::ComputeWorker::Service_Stub stub(channels.at(compute_worker_idx).get());
            ProtoBuf::ComputeWorker::SendQueryRequest query_req;
            ProtoBuf::ComputeWorker::SendQueryResponse query_resp;
            brpc::Controller query_cntl;

            int qt_idx = std::get<1>(p);
            query_req.set_query(std::get<2>(p));
            query_req.set_client_port(11000);
            if (config.queryTemplates.at(qt_idx).timeout) {
                query_req.set_timeout(config.queryTemplates.at(qt_idx).timeout);
            }
            query_req.set_is_throughput_test(true);
            query_req.set_query_template_idx(qt_idx);

            stub.SendQuery(&query_cntl, &query_req, &query_resp, nullptr);
            query_cnt++;
            period_query_cnt++;

            auto cur_ts = std::chrono::high_resolution_clock::now();
            if (cur_ts < ts) {
                sleep_cnt++;
                std::this_thread::sleep_until(ts);
            }

            if (Tool::getTimeNs() - printT > 1E9) {
                LOG_IF(INFO, rank == 0) << "current QPS: " << period_query_cnt;
                real_qps.emplace_back(period_query_cnt);
                printT = Tool::getTimeNs();
                period_query_cnt = 0;
            }
        }
        auto duration = Tool::getTimeNs() - startT;
        auto thpt = query_cnt / ((double)duration / 1E9);

        LOG(INFO) << "Client " << rank << " finishes thpt testing with total input qps: " << thpt;
        LOG(INFO) << "Client " << rank << " sleeped " << sleep_cnt << " times";
        helper.report_end();
    }

    bool prepare_thpt_test() {
        master_channel = BRPCHelper::build_channel(10000, 3, master.host, master.port);
        // Send throutput test request to mate server
        ProtoBuf::Master::Service_Stub stub(master_channel.get());

        {
            ProtoBuf::Master::RequestAllWorkerReq req;
            ProtoBuf::Master::RequestAllWorkerResp resp;
            brpc::Controller cntl;
            stub.RequestAllWorkers(&cntl, &req, &resp, nullptr);

            if (cntl.Failed()) {
                LOG(ERROR) << cntl.ErrorText() << flush;
                return false;
            }

            if (!resp.success()) {
                LOG(ERROR) << "ProtoBuf::Master::RequestAllWorkerResp.success() fails" << flush;
                return false;
            }

            vector<string> hosts;
            vector<int> ports;
            CHECK(resp.hosts_size() == resp.ports_size());
            for (int i = 0; i < resp.hosts_size(); i++) {
                hosts.emplace_back(resp.hosts(i));
                ports.emplace_back(resp.ports(i));
            }
            helper.init_compute_workers(hosts, ports);

            // init brpc channels
            for (auto& node : helper.get_nodes()) {
                string addr = node.host + ":" + std::to_string(node.port);
                channels.emplace_back(BRPCHelper::build_channel(1000, 3, addr));
            }
        }

        if (helper.generate_trace()) {
            // Generate or read seeds
            for (int i = 0; i < (int)helper.get_config().queryTemplates.size(); i++) {
                string query_template = helper.get_config().queryTemplates[i].tmpl;
                int randPos = query_template.find("$RAND");
                if (randPos == string::npos) {
                    continue;
                }

                string seed_prop = helper.get_config().queryTemplates[i].place_holder + "_" +
                                   helper.get_config().queryTemplates[i].place_holder_label;

                if (!helper.init_seeds(seed_prop)) continue;  // seeds already exists

                if (!helper.using_random_seed()) {
                    string seedfile = helper.get_config().get_seed_fn(helper.get_trace_folder(), seed_prop);
                    ifstream ifs(seedfile);
                    if (ifs.is_open()) {
                        string line;
                        while (std::getline(ifs, line)) {
                            helper.add_seeds(seed_prop, line.c_str());
                        }
                        continue;
                    }
                }

                ProtoBuf::Master::RandomQueriesReq req;
                ProtoBuf::Master::RandomQueriesResp resp;
                brpc::Controller cntl;

                helper.get_config().ToString(req.mutable_config(), i);
                stub.GetRandomQueries(&cntl, &req, &resp, nullptr);

                if (cntl.Failed()) {
                    LOG(ERROR) << cntl.ErrorText() << flush;
                    return false;
                }

                if (!resp.success()) {
                    LOG(ERROR) << "ProtoBuf::Master::RandomQueriesResp.success() fails" << flush;
                    return false;
                }

                LOG(INFO) << "Got " << resp.seeds_size() << " random seeds for " << seed_prop;
                for (auto& seed : resp.seeds()) {
                    helper.add_seeds(seed_prop, seed);
                }
            }

            // Generate trace
            ThptTestConfig& config = helper.get_config();
            std::mt19937 rng(time(nullptr));
            std::unordered_map<int, int> limit_dist;
            string tracefile_prefix = config.get_trace_fn(helper.get_trace_folder());
            LOG(INFO) << "Trace is dumped to " << tracefile_prefix << "_clientX";

            for (int cid = 0; cid < config.kClient; cid++) {
                string tracefile = tracefile_prefix + "_client" + std::to_string(cid);
                if (config.testMode == 0) {  // Fixed qps
                    uint64_t num_queries = config.inputQPS * config.testDuration;
                    generate_queries(num_queries, config.inputQPS, limit_dist, rng, cid);
                } else if (config.testMode == 1) {  // Changed qps
                    u32 cur_qps = config.inputQPS;
                    while (cur_qps <= config.endQPS) {
                        uint64_t num_queries = cur_qps * config.changePeriod;
                        generate_queries(num_queries, cur_qps, limit_dist, rng, cid);
                        cur_qps += config.changeStep;
                    }
                    while (cur_qps > config.inputQPS) {
                        cur_qps -= config.changeStep;
                        uint64_t num_queries = cur_qps * config.changePeriod;
                        generate_queries(num_queries, cur_qps, limit_dist, rng, cid);
                    }
                } else {
                    LOG(ERROR) << "Unsupported test mode, please check config file";
                    exit(1);
                }

                // dump trace file
                std::fstream ofs;
                try {
                    ofs.open(tracefile, std::ios::out);
                    for (auto& trace : helper.get_timed_queries(cid)) {
                        ofs << std::get<0>(trace).count() << "|" << std::get<1>(trace) << "|" << std::get<2>(trace)
                            << "\n";
                    }
                    ofs.close();
                } catch (...) {
                    LOG(ERROR) << "Cannot dump trace file";
                }
            }

            for (auto it = limit_dist.begin(); it != limit_dist.end(); it++) {
                LOG(INFO) << "Random limit " << it->first << " has " << it->second << " queries" << flush;
            }
        } else {
            // Read the trace
            auto& config = helper.get_config();
            string tracefile_prefix = config.get_trace_fn(helper.get_trace_folder());
            LOG(INFO) << "Read trace from " << tracefile_prefix + "_clientX";
            for (int cid = 0; cid < helper.get_config().kClient; cid++) {
                string tracefile = tracefile_prefix + "_client" + std::to_string(cid);
                ifstream ifs(tracefile);
                if (ifs.is_open()) {
                    string line;
                    while (std::getline(ifs, line)) {
                        vector<string> tokens = Tool::split(line, "|");
                        CHECK_EQ(tokens.size(), static_cast<u64>(3));
                        helper.add_timed_random_query(std::chrono::nanoseconds(std::stoi(tokens[0])),
                                                      std::stoi(tokens[1]), std::move(tokens[2]), cid);
                    }
                } else {
                    LOG(ERROR) << "Cannot open trace file " << tracefile;
                    exit(1);
                }
            }
        }
        return true;
    }

    void clear_monitor() {
        // Clear compute workers
        const size_t num_compute_workers = helper.get_nodes().size();
        for (size_t i = 0; i < num_compute_workers; i++) {
            ProtoBuf::ComputeWorker::Service_Stub stub(channels.at(i).get());
            ProtoBuf::AnnounceReq req;
            ProtoBuf::AnnounceResp resp;
            brpc::Controller cntl;
            stub.ClearThptMonitor(&cntl, &req, &resp, nullptr);
        }

        // Clear master
        ProtoBuf::Master::Service_Stub stub(master_channel.get());
        ProtoBuf::AnnounceReq req;
        ProtoBuf::AnnounceResp resp;
        brpc::Controller cntl;
        stub.ClearThptMonitor(&cntl, &req, &resp, nullptr);
    }

    void announce_start() {
        // To master
        ProtoBuf::Master::Service_Stub stub(master_channel.get());
        ProtoBuf::Master::ThptStartReq req;
        ProtoBuf::AnnounceResp resp;
        req.set_wait_threshold(helper.get_config().waitPeriod);
        req.set_prefix(output_prefix);
        brpc::Controller mcntl;
        stub.ThptStart(&mcntl, &req, &resp, nullptr);

        // To compute workers
        const size_t num_compute_workers = helper.get_nodes().size();
        for (size_t i = 0; i < num_compute_workers; i++) {
            ProtoBuf::ComputeWorker::Service_Stub stub(channels.at(i).get());
            ProtoBuf::AnnounceReq req;
            ProtoBuf::AnnounceResp resp;
            brpc::Controller cntl;
            stub.ThptStart(&cntl, &req, &resp, nullptr);
        }
    }

    void announce_send_end() {
        // To compute workers
        const size_t num_compute_workers = helper.get_nodes().size();
        ProtoBuf::Master::ThptSendEndReq mreq;
        for (size_t i = 0; i < num_compute_workers; i++) {
            ProtoBuf::ComputeWorker::Service_Stub stub(channels.at(i).get());
            ProtoBuf::AnnounceReq req;
            ProtoBuf::AnnounceResp resp;
            brpc::Controller cntl;
            stub.ThptSendEnd(&cntl, &req, &resp, nullptr);

            mreq.add_num_sent_queries(helper.get_num_sent_queries(i));
            LOG(INFO) << "Has sent " << helper.get_num_sent_queries(i) << " queries to Worker ";
        }

        // To master
        ProtoBuf::Master::Service_Stub stub(master_channel.get());
        ProtoBuf::AnnounceResp resp;
        brpc::Controller mcntl;
        auto& config = helper.get_config();
        if (config.testMode == 0) {
            mreq.set_send_duration(config.testDuration);
        } else if (config.testMode == 1) {
            uint64_t dur = 2 * ((config.endQPS - config.inputQPS) / config.changeStep + 1) * config.changePeriod;
            mreq.set_send_duration(dur);
        }
        stub.ThptSendEnd(&mcntl, &mreq, &resp, nullptr);
    }

    void generate_queries(u64 num_queries, u32 qps, std::unordered_map<int, int>& limit_dist, std::mt19937& rng,
                          int cid) {
        u64 cnt = 0;
        auto& config = helper.get_config();
        auto dist = Tool::ScheduleDistribution(qps);
        while (cnt < num_queries) {
            // Select template
            int rand_query_templ_id = 0;
            int rand_num = helper.get_random_num() % config.totalWeight;
            while (true) {
                rand_num -= config.queryTemplates[rand_query_templ_id].weight;
                if (rand_num < 0) {
                    break;
                }
                rand_query_templ_id++;
            }

            // Select seed
            string query = config.queryTemplates[rand_query_templ_id].tmpl;
            string seed_prop = config.queryTemplates[rand_query_templ_id].place_holder + "_" +
                               config.queryTemplates[rand_query_templ_id].place_holder_label;
            string seed =
                helper.get_seeds().at(seed_prop)[helper.get_random_num() % helper.get_seeds().at(seed_prop).size()];

            int randPos = query.find("$RAND");
            query.replace(randPos, 5, seed.c_str());

            // Replace RandLimit, if there is
            int randLimitPos = query.find("$RANDLIMIT");
            if (randLimitPos != string::npos) {
                uint32_t randLimitNum = helper.get_random_limit_num();
                if (limit_dist.count(randLimitNum) == 0) {
                    limit_dist.insert(std::make_pair(randLimitNum, 1));
                } else {
                    limit_dist[randLimitNum]++;
                }
                query.replace(randLimitPos, 10, std::to_string(randLimitNum));
            }

            // Generate time interval
            auto ti = dist(rng);  // ns
            helper.add_timed_random_query(ti, rand_query_templ_id, std::move(query), cid);

            cnt++;
        }
    }
};

}  // namespace AGE
