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
#include <stdio.h>
#include <atomic>
#include <fstream>
#include <map>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/node.h"
#include "base/serializer.h"
#include "util/thpt_test_config.h"

namespace AGE {

using std::flush;
using std::map;
using std::pair;
using std::shared_mutex;
using std::string;
using std::tuple;
using std::vector;

class ThptTestHelper {
   public:
    ThptTestHelper(std::default_random_engine& generator, bool generate_trace, bool record_seeds, string trace_folder,
                   bool random_seed)
        : rand_generator_(generator),
          generate_trace_(generate_trace),
          record_seeds_(record_seeds),
          trace_folder_(trace_folder),
          random_seed_(random_seed) {}

    void Init(string config_file_path);
    void init_compute_workers(vector<string> hosts, vector<int> ports);
    bool init_seeds(string prop);

    /* Class Members */
    inline ThptTestConfig& get_config() { return config_; }
    inline vector<Node>& get_nodes() { return compute_workers_; }
    inline bool using_random_seed() { return random_seed_; }
    inline bool generate_trace() { return generate_trace_; }
    inline string get_trace_folder() { return trace_folder_; }
    std::unordered_map<string, vector<string>>& get_seeds() { return seeds; }

    /* Query and Seeds related */
    void add_random_query(int i, string&& query);
    void add_timed_random_query(std::chrono::nanoseconds arrival_time, int qt_idx, string&& query, int cid);
    void add_seeds(string prop, string seed);
    int get_random_query(int rank, string& ret, int i = 0, bool record = true);
    void get_random_query_with_time(string& ret, std::chrono::nanoseconds& time, int& qt_idx, int idx, int cid);
    uint64_t get_random_num() { return rand_generator_(); }
    uint32_t get_random_limit_num() {
        return config_.minLimitNum + rand_generator_() % (config_.maxLimitNum - config_.minLimitNum);
    }
    bool queries_not_empty();
    vector<vector<tuple<std::chrono::nanoseconds, int, string>>>& get_timed_queries() { return timed_queries_; }
    vector<tuple<std::chrono::nanoseconds, int, string>>& get_timed_queries(int cid) { return timed_queries_[cid]; }

    /* Monitoring Usage */
    /* Get */
    uint32_t get_num_completed_queries();
    uint32_t get_num_timeout_queries();
    void get_num_timeout_queries_by_qt(vector<uint32_t>& ret);
    vector<string> get_all_queries();
    inline vector<int>& get_query_distribution() { return query_distribution_; }
    vector<pair<string, uint64_t>> get_all_queries_with_latency();
    vector<uint64_t> get_latencies();
    vector<uint64_t> get_latencies(size_t qt_idx);
    uint64_t get_duration();
    string get_running_queries();
    /* Set */
    void record_query(string& query, int rank, uint64_t lat);
    void single_query_end(uint64_t lat, int rank, int qt_idx);
    void single_query_timeout(int rank, int qt_idx, string& query);

    /* Sync Client Usage */
    void report_ready();
    void report_end(uint64_t duration, int rank);
    void report_end();
    bool all_client_ready() { return kReadyClients_.load() == (int)config_.kClient; }
    bool all_client_not_ready() { return kReadyClients_.load() == 0; }
    bool all_client_end() { return kFinishedClients_.load() == (int)config_.kClient; }
    bool all_client_not_end() { return kFinishedClients_.load() == 0; }
    void end_all_clients() { kFinishedClients_ = config_.kClient; }

    /* Async Client Usage */
    void record_sent_queries(int cid, int wid) { kSentQueries_[cid][wid]++; }
    uint32_t get_num_sent_queries(int wid) {
        uint32_t ret = 0;
        for (auto& v : kSentQueries_) ret += v[wid];
        return ret;
    }

   private:
    ThptTestConfig config_;

    std::default_random_engine& rand_generator_;
    bool generate_trace_;
    bool record_seeds_;
    string trace_folder_;
    bool random_seed_;

    std::unordered_map<string, vector<string>> seeds;
    vector<vector<string>> queries_;  // randomized query candidates
    vector<vector<tuple<std::chrono::nanoseconds, int, string>>>
        timed_queries_;  // randomized query with arrival timestamp

    vector<int> query_distribution_;
    vector<int> running_queries_with_type_;
    vector<Node> compute_workers_;

    vector<uint32_t> kCompletedQueries_;        // completed queries in total
    vector<vector<uint32_t>> kTimeoutQueries_;  // timeout queries in total
    vector<vector<uint32_t>> kSentQueries_;     // sent queries in total

    /* Used for output more infos */
    vector<vector<pair<string, uint64_t>>> completed_queries_;
    vector<vector<vector<uint64_t>>> latencies_;

    // Used to sync among clients
    std::atomic<int> kReadyClients_;
    std::atomic<int> kFinishedClients_;
    vector<uint64_t> durations_;
};
}  // namespace AGE
