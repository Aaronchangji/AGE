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

#include "util/thpt_test_helper.h"

namespace AGE {

void ThptTestHelper::Init(string config_file_path) {
    compute_workers_.resize(0);

    config_.ReadFile(config_file_path);
    LOG(INFO) << config_.DebugString() << std::flush;
    queries_.resize(config_.queryTemplates.size());
    timed_queries_.resize(config_.kClient);
    query_distribution_.resize(config_.queryTemplates.size(), 0);
    running_queries_with_type_.resize(config_.kClient, 0);
    completed_queries_.resize(config_.kClient);
    kCompletedQueries_.resize(config_.kClient, 0);
    kTimeoutQueries_.resize(config_.kClient);
    for (auto& v : kTimeoutQueries_) {
        v.resize(config_.queryTemplates.size(), 0);
    }
    latencies_.resize(config_.kClient);
    for (auto& l : latencies_) {
        l.resize(config_.queryTemplates.size());
    }
    durations_.resize(config_.kClient, 0);

    kReadyClients_ = 0;
    kFinishedClients_ = 0;
}

void ThptTestHelper::init_compute_workers(vector<string> hosts, vector<int> ports) {
    CHECK(hosts.size() == ports.size());
    for (size_t i = 0; i < hosts.size(); i++) {
        compute_workers_.emplace_back(Node(hosts[i], ports[i]));
    }
    kSentQueries_.resize(config_.kClient);
    for (auto& v : kSentQueries_) {
        v.resize(compute_workers_.size(), 0);
    }
}
bool ThptTestHelper::init_seeds(string prop) {
    if (!record_seeds_) return false;

    if (seeds.count(prop) == (size_t)0) {
        LOG(INFO) << "Init seed map for " << prop << std::flush;
        seeds.insert(std::make_pair(prop, vector<string>{}));
        return true;
    }
    return false;
}

void ThptTestHelper::add_random_query(int i, string&& query) {
    CHECK_LT(i, (int)queries_.size());
    queries_[i].emplace_back(std::move(query));
}

void ThptTestHelper::add_timed_random_query(std::chrono::nanoseconds arrival_time, int qt_idx, string&& query,
                                            int cid) {
    timed_queries_[cid].emplace_back(std::make_tuple(arrival_time, qt_idx, std::move(query)));
}

void ThptTestHelper::add_seeds(string prop, string seed) {
    CHECK_NE(seeds.count(prop), (size_t)0);
    seeds.at(prop).emplace_back(seed);
}
int ThptTestHelper::get_random_query(int rank, string& ret, int i, bool record) {  // return the query template id
    int rand_query_templ_id = 0;
    int rand_num = rank % config_.totalWeight;
    while (true) {
        rand_num -= config_.queryTemplates[rand_query_templ_id].weight;
        if (rand_num < 0) {
            break;
        }
        rand_query_templ_id++;
    }
    CHECK_LT(rand_query_templ_id, (int)queries_.size());
    if (record) query_distribution_[rand_query_templ_id]++;
    // running_queries_with_type_[rank] = rand_query_templ_id;
    if (!random_seed_) {
        ret = queries_[rand_query_templ_id][i % queries_[rand_query_templ_id].size()];
    } else {
        ret = queries_[rand_query_templ_id][rand_generator_() % queries_[rand_query_templ_id].size()];
    }
    return rand_query_templ_id;
}

void ThptTestHelper::get_random_query_with_time(string& query, std::chrono::nanoseconds& time, int& qt_idx, int idx,
                                                int cid) {
    auto p = timed_queries_.at(cid).at(idx % timed_queries_.size());
    time = std::get<0>(p);
    qt_idx = std::get<1>(p);
    query = std::get<2>(p);
}

uint32_t ThptTestHelper::get_num_completed_queries() {
    uint32_t ret = 0;
    for (auto& k : kCompletedQueries_) {
        ret += k;
    }
    return ret;
}
uint32_t ThptTestHelper::get_num_timeout_queries() {
    uint32_t ret = 0;
    for (auto& v : kTimeoutQueries_) {
        for (auto& k : v) ret += k;
    }
    return ret;
}
void ThptTestHelper::get_num_timeout_queries_by_qt(vector<uint32_t>& ret) {
    ret.resize(config_.queryTemplates.size(), 0);
    for (auto& v : kTimeoutQueries_) {
        for (size_t i = 0; i < v.size(); i++) {
            ret[i] += v[i];
        }
    }
}
vector<string> ThptTestHelper::get_all_queries() {
    vector<string> ret;
    for (auto& v : completed_queries_) {
        for (auto& p : v) ret.emplace_back(p.first);
    }
    return ret;
}
vector<pair<string, uint64_t>> ThptTestHelper::get_all_queries_with_latency() {
    vector<pair<string, uint64_t>> ret;
    for (auto& v : completed_queries_) {
        ret.insert(ret.end(), v.begin(), v.end());
    }
    return ret;
}
vector<uint64_t> ThptTestHelper::get_latencies() {
    vector<uint64_t> ret;
    for (auto& lat : latencies_) {
        for (auto& l : lat) {
            ret.insert(ret.end(), l.begin(), l.end());
        }
    }
    return ret;
}
vector<uint64_t> ThptTestHelper::get_latencies(size_t qt_idx) {
    CHECK_LT(qt_idx, config_.queryTemplates.size());
    vector<uint64_t> ret;
    for (auto& lat : latencies_) {
        ret.insert(ret.end(), lat[qt_idx].begin(), lat[qt_idx].end());
    }
    return ret;
}
uint64_t ThptTestHelper::get_duration() {
    uint64_t ret = 0;
    for (auto& dur : durations_) {
        if (ret < dur) {
            ret = dur;
        }
    }
    return ret;
}
bool ThptTestHelper::queries_not_empty() {
    for (auto& qs : queries_) {
        if (qs.size() == 0) {
            return false;
        }
    }
    return true;
}
string ThptTestHelper::get_running_queries() {
    map<int, int> qt_cnt;
    for (auto& qt : running_queries_with_type_) {
        qt_cnt[qt]++;
    }
    string ret;
    for (auto& p : qt_cnt) {
        ret += std::to_string(p.first) + ":" + std::to_string(p.second) + "\t";
    }
    return ret;
}

void ThptTestHelper::record_query(string& query, int rank, uint64_t lat) {
    completed_queries_[rank].emplace_back(std::make_pair(query, lat));
}

void ThptTestHelper::single_query_end(uint64_t lat, int rank, int qt_idx) {
    kCompletedQueries_[rank]++;
    latencies_[rank][qt_idx].emplace_back(lat);
}
void ThptTestHelper::single_query_timeout(int rank, int qt_idx, string& query) { kTimeoutQueries_[rank][qt_idx]++; }

void ThptTestHelper::report_ready() {
    kReadyClients_++;
    LOG(INFO) << kReadyClients_.load() << "/" << config_.kClient << " clients are ready" << std::flush;
}
void ThptTestHelper::report_end() { kFinishedClients_++; }
void ThptTestHelper::report_end(uint64_t duration, int rank) {
    kFinishedClients_++;
    LOG(INFO) << kFinishedClients_.load() << "/" << config_.kClient << " clients finish" << std::flush;
    durations_[rank] = duration;
}
}  // namespace AGE
