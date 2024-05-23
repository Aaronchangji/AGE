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

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdint>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include "base/type.h"
#include "glog/logging.h"
#include "util/tool.h"

#ifdef __cplusplus
extern "C" {
#endif

#include "./iniparser.h"

#ifdef __cplusplus
}
#endif

using std::map;
using std::pair;
using std::string;
using std::vector;

namespace AGE {
// AGE config
class Config {
   public:
    static Config* GetInstance() {
        static Config config_single_instance;
        return &config_single_instance;
    }

    void Init(const string& confPath) {
        int val, nullVal = -1;
        char *str, nullStr[] = "null";

        dictionary* ini = iniparser_load(confPath.c_str());
        if (ini == nullptr) {
            fprintf(stderr, "can not open %s\n", confPath.c_str());
            exit(-1);
        }

#define _CFG_GETINT_(prefix, key, varName, defaultVal)      \
    val = iniparser_getint(ini, #prefix ":" #key, nullVal); \
    varName = (val != nullVal) ? val : defaultVal;

#define _CFG_GETSTR_(prefix, key, varName, defaultStr)         \
    str = iniparser_getstring(ini, #prefix ":" #key, nullStr); \
    varName = (str != nullStr) ? str : defaultStr;

#define _CFG_GETBOOL_(prefix, key, varName, defaultVal)         \
    val = iniparser_getboolean(ini, #prefix ":" #key, nullVal); \
    varName = (val != nullVal) ? val : defaultVal;

        _CFG_GETSTR_(GLOBAL, GRAPH_DIR, graph_dir_, AGE_ROOT "/temp");
        _CFG_GETSTR_(GLOBAL, GRAPH_NAME, graph_name_, "LDBC Graph");
        _CFG_GETSTR_(GLOBAL, ID_MAPPER, id_mapper_type_, "hash");
        _CFG_GETINT_(GLOBAL, MASTER_LISTEN_PORT, master_listen_port_, 9158);
        _CFG_GETSTR_(GLOBAL, MASTER_IP_ADDRESS, master_ip_address_, "129.0.0.1");
        _CFG_GETINT_(GLOBAL, batch_size_sample, batch_size_sample_, 64);
        _CFG_GETINT_(GLOBAL, MAX_LOG_DATA_SIZE, max_log_data_size_, 5);
        _CFG_GETBOOL_(GLOBAL, BRPC_USE_RDMA, brpc_use_rdma_, false);
        _CFG_GETBOOL_(GLOBAL, USE_RDMA_MAILBOX, use_rdma_mailbox_, true);
        _CFG_GETINT_(GLOBAL, BRPC_MAX_BODY_SIZE, brpc_max_body_size_, 2147483647);
        _CFG_GETINT_(GLOBAL, BRPC_SOCKET_MAX_UNWRITTEN_BYTES, brpc_socket_max_unwritten_bytes_, /* 64MB*/ 67108864);
        _CFG_GETINT_(GLOBAL, VERBOSE, verbose_, 0);
        _CFG_GETINT_(GLOBAL, OP_PROFILER, op_profiler_, 0);
        _CFG_GETBOOL_(GLOBAL, USE_DFS, use_dfs_, true);
        _CFG_GETBOOL_(GLOBAL, ADAPT_BATCH_SIZE, adaptive_batch_size_, true);
        _CFG_GETBOOL_(GLOBAL, ENABLE_TIMEOUT, enable_timeout_, false);
        _CFG_GETINT_(GLOBAL, TIMEOUT_MS, timeout_ms_, /*1 min*/ 60000);

        _CFG_GETBOOL_(LOCAL, CORE_BIND, core_bind_, false);
        _CFG_GETINT_(LOCAL, NUM_THREADS, num_threads_, 4);
        _CFG_GETINT_(LOCAL, NUM_MAILBOX_THREADS, num_mailbox_threads_, 4);
        _CFG_GETBOOL_(LOCAL, ENABLE_STEAL, enable_steal_, true);
        string scheduler_policy;
        _CFG_GETSTR_(LOCAL, SCHEDULER_POLICY, scheduler_policy, "roundrobin");
        scheduler_policy_ = ParseSchedulerPolicy(scheduler_policy);
        if (scheduler_policy_ == SchedulerPolicy::INVALID) {
            LOG(ERROR) << "Invalid scheduler policy: " << scheduler_policy;
            exit(-1);
        }
        _CFG_GETINT_(LOCAL, NUM_HEAVY_THREADS, num_heavy_threads_, 1);
        CHECK_LE(num_heavy_threads_, num_threads_);
        _CFG_GETINT_(LOCAL, MESSAGE_SIZE, message_size_, 2048);
        _CFG_GETINT_(LOCAL, BATCH_SIZE, batch_size_, 16384);

        /* Metadata */
        string build_targets;
        _CFG_GETBOOL_(METADATA, BUILD_HISTOGRAM, build_histogram_, false);
        _CFG_GETINT_(METADATA, HISTOGRAM_GRANULARITY, histogram_granularity_, /* 100%*/ 100);
        _CFG_GETSTR_(METADATA, HISTOGRAM_TARGETS, build_targets, "");
        parse_histogram_info(build_targets);

        /* MLModel Related */
        _CFG_GETINT_(MODEL, MODEL_DATA_COLLECTION_DURATION_SEC, model_data_collection_duration_sec_, /*1 sec*/ 1);
        _CFG_GETINT_(MODEL, HEAVY_THRESHOLD, heavy_threshold_, 100);
        _CFG_GETINT_(MODEL, INTER_DATA_LEVEL, inter_data_level_, 0);
        _CFG_GETSTR_(MODEL, SHARED_MEMORY_PREFIX, shared_memory_prefix_, "age");

#undef _CFG_GETVAL_
#undef _CFG_GETSTR_
#undef _CFG_GETBOOL_

        init_ = true;
        iniparser_freedict(ini);
    }

    string DebugString() {
        std::stringstream ss;
        ss << "[AGE] config:\n";
        size_t keyPrintLen = 16;
#define _CFG_DEBUG_(key)                                            \
    ss << #key;                                                     \
    for (size_t i = sizeof(#key); i <= keyPrintLen; i++) ss << " "; \
    ss << ": " << key << std::endl;

        _CFG_DEBUG_(master_ip_address_);
        _CFG_DEBUG_(master_listen_port_);
        _CFG_DEBUG_(graph_dir_);
        _CFG_DEBUG_(graph_name_);
        _CFG_DEBUG_(num_threads_);
        _CFG_DEBUG_(num_mailbox_threads_);
        _CFG_DEBUG_(core_bind_);
        _CFG_DEBUG_(verbose_);
        _CFG_DEBUG_(use_dfs_);
        _CFG_DEBUG_(message_size_);
        _CFG_DEBUG_(batch_size_);
        _CFG_DEBUG_(batch_size_sample_);
        _CFG_DEBUG_(max_log_data_size_);
        _CFG_DEBUG_(brpc_use_rdma_);
        _CFG_DEBUG_(use_rdma_mailbox_);
        _CFG_DEBUG_(brpc_max_body_size_);
        _CFG_DEBUG_(brpc_socket_max_unwritten_bytes_);
        _CFG_DEBUG_(op_profiler_);
        _CFG_DEBUG_(adaptive_batch_size_);
        _CFG_DEBUG_(enable_timeout_);
        _CFG_DEBUG_(timeout_ms_);
        _CFG_DEBUG_(model_data_collection_duration_sec_);
        _CFG_DEBUG_(heavy_threshold_);
        _CFG_DEBUG_(inter_data_level_);
        _CFG_DEBUG_(shared_memory_prefix_);
        _CFG_DEBUG_(enable_steal_);

#undef _CFG_DEBUG_
        return ss.str();
    }

#define _CFG_SET_(name, type) \
    void set_##name(type v) { name##_ = v; }

    _CFG_SET_(batch_size, uint64_t)
    _CFG_SET_(message_size, uint64_t)

    string graph_dir_;
    string graph_name_;
    string id_mapper_type_;
    string master_ip_address_;
    int master_listen_port_;
    int num_threads_;
    int num_mailbox_threads_;
    bool core_bind_;
    bool init_;
    bool use_dfs_;
    std::atomic<int> message_size_;
    std::atomic<int> batch_size_;
    int batch_size_sample_;
    int max_log_data_size_;
    bool brpc_use_rdma_;
    bool use_rdma_mailbox_;
    u64 brpc_max_body_size_;
    i64 brpc_socket_max_unwritten_bytes_;
    int verbose_;
    int op_profiler_;
    bool adaptive_batch_size_;
    bool enable_steal_;
    SchedulerPolicy scheduler_policy_;
    double num_heavy_threads_;
    bool enable_timeout_;
    uint64_t timeout_ms_;
    int model_data_collection_duration_sec_;
    int heavy_threshold_;
    string shared_memory_prefix_;
    int inter_data_level_;

    bool standalone{false};

    /* Metadata related */
    bool build_histogram_;
    int histogram_granularity_;
    map<string, vector<pair<string, u8>>> histogram_targets_;

   private:
    Config(const Config&) : init_(false) {}
    Config& operator=(const Config&);
    Config() {}

    std::pair<string, u8> parse_edge_type(const string& str) {
        string elabel = Tool::split(str, '[', ']').at(0);
        DirectionType dir;
        bool find_out = str.find(">") != string::npos, find_in = str.find("<") != string::npos;
        if (find_out && !find_in)
            dir = DirectionType_OUT;
        else if (!find_out && find_in)
            dir = DirectionType_IN;
        else
            dir = DirectionType_BOTH;
        return std::make_pair(elabel, dir);
    }

    void parse_histogram_info(string targets) {
        // Remove redundent space characters. The get {label: properties} groups
        targets.erase(std::remove_if(targets.begin(), targets.end(), isspace), targets.end());
        vector<string> target_groups = Tool::split(targets, '{', '}');
        for (const string& str : target_groups) {
            vector<string> names = Tool::split(str, ":");
            CHECK_EQ(names.size(), static_cast<u64>(2));
            string label = names[0];
            vector<string> props = Tool::split(names[1], ",");

            if (!histogram_targets_.count(label)) histogram_targets_[label] = {};
            for (const string& prop : props) {
                if (prop.find("[") != string::npos && prop.find("]") != string::npos) {
                    histogram_targets_[label].emplace_back(parse_edge_type(prop));
                } else {
                    histogram_targets_[label].emplace_back(std::make_pair(prop, 255));
                }
            }
        }
    }
};
}  // namespace AGE
