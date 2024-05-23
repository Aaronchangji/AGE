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
#include <algorithm>
#include <cstdint>
#include <fstream>
#include <limits>
#include <map>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/serializer.h"
#include "base/type.h"
#include "util/tool.h"

namespace AGE {

using std::flush;
using std::string;
using std::vector;

class ThptTestConfig {
   public:
    struct QueryTemplate {
        string tmpl, place_holder, place_holder_label, edge_label;
        u32 weight, timeout = 0, edge_dir = std::numeric_limits<u8>::max();
    };
    ThptTestConfig() {}

    u32 testMode;
    u32 testDuration;
    u32 kQueries;
    u32 kSeedPerWorker;
    u32 inputQPS;
    u32 endQPS;
    u32 changePeriod;
    u32 changeStep;
    u32 kClient;
    u32 totalWeight = 0;
    u32 minLimitNum;
    u32 maxLimitNum;
    u32 minVertexDegree;
    u32 maxVertexDegree;
    u32 waitPeriod;
    bool rerunOnFailure;
    u32 workloadIdx;
    vector<QueryTemplate> queryTemplates;

    bool ReadFile(string& conf) {
        std::ifstream throughputConfigFile(conf);
        if (throughputConfigFile.is_open()) {
            bool read_config = true;
            try {
                string line;
                while (getline(throughputConfigFile, line)) {
                    if (line[0] == '#' || line.empty()) continue;
                    if (line[0] == '[' && line.back() == ']') {
                        read_config = line == "[Config]";
                        continue;
                    }

                    if (read_config) {
                        ParseConfig(line);
                    } else {
                        ParseQueryTemplate(line);
                    }
                }
            } catch (...) {
                LOG(INFO) << "Parse configuration file fail." << flush;
                return false;
            }
        } else {
            LOG(ERROR) << "Couldn't open throughput config file." << flush;
            return false;
        }
        throughputConfigFile.close();
        InitConfig();
        return true;
    }

    void ToString(string* s, int i) const {
        Serializer::appendU32(s, kSeedPerWorker);
        Serializer::appendU32(s, minVertexDegree);
        Serializer::appendU32(s, maxVertexDegree);
        Serializer::appendU32(s, queryTemplates[i].edge_dir);
        Serializer::appendStr(s, queryTemplates[i].place_holder);
        Serializer::appendStr(s, queryTemplates[i].place_holder_label);
        Serializer::appendStr(s, queryTemplates[i].edge_label);
    }

    void FromString(const string& s, size_t& pos) {
        Serializer::readU32(s, pos, &kSeedPerWorker);
        Serializer::readU32(s, pos, &minVertexDegree);
        Serializer::readU32(s, pos, &maxVertexDegree);
        queryTemplates.emplace_back();
        queryTemplates.back().edge_dir = Serializer::readU32(s, pos);
        queryTemplates.back().place_holder = Serializer::readStr(s, pos);
        queryTemplates.back().place_holder_label = Serializer::readStr(s, pos);
        queryTemplates.back().edge_label = Serializer::readStr(s, pos);
    }

    string DebugString() {
        using std::to_string;
        string ret = "Throughput Test Configuration:\n";
        ret += "Test Mode: " + to_string(testMode) + ".\n";
        ret += "Test Duration: " + to_string(testDuration) + " seconds.\n";
        ret += "Num Queries: " + to_string(kQueries) + ".\n";
        ret += "Num Queries per Worker: " + to_string(kSeedPerWorker) + ".\n";
        ret += "Input QPS: " + to_string(inputQPS) + ", End QPS: " + to_string(endQPS) + ".\n";
        ret += "Change Period: " + to_string(changePeriod) + " s.\n";
        ret += "Change Step: " + to_string(changeStep) + ".\n";
        ret += "#Client: " + to_string(kClient) + ".\n";
        ret += "LimitRange: [" + to_string(minLimitNum) + ", " + to_string(maxLimitNum) + "].\n";
        ret += "DegreeRange: [" + to_string(minVertexDegree) + ", " + to_string(maxVertexDegree) + "].\n";
        ret += "RerunOnFailure: " + to_string(rerunOnFailure) + ".\n";
        ret += "Wait Period: " + to_string(waitPeriod) + " seconds.\n";
        ret += "Workload Index: " + to_string(workloadIdx) + ".\n";
        ret += "Query Templates:\n";
        for (size_t i = 0; i < queryTemplates.size(); i++) {
            QueryTemplate tmpl = queryTemplates[i];
            ret += "\t" + tmpl.tmpl + "|" + tmpl.place_holder_label + "|" + tmpl.place_holder + "|" +
                   to_string(tmpl.weight);
            ret += (tmpl.timeout ? "|" + to_string(tmpl.timeout) : "");
            ret += (tmpl.edge_dir != 255 ? "|" + to_string(tmpl.edge_dir) + "|" + tmpl.edge_label : "");
            ret += "\n";
        }
        return ret;
    }

    string get_trace_fn(string trace_folder) {
        string trace_file_prefix =
            trace_folder + "/trace_mode" + std::to_string(testMode) + "_worklaod" + std::to_string(workloadIdx) + "_";
        if (testMode == 0) {
            trace_file_prefix += std::to_string(inputQPS) + "_" + std::to_string(testDuration);
        } else if (testMode == 1) {
            trace_file_prefix += std::to_string(inputQPS) + "_" + std::to_string(endQPS) + "_" +
                                 std::to_string(changePeriod) + "_" + std::to_string(changeStep);
        }
        return trace_file_prefix;
    }

    string get_seed_fn(string seed_folder, string seed_prop) {
        string seed_file = seed_folder + "/seed_" + seed_prop;
        return seed_file;
    }

   private:
    std::unordered_map<string, string> str_params;

    void ParseConfig(string& line) {
        line.erase(std::remove_if(line.begin(), line.end(), ::isspace), line.end());
        auto delimiter_pos = line.find("=");
        string name = line.substr(0, delimiter_pos);
        string value = line.substr(delimiter_pos + 1);
        str_params.insert({name, value});
    }

    void InitConfig() {
        int val, nullVal = -1;
        string s;

#define _THPT_CFG_GETINT_(key, varName, defaultVal) \
    if (str_params.count(#key)) {                   \
        val = std::stoi(str_params.at(#key));       \
    } else {                                        \
        val = nullVal;                              \
    }                                               \
    varName = (val != nullVal) ? val : defaultVal;

#define _THPT_CFG_GETBOOL_(key, varName, defaultVal) \
    if (str_params.count(#key)) {                    \
        s = str_params.at(#key);                     \
        if (std::tolower(s[0]) == 't') {             \
            val = 1;                                 \
        } else if (std::tolower(s[0]) == 'f') {      \
            val = 0;                                 \
        } else {                                     \
            val = nullVal;                           \
        }                                            \
    } else {                                         \
        val = nullVal;                               \
    }                                                \
    varName = (val != nullVal) ? val : defaultVal;

        _THPT_CFG_GETINT_(testMode, testMode, 0);
        _THPT_CFG_GETINT_(testTime, testDuration, 10);
        _THPT_CFG_GETINT_(numQueries, kQueries, 30000);
        _THPT_CFG_GETINT_(randomSeedPerWorker, kSeedPerWorker, 20000);
        _THPT_CFG_GETINT_(inputQPS, inputQPS, 3000);
        _THPT_CFG_GETINT_(endQPS, endQPS, 3000);
        _THPT_CFG_GETINT_(changePeriod, changePeriod, 10);
        _THPT_CFG_GETINT_(changeStep, changeStep, 100);
        _THPT_CFG_GETINT_(kClient, kClient, 1);
        _THPT_CFG_GETINT_(minLimitNum, minLimitNum, 100);
        _THPT_CFG_GETINT_(maxLimitNum, maxLimitNum, 10000);
        _THPT_CFG_GETINT_(minPercentage, minVertexDegree, 40);
        _THPT_CFG_GETINT_(maxPercentage, maxVertexDegree, 60);
        _THPT_CFG_GETBOOL_(enableRerun, rerunOnFailure, 0);
        _THPT_CFG_GETINT_(waitPeriod, waitPeriod, 50);
        _THPT_CFG_GETINT_(workloadIdx, workloadIdx, 0);

#undef _THPT_CFG_GETINT_
#undef _THPT_CFG_GETBOOL_
    }

    void ParseQueryTemplate(string& line) {
        static std::map<string, u32> edge_dir_map = {{"both", 0}, {"in", 1}, {"out", 2}};

        // Parse the query templates
        vector<string> ret = Tool::split(line, "|");
        CHECK_GE(ret.size(), static_cast<u64>(4));

        queryTemplates.emplace_back();
        queryTemplates.back().tmpl = ret[0];
        queryTemplates.back().place_holder_label = ret[1];
        queryTemplates.back().place_holder = ret[2];
        queryTemplates.back().weight = std::stoi(ret[3]);
        if (ret.size() >= 5) {
            queryTemplates.back().timeout = std::stoi(ret[4]);
        }
        if (ret.size() >= 6 && edge_dir_map.count(ret[5])) {
            queryTemplates.back().edge_dir = edge_dir_map[ret[5]];
        }
        if (ret.size() >= 7) {
            queryTemplates.back().edge_label = ret[6];
        }
        totalWeight += queryTemplates.back().weight;
    }
};

}  // namespace AGE
