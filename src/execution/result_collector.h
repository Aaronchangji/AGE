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
#include <string>
#include <utility>
#include <vector>
#include "base/row.h"
#include "base/serializer.h"
#include "base/type.h"
#include "execution/mailbox.h"
#include "execution/op_profiler.h"
#include "execution/query_meta.h"
#include "util/tool.h"

using std::string;

namespace AGE {
struct Result {
   private:
    bool is_success;
    bool is_thpt_test = false;

   public:
    QueryId qid;
    std::vector<Row> data;
    std::vector<string> return_str;

    // Inner system process time, do not including query send & recv between client and server
    uint64_t proc_time;
    vector<pair<string, OpPerfStat>> perf_stats;

    string client_host_port;

    Result() : is_success(false) {}

    Result(const string& parseError, string& client_host_port)
        : is_success(false), qid(0), data(1), proc_time(0), client_host_port(std::move(client_host_port)) {
        data.back().emplace_back(T_STRING, parseError);
    }
    Result(const string& parseError, string&& client_host_port)
        : is_success(false), qid(0), data(1), proc_time(0), client_host_port(std::move(client_host_port)) {
        data.back().emplace_back(T_STRING, parseError);
    }

    // We assume QueryMeta will be dropped after this constructor
    Result(QueryId qid, const std::vector<Row>&& data, QueryMeta&& qm)
        : is_success(true),
          is_thpt_test(qm.is_thpt_test_),
          qid(qid),
          data(std::move(data)),
          return_str(std::move(qm.return_str_)),
          proc_time(Tool::getTimeNs() / 1000 - qm.proc_time_),
          perf_stats(std::move(qm.perf_stats_)),
          client_host_port(std::move(qm.client_host_port_)) {}
    Result(QueryId qid, QueryMeta&& qm)
        : is_success(true),
          is_thpt_test(qm.is_thpt_test_),
          qid(qid),
          return_str(std::move(qm.return_str_)),
          proc_time(Tool::getTimeNs() / 1000 - qm.proc_time_),
          perf_stats(std::move(qm.perf_stats_)),
          client_host_port(std::move(qm.client_host_port_)) {}

    string DebugString(bool to_console) const {
        if (!IsSuccess()) return ErrorInfoStr();
        return ReturnSchemaStr() + "\n" + ResultDataStr(to_console) + "\n" + ExecuteTimeStr();
    }

    bool IsSuccess() const { return is_success; }
    bool IsThroughputTest() const { return is_thpt_test; }

    void ToString(string* s) const {
        Serializer::appendVar(s, qid);
        Serializer::appendU64(s, proc_time);
        Serializer::appendBool(s, is_thpt_test);
        Serializer::appendBool(s, is_success);

        Serializer::appendU32(s, perf_stats.size());
        for (const auto& stat : perf_stats) {
            Serializer::appendStr(s, stat.first);
            stat.second.ToString(s);
        }

        Serializer::appendU32(s, return_str.size());
        for (const string& str : return_str) Serializer::appendStr(s, str);

        Serializer::appendU32(s, data.size());
        for (const Row& r : data) r.ToString(s);
    }

    void FromString(const string& s, size_t& pos) {
        data.clear();
        Serializer::readVar(s, pos, &qid);
        proc_time = Serializer::readU64(s, pos);
        is_thpt_test = Serializer::readBool(s, pos);
        is_success = Serializer::readBool(s, pos);

        u32 len = Serializer::readU32(s, pos);
        perf_stats.resize(len);
        for (u32 i = 0; i < len; i++) {
            perf_stats[i].first = Serializer::readStr(s, pos);
            perf_stats[i].second.FromString(s, pos);
        }

        len = Serializer::readU32(s, pos);
        while (len--) return_str.emplace_back(Serializer::readStr(s, pos));

        len = Serializer::readU32(s, pos);
        while (len--) data.emplace_back(Row::CreateFromString(s, pos));
    }

    static Result CreateFromString(const string& s, size_t& pos) {
        Result r;
        r.FromString(s, pos);
        return r;
    }

   private:
    string ErrorInfoStr() const {
        assert(data.size() == 1ull && data[0].size() == 1ull && data[0][0].type == T_STRING);
        return data[0][0].stringVal;
    }

    string ReturnSchemaStr() const {
        if (return_str.size() == 0ull) return "";
        string ret = "| ";
        for (const string& str : return_str) ret += str + " | ";
        return ret;
    }

    string ResultDataStr(bool to_console) const {
        if (data.size() == 0ull) return "Empty\n";
        string ret = "[Query Output] (Size: " + to_string(data.size()) + ")\n";
        u32 output_counter = Config::GetInstance()->max_log_data_size_;
        for (auto& r : data) {
            if (!to_console && output_counter-- == 0) break;
            ret += r.DebugString() + '\n';
        }
        return ret;
    }

    string ExecuteTimeStr() const {
        string ret = "";
        for (const auto& p : perf_stats) {
            ret += "[" + p.first + " Process Stat]";
            ret += p.second.DebugString();
        }
        ret += "[Query Process Time] " + Tool::double2str(proc_time / 1000.0, 2) + " ms\n";
        return ret;
    }
};

class ResultCollector {
   public:
    template <typename... Args>
    void insert(Args&&... args) {
        reply_queue_.enqueue(Result(std::forward<Args>(args)...));
    }
    Result pop() {
        Result res;
        reply_queue_.wait_dequeue(res);
        return res;
    }
    size_t size() const { return reply_queue_.size_approx(); }

   private:
    BlockingConcurrentQueue<Result> reply_queue_;
};
}  // namespace AGE
