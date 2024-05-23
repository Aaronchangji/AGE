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
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>
#include "execution/mailbox.h"
#include "execution/op_profiler.h"

using std::pair;
using std::string;
using std::vector;
namespace AGE {

// Metadata of a query
class QueryMeta {
   public:
    QueryMeta() {}
    QueryMeta(string query_str, u64 proc_time, const string& client_host_port, vector<string>&& return_str,
              bool is_thpt_test = false)
        : proc_time_(proc_time),
          query_str_(query_str),
          client_host_port_(client_host_port),
          return_str_(std::move(return_str)),
          is_thpt_test_(is_thpt_test) {}
    QueryMeta(RawQuery rawQuery, vector<string>&& returnStrs)
        : QueryMeta(rawQuery.query_, rawQuery.start_time_, rawQuery.client_host_port_, std::move(returnStrs),
                    rawQuery.is_thpt_) {}

    // Machine local time of the beginning of query processing.
    u64 proc_time_;
    u64 batchwidth_ = 0;

    string query_str_;
    string client_host_port_;

    vector<pair<string, OpPerfStat>> perf_stats_;
    vector<string> return_str_;

    bool is_thpt_test_;
    bool is_heavy_ = false;
};
}  // namespace AGE
