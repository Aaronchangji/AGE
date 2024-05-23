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
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "execution/message.h"
#include "execution/monitor.h"
#include "execution/physical_plan.h"
#include "tbb/concurrent_hash_map.h"

namespace AGE {

using std::string;
using std::unordered_map;

class BarrierUtil {
   public:
    using CounterMapType = std::unordered_map<std::string, int>;

    // Lock is guaranteed outside the function
    static bool Sync(Message& msg, CounterMapType& counter_map, std::shared_ptr<OpMonitor> monitor, int8_t stop_depth) {
        uint8_t cur_path_size = msg.header.splitHistory.size();
        CHECK_GE(cur_path_size, stop_depth) << "Barrier condition already hit";
        while (cur_path_size != stop_depth) {
            size_t last_delimiter = msg.header.splitHistory.find_last_of("\t");
            int split_counter = stoi(msg.header.splitHistory.substr(last_delimiter + 1));

            // Before syncing this split, move the perf stats to profiler & monitor
            // TODO(TBD): fix the problem when earlystop is enabled
            DumpCurrentSplitPerfStat(msg, monitor);

            auto itr = FindCounter(msg, counter_map);
            if (++(itr->second) == split_counter) {
                itr->second = 0;
                msg.header.splitHistory.resize(last_delimiter);
                cur_path_size = msg.header.splitHistory.size();
            } else {
                return false;
            }
        }
        return true;
    }

   private:
    static CounterMapType::iterator FindCounter(const Message& msg, CounterMapType& counter_map) {
        CounterMapType::iterator itr = counter_map.find(msg.header.splitHistory);
        if (itr == counter_map.end()) {
            counter_map[msg.header.splitHistory] = 0;
            itr = counter_map.find(msg.header.splitHistory);
        }
        return itr;
    }

    static void DumpCurrentSplitPerfStat(Message& msg, std::shared_ptr<OpMonitor> monitor) {
        if (monitor) monitor->DumpCurrentSplitPerfStat(msg);
        msg.header.ClearCurrentSplitPerfStat();
    }
};

}  // namespace AGE
