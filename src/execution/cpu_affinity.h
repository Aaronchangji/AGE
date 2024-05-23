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

#include <glog/logging.h>
#include <sched.h>
#include <stdio.h>
#include <algorithm>
#include <fstream>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

// Multi-core cpu affinity
namespace AGE {

using std::map;
using std::set;
using std::string;
using std::vector;

class CPUAffinity {
   public:
    CPUAffinity() { GetProcessors(); }
    using ThreadId = int;
    using ProcessorId = int;

    // Return
    //  true --> bind to core success
    //  false --> bind to core fail
    bool BindToCore(ThreadId tid) {
        std::lock_guard<std::mutex> lck(mu);

        if (idleProcessors.empty()) return false;
        if (threadProcessorMap.count(tid) == 0) {
            threadProcessorMap[tid] = idleProcessors.back();
            idleProcessors.pop_back();
        }

        ProcessorId processor = threadProcessorMap[tid];

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(processor, &cpuset);
        if (sched_setaffinity(0, sizeof(cpuset), &cpuset)) {
            VLOG(1) << "sched_setaffinity failed!\n";
            return false;
        }

        return true;
    }

   private:
    void GetProcessors() {
        std::ifstream ifs("/proc/cpuinfo", std::ifstream::in);
        if (!ifs.is_open()) {
            VLOG(1) << "Read CPU Info failed, no cpu affinity is set!";
            return;
        }

        string key;
        while (ifs >> key) {
            if (key.compare("processor") != 0) continue;
            ifs >> key;
            ProcessorId processor;
            ifs >> processor;
            idleProcessors.emplace_back(processor);
        }

        srand(time(NULL));
        std::random_shuffle(idleProcessors.begin(), idleProcessors.end());
    }

    vector<ProcessorId> idleProcessors;
    map<ThreadId, ProcessorId> threadProcessorMap;
    std::mutex mu;
};

}  // namespace AGE
