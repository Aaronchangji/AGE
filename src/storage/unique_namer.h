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
#include "base/math.h"
#include "base/node.h"
#include "util/config.h"

using std::string;
using std::to_string;

namespace AGE {
// Unique namer to name directories:
//  - (partitioned) data file directory:
//      {config->graph_dir_}/{hashValue}_{cache_worker_rank}
//          note that {cache_worker_rank} equal to {graph_partition_id} currently
//  - schema file directory:
//      {config->graph_dir_}/{hashValue}_s
//
//  {hashValue} is uniquely determined by:
//      1. config->graph_name
class UniqueNamer {
   public:
    explicit UniqueNamer(const string& graph_name) { Init(graph_name); }

    string getGraphPartitionDir(const string& graphdir, int rank) {
        return graphdir + "/" + hashValue + "_" + to_string(rank);
    }

    string getSchemaDir(const string& graphdir) { return graphdir + "/" + hashValue + "_s"; }

   private:
    void Init(const string& graph_name) {
        // Generate hash key by cache nodes ip list
        string hashKey = graph_name;
        hashValue = to_string(Math::hash_str(hashKey));
    }

    string hashValue;
};
}  // namespace AGE
