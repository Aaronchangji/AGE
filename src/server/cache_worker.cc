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

#include <brpc/controller.h>
#include <brpc/protocol.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "base/type.h"
#include "server/cache_worker.h"
#include "util/config.h"
#include "util/tool.h"

namespace brpc {
DECLARE_int64(socket_max_unwritten_bytes);
DECLARE_int32(defer_close_second);
};  // namespace brpc

DEFINE_string(config, AGE_ROOT "/config/conf.ini.cache", "config path");
DEFINE_string(worker, AGE_ROOT "/config/worker_nodes.txt", "node list path");
DEFINE_string(remark, "", "remark string");
DEFINE_int32(rank, -1, "cahce worker rank");
// DEFINE_int32(port, -1, "cache worker rpc listen port");
DEFINE_string(schema, AGE_ROOT "/config/schema", "schema file path");
DEFINE_bool(index, false, "whether build index initially");

static bool validateRank(const char *flag, int rank) { return rank >= 0; }
DEFINE_validator(rank, &validateRank);

using AGE::CacheWorker;
using AGE::ClusterRole;
using AGE::Config;
using AGE::Nodes;

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Load config.
    Config *config = Config::GetInstance();
    config->Init(FLAGS_config);
    LOG(INFO) << "\n" << config->DebugString();
    brpc::FLAGS_max_body_size = config->brpc_max_body_size_;
    brpc::FLAGS_socket_max_unwritten_bytes = config->brpc_socket_max_unwritten_bytes_;
    brpc::FLAGS_defer_close_second = 10;

    // Cache Nodes
    Nodes cache_nodes(FLAGS_worker, ClusterRole::CACHE, FLAGS_rank);

    // Worker
    CacheWorker worker(cache_nodes, FLAGS_schema, FLAGS_index);
    worker.Start();

    return 0;
}
