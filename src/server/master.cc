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
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "server/master.h"
#include "util/config.h"
#include "util/tool.h"

namespace brpc {
DECLARE_int64(socket_max_unwritten_bytes);
};

DEFINE_string(config, AGE_ROOT "/config/conf.ini", "config path");
DEFINE_string(master, AGE_ROOT "/config/master_nodes.txt", "master node list path");
DEFINE_string(worker, AGE_ROOT "/config/worker_nodes.txt", "worker node list path");

DEFINE_string(remark, "", "remark string");
DEFINE_int32(rank, -1, "node rank");

DEFINE_bool(rlmodel, false, "whether this run is assisted by rl model");
DEFINE_int32(thpt_mode, 0, "using batch:0 or stream:1 mode for thpt test");

static bool validateRank(const char *flag, int rank) { return rank >= 0; }
DEFINE_validator(rank, &validateRank);

using AGE::Config;
using AGE::Master;
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

    // Init Computing Nodes
    Nodes master_nodes(FLAGS_master, AGE::ClusterRole::MASTER, FLAGS_rank);
    Nodes compute_nodes(FLAGS_worker, AGE::ClusterRole::COMPUTE);
    Nodes cache_nodes(FLAGS_worker, AGE::ClusterRole::CACHE);

    // Start master
    Master *master = new Master(compute_nodes, cache_nodes, master_nodes, FLAGS_rlmodel, FLAGS_thpt_mode);
    master->start();
    delete master;

    return 0;
}
