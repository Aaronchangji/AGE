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

#include "server/client.h"
#include <gflags/gflags.h>
#include <glog/logging.h>
#include "base/node.h"
#include "util/config.h"
#include "util/tool.h"

namespace brpc {
DECLARE_int32(defer_close_second);
}

DEFINE_string(config, AGE_ROOT "/config/conf.ini", "config path");
DEFINE_int32(port, 11010, "Client listen port");

using AGE::Config;
using AGE::Node;

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Load config
    Config *config = Config::GetInstance();
    config->Init(FLAGS_config);
    brpc::FLAGS_defer_close_second = 10;

    // Load nodes
    Node master_node(config->master_ip_address_, config->master_listen_port_);

    AGE::Client client(FLAGS_port, master_node);
    client.start();
    client.join();

    return 0;
}
