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

#include "server/standalone.h"
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <stdlib.h>
#include <iostream>
#include "base/node.h"
#include "util/config.h"

using AGE::Config;
using AGE::Nodes;
using std::string;

DEFINE_string(config, AGE_ROOT "/config/conf.ini", "config path");

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Load config
    Config *config = Config::GetInstance();
    config->Init(FLAGS_config);

    AGE::Standalone server(config->graph_name_, config->graph_dir_, config->num_threads_, true);
    server.StartConsole();
    return 0;
}
