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

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "base/node.h"
#include "test/thpt_async_tester.h"
#include "util/config.h"
#include "util/thpt_test_helper.h"
#include "util/tool.h"

namespace brpc {
DECLARE_int32(defer_close_second);
};

DEFINE_string(config, AGE_ROOT "/config/conf.ini", "config path");
DEFINE_string(test_config, AGE_ROOT "/throughput_test.conf", "throughput test config path");
DEFINE_string(output_prefix, AGE_ROOT "/logs/throughput.cdf", "throughput output path prefix");
DEFINE_bool(trace_generator, false, "trace generator");
DEFINE_string(trace_folder, AGE_ROOT "/trace/", "trace folder");
DEFINE_bool(random_seed, false, "random seed");
DEFINE_int32(port, 11010, "Throughput Test Port");

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

    // Load throughput test config
    std::default_random_engine generator(time(nullptr));
    AGE::ThptTestHelper helper(generator, FLAGS_trace_generator, true, FLAGS_trace_folder, FLAGS_random_seed);
    helper.Init(FLAGS_test_config);

    // Start the throughput test
    AGE::ThptAsyncTester tester(helper, master_node, FLAGS_port, FLAGS_output_prefix);
    tester.Start();

    return 0;
}
