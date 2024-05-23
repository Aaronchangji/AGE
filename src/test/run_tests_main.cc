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
#include "base/node.h"
#include "gtest/gtest.h"
#include "test/test_tool.h"
#include "util/config.h"

using AGE::Config;

GTEST_API_ int main(int argc, char **argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    ::testing::InitGoogleTest(&argc, argv);
    GTEST_FLAG_SET(death_test_style, "threadsafe");

    Config *config = Config::GetInstance();
    config->Init(AGE_ROOT "/config/conf.ini");
    config->standalone = true;
    CHECK_EQ(config->op_profiler_, 0) << "Unit Test NOT compatiable with profiler right now";
    return RUN_ALL_TESTS();
}
