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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>
#include <string>
#include <vector>
#include "base/node.h"
#include "storage/graph_loader.h"
#include "util/config.h"
#include "util/tool.h"

DEFINE_string(vertices, "", "Vertex files group, separated by '||'");
DEFINE_string(edges, "", "Edge files group, separated by '||'");
DEFINE_string(config, AGE_ROOT "/config/conf.ini", "Config path");
DEFINE_string(worker, AGE_ROOT "/config/worker_nodes.txt", "Node list path");
DEFINE_string(output, "./output", "Output directory path");
DEFINE_string(delimiter, "|", "Delimiter inside csv file");

static bool validateDelimiter(const char *flag, const string &delimiter) { return delimiter.size() == 1ull; }
DEFINE_validator(delimiter, &validateDelimiter);

using AGE::ClusterRole;
using AGE::Config;
using AGE::GraphLoader;
using AGE::Nodes;
using AGE::Tool;
using std::string;
using std::vector;

#define p(x) LOG(INFO) << "\t" << #x << " : " << FLAGS_##x << std::endl;
void printArgs() {
    LOG(INFO) << "Args: {\n";
    p(config);
    p(worker);
    p(output);
    p(delimiter);
    p(vertices);
    p(edges);
    LOG(INFO) << "}\n";
}
#undef p

void printFileGroups(const vector<vector<string>> &vGroups, const vector<vector<string>> &eGroups) {
    // Print vGroups / eGroups
    LOG(INFO) << "Vertex file groups:";
    for (auto &vec : vGroups) {
        LOG(INFO) << "{\n";
        for (auto &s : vec) LOG(INFO) << "\t" << s << ",\n";
        LOG(INFO) << "}\n";
    }
    LOG(INFO) << "Edge file groups:";
    for (auto &vec : eGroups) {
        LOG(INFO) << "{\n";
        for (auto &s : vec) LOG(INFO) << "\t" << s << ",\n";
        LOG(INFO) << "}\n";
    }
}

int main(int argc, char *argv[]) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Parse vertex/edge file group
    vector<string> vGroupStr = Tool::split(FLAGS_vertices, "||"), eGroupStr = Tool::split(FLAGS_edges, "||");
    vector<vector<string>> vGroups, eGroups;
    for (string &s : vGroupStr) vGroups.emplace_back(Tool::split(s, ","));
    for (string &s : eGroupStr) eGroups.emplace_back(Tool::split(s, ","));

    printArgs();
    printFileGroups(vGroups, eGroups);

    Config *config = Config::GetInstance();
    Nodes nodes(FLAGS_worker, ClusterRole::CACHE);

    config->Init(FLAGS_config);

    GraphLoader loader(nodes, FLAGS_output, FLAGS_delimiter[0]);
    loader.setVerbose(true);
    loader.load(vGroups, eGroups);
    return 0;
}
