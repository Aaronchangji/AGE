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
// limitations under the License

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include "base/node.h"
#include "server/client.h"
#include "server/standalone.h"
#include "storage/id_mapper.h"
#include "storage/unique_namer.h"
#include "test/test_tool.h"
#include "util/config.h"

using std::make_pair;
using std::pair;
using std::string;
using std::vector;

namespace AGE {
const char input_graphdir[] = AGE_ROOT "/example_graph/raw_data/";
const char output_graphdir[] = AGE_ROOT "/example_graph/output/single/";
const int client_port = 11010;

/**
 * We assume there is already a cluster ready for query processing for this test
 */

void load_example_graph() {
    GraphLoader loader(Nodes::CreateStandaloneNodes(), output_graphdir);
    vector<vector<string>> vFiles;
    vector<vector<string>> eFiles;
    vFiles.emplace_back(vector<string>{input_graphdir + string("vertex.csv")});
    eFiles.emplace_back(vector<string>{input_graphdir + string("edge.csv")});
    loader.load(vFiles, eFiles);
}

bool client_run_query(Client& client, string& query) {
    bool opt2file = false;
    if (!client.run_single_query(query, opt2file)) {
        return false;
    }
    return true;
}

void test_query() {
    load_example_graph();
    Config* config = Config::GetInstance();
    AGE::Standalone standalone(config->graph_name_, output_graphdir, 4, false);

    Node master_node(config->master_ip_address_, config->master_listen_port_);
    AGE::Client client(client_port, master_node);

    // get result
    string query = "match (n:person)-[:knows]->(m:person) return n, m, n.firstName, m.firstName;";
    bool opt2file = false;
    CHECK(client.run_single_query(query, opt2file));
    Result* res = client.get_result();
    std::sort(res->data.begin(), res->data.end(), [](const Row& l, const Row& r) {
        if (l[0].vertex.id != r[0].vertex.id) return l[0].vertex.id < r[0].vertex.id;
        return l[1].vertex.id < r[1].vertex.id;
    });

    // get answer
    Result ans = standalone.RunQuery(query);
    std::sort(ans.data.begin(), ans.data.end(), [](const Row& l, const Row& r) {
        if (l[0].vertex.id != r[0].vertex.id) return l[0].vertex.id < r[0].vertex.id;
        return l[1].vertex.id < r[1].vertex.id;
    });

    // check correctness
    EXPECT_EQ(res->data.size(), ans.data.size());
    for (size_t i = 0; i < ans.data.size(); i++) {
        for (size_t j = 0; j < ans.data.at(i).size(); j++) {
            EXPECT_EQ(res->data.at(i).at(j), ans.data.at(i).at(j));
        }
    }

    /*
    // Github issue #123
    // Get result.
    res = standalone.RunQuery("match (n:person)-[:knows]->(m:person) with m,n return m.gender, count(m)");
    sort(res.data.begin(), res.data.end(), [](const Row& l, const Row& r) { return l[0].boolVal < r[0].boolVal; });

    // Check answer.
    EXPECT_EQ(res.data.size(), 2ull);
    EXPECT_EQ(res.data[0][0], Item(T_BOOL, false));
    EXPECT_EQ(res.data[0][1], Item(T_INTEGER, 1));
    EXPECT_EQ(res.data[1][0], Item(T_BOOL, true));
    EXPECT_EQ(res.data[1][1], Item(T_INTEGER, 3));

    // Github issue #128
    // Get result.
    res = standalone.RunQuery("match (n:person)-[:knows]->(m:person) return m.gender, count(m.gender)");
    sort(res.data.begin(), res.data.end(), [](const Row& l, const Row& r) { return l[0].boolVal < r[0].boolVal; });

    // Check answer.
    EXPECT_EQ(res.data.size(), 2ull);
    EXPECT_EQ(res.data[0][0], Item(T_BOOL, false));
    EXPECT_EQ(res.data[0][1], Item(T_INTEGER, 1));
    EXPECT_EQ(res.data[1][0], Item(T_BOOL, true));
    EXPECT_EQ(res.data[1][1], Item(T_INTEGER, 3));
    */
}

TEST(test_query, test_query) { test_query(); }
}  // namespace AGE

using AGE::Config;
using AGE::Nodes;

#define DISTRIBUTED_CONFIG_DIR AGE_ROOT "/scripts/test/config"

GTEST_API_ int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    ::testing::InitGoogleTest(&argc, argv);
    GTEST_FLAG_SET(death_test_style, "threadsafe");

    Config* config = Config::GetInstance();
    config->Init(DISTRIBUTED_CONFIG_DIR "/conf.ini");
    return RUN_ALL_TESTS();
}
