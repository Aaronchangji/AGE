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
// limitations under the License

#include <gtest/gtest.h>
#include <fstream>
#include <string>
#include <utility>
#include <vector>
#include "base/expression.h"
#include "base/node.h"
#include "base/type.h"
#include "storage/graph.h"
#include "storage/graph_loader.h"
#include "storage/id_mapper.h"
#include "storage/layout.h"
#include "storage/unique_namer.h"
#include "test/test_tool.h"
#include "util/config.h"

using std::string;
using std::vector;

namespace AGE {
const char storage_graphdir[] = AGE_TEST_DATA_DIR "/test_storage_data";

string loadExample() {
    TestTool::ExampleGraph g(storage_graphdir);
    g.generate();
    GraphLoader loader(Nodes::CreateStandaloneNodes(), storage_graphdir);
    loader.load({{g.vSchemaFile, g.vf}}, {{g.eSchemaFile, g.ef}});
    return storage_graphdir;
}

void test_storage_example() {
    loadExample();
    Graph g(TestTool::getTestGraphDir(storage_graphdir));
    g.load();
    vector<Item> vtx;
    g.getAllVtx(vtx);
    PropId name = g.getPropId("name"), gender = g.getPropId("gender"), age = g.getPropId("age"),
           grade = g.getPropId("grade"), id = g.getPropId("id");
    LabelId knows = g.getLabelId("knows"), follow = g.getLabelId("follow");

    unordered_map<int64_t, Item> idMap;
    for (Item& v : vtx) {
        Item id_ = g.getProp(v, id);
        idMap[id_.integerVal] = v;
        // cout << "idMap[" << id_.integerVal << "]: " << idMap[id_.integerVal].DebugString() << endl;
    }

    // Check getProp.
    EXPECT_EQ(Item(T_STRING, "John"), g.getProp(idMap[1], name));
    EXPECT_EQ(Item(T_BOOL, true), g.getProp(idMap[2], gender));
    EXPECT_EQ(Item(T_INTEGER, 1), g.getProp(idMap[3], age));
    EXPECT_EQ(Item(T_FLOAT, 0.9), g.getProp(idMap[4], grade));

    // Check adj list.
    vector<Item> nb;
    g.getNeighbor(idMap[1], knows, DirectionType_OUT, nb);
    EXPECT_EQ(nb.size(), 2ull);
    EXPECT_EQ(nb[0], idMap[2]);
    EXPECT_EQ(nb[1], idMap[3]);

    nb.clear();
    g.getNeighbor(idMap[5], knows, DirectionType_OUT, nb);
    EXPECT_EQ(nb.size(), 0ull);

    nb.clear();
    g.getNeighbor(idMap[1], knows, DirectionType_IN, nb);
    EXPECT_EQ(nb.size(), 1ull);
    EXPECT_EQ(nb[0], idMap[2]);

    nb.clear();
    g.getNeighbor(idMap[2], follow, DirectionType_BOTH, nb);
    EXPECT_EQ(nb.size(), 2ull);
    EXPECT_EQ(nb[0], idMap[1]);
    EXPECT_EQ(nb[1], idMap[3]);

    nb.clear();
    g.getNeighbor(idMap[1], 0, DirectionType_BOTH, nb);
    sort(nb.begin(), nb.end(), [](const Item& lhs, const Item& rhs) { return lhs.vertex.id < rhs.vertex.id; });
    EXPECT_EQ(nb.size(), 5ull);
    EXPECT_EQ(nb[0], idMap[2]);
    EXPECT_EQ(nb[1], idMap[2]);
    EXPECT_EQ(nb[2], idMap[2]);
    EXPECT_EQ(nb[3], idMap[3]);
    EXPECT_EQ(nb[4], idMap[4]);
}

void checkAdj(Graph& g, int lcnt, vector<vector<int>>& adj, DirectionType dir) {
    // printAdj(adj);
    int vcnt = adj.size();
    // check adj list.
    for (int u = 1; u <= vcnt; u++) {
        // Test labeled getNeighbor.
        for (LabelId label = 1; label <= lcnt; label++) {
            vector<Item> answer;
            for (int x : adj[u - 1]) {
                int v = x + 1;
                LabelId el = (u + v - 1) % lcnt + 1;
                if (el == label) {
                    answer.emplace_back(Item(T_VERTEX, v, (v - 1) % lcnt + 1));
                }
            }
            vector<Item> res = g.getNeighbor(Item(T_VERTEX, u, (u - 1) % lcnt + 1), label, dir);
            sort(res.begin(), res.end());
            sort(answer.begin(), answer.end());
            EXPECT_EQ(answer.size(), res.size());
            for (size_t i = 0; i < answer.size(); i++) {
                EXPECT_EQ(answer[i], res[i]);
            }
        }

        // Test all label getNeighbor.
        vector<Item> res = g.getNeighbor(Item(T_VERTEX, u, (u - 1) % lcnt + 1), ALL_LABEL, dir);
        sort(adj[u - 1].begin(), adj[u - 1].end());
        sort(res.begin(), res.end(), [](const Item& lhs, const Item& rhs) { return lhs.vertex.id < rhs.vertex.id; });
        EXPECT_EQ(adj[u - 1].size(), adj[u - 1].size());
        for (size_t i = 0; i < adj[u - 1].size(); i++) {
            EXPECT_EQ(static_cast<uint64_t>(adj[u - 1][i]), res[i].vertex.id - 1);
        }
    }
}

void test_cursor(int vcnt, int ecnt, int lcnt) {
    TestTool::MultiLabelGraphGenerator gen(storage_graphdir, vcnt, ecnt, lcnt);
    auto loadPath = gen.loadPath();
    {
        GraphLoader loader(Nodes::CreateStandaloneNodes(), storage_graphdir);
        loader.load(loadPath.first, loadPath.second);
    }

    Graph g(TestTool::getTestGraphDir(storage_graphdir));
    g.load();
    for (int i = 1; i <= lcnt; i++) {
        vector<int> v;
        for (VPropStore::VtxCursor cursor = g.getLabelVtxCursor(i); !cursor.end(); ++cursor) {
            // printf("%s,", (*cursor).DebugString().c_str());
            v.emplace_back((*cursor).id);
        }
        // printf("\n");
        sort(v.begin(), v.end());
        EXPECT_EQ(v.size(), static_cast<size_t>((vcnt + lcnt - i) / lcnt));
        for (int j = i; j <= vcnt; j += lcnt) {
            EXPECT_EQ(v[(j - 1) / lcnt], j);
        }
    }
}

void test_partitionGraph(int vcnt, int ecnt, int lcnt, int worldSize) {
    assert(worldSize > 0);
    string configPath = string(AGE_ROOT) + "/config/conf.ini";
    string nodesPath = string(storage_graphdir) + "/nodes.txt";
    TestTool::NodesGenerator nodesGen(nodesPath, worldSize);
    TestTool::MultiLabelGraphGenerator graphGen(storage_graphdir, vcnt, ecnt, lcnt);

    Nodes nodes(nodesPath, ClusterRole::CACHE);
    Config* config = Config::GetInstance();
    config->Init(configPath);

    IdMapper idMapper(nodes);
    UniqueNamer uniqueNamer(storage_graphdir);

    auto loadPath = graphGen.loadPath();
    vector<string> partitionDir;

    {
        GraphLoader loader(nodes, storage_graphdir);
        loader.load(loadPath.first, loadPath.second);
        for (int i = 0; i < worldSize; i++) {
            partitionDir.emplace_back(loader.getPartitionDir(i));
        }
    }

    vector<Graph*> g;
    for (int i = 0; i < worldSize; i++) {
        g.emplace_back(new Graph(partitionDir[i]));
        if (!g.back()->load()) printf("graph partition %d load fail!\n", i);
    }

    PropId pint = g[0]->getPropId("int"), pfl = g[0]->getPropId("float"), pstr = g[0]->getPropId("string"),
           pbool = g[0]->getPropId("bool");
    vector<Item> vtx, edge;
    for (int i = 0; i < worldSize; i++) {
        g[i]->getAllVtx(vtx);
    }

    sort(vtx.begin(), vtx.end(), [](const Item& lhs, const Item& rhs) { return lhs.vertex.id < rhs.vertex.id; });
    vector<vector<int>> outAdj = graphGen.adjList;

    // Check vertex and adj.
    EXPECT_EQ(vtx.size(), static_cast<u32>(vcnt));
    for (size_t i = 0; i < vtx.size(); i++) {
        int64_t vid = i + 1;

        Item curV(T_VERTEX, vid, (vid - 1) % lcnt + 1);
        EXPECT_EQ(vtx[i], curV);
        int rk = idMapper.GetRank(vtx[i]);
        // printf("vertex: %s, rank: %d\n", curV.DebugString().c_str(), rk);
        EXPECT_EQ(g[rk]->getProp(vtx[i], pint), Item(T_INTEGER, vid));
        EXPECT_EQ(g[rk]->getProp(vtx[i], pfl), Item(T_FLOAT, vid * 1.0));
        EXPECT_EQ(g[rk]->getProp(vtx[i], pstr), Item(T_STRING, std::to_string(vid)));
        EXPECT_EQ(g[rk]->getProp(vtx[i], pbool), Item(T_BOOL, static_cast<bool>(vid % 2)));

        vector<Item> res = g[rk]->getNeighbor(curV, ALL_LABEL, DirectionType_OUT);
        sort(outAdj[i].begin(), outAdj[i].end());
        sort(res.begin(), res.end(), [](const Item& lhs, const Item& rhs) { return lhs.vertex.id < rhs.vertex.id; });
        EXPECT_EQ(res.size(), outAdj[i].size());
        for (size_t j = 0; j < outAdj[i].size(); j++) {
            EXPECT_EQ(res[j].vertex.id, static_cast<u32>(outAdj[i][j] + 1));
        }
    }

    for (int i = 0; i < worldSize; i++) delete g[i];
}

void test_generatedGraph(int vcnt, int ecnt, int lcnt) {
    TestTool::MultiLabelGraphGenerator gen(storage_graphdir, vcnt, ecnt, lcnt);
    auto loadPath = gen.loadPath();
    {
        GraphLoader loader(Nodes::CreateStandaloneNodes(), storage_graphdir);
        loader.load(loadPath.first, loadPath.second);
    }

    Graph g(TestTool::getTestGraphDir(storage_graphdir));
    g.load();

    PropId pint = g.getPropId("int"), pfl = g.getPropId("float"), pstr = g.getPropId("string"),
           pbool = g.getPropId("bool");
    vector<Item> vtx = g.getAllVtx(), edge;
    sort(vtx.begin(), vtx.end(), [](const Item& lhs, const Item& rhs) { return lhs.vertex.id < rhs.vertex.id; });

    // Check vertex.
    for (size_t i = 0; i < vtx.size(); i++) {
        int64_t vid = i + 1;
        EXPECT_EQ(vtx[i], Item(T_VERTEX, vid, (vid - 1) % lcnt + 1));
        EXPECT_EQ(g.getProp(vtx[i], pint), Item(T_INTEGER, vid));
        EXPECT_EQ(g.getProp(vtx[i], pfl), Item(T_FLOAT, vid * 1.0));
        EXPECT_EQ(g.getProp(vtx[i], pstr), Item(T_STRING, std::to_string(vid)));
        EXPECT_EQ(g.getProp(vtx[i], pbool), Item(T_BOOL, static_cast<bool>(vid % 2)));
    }

    // Check Adj list.
    vector<vector<int>> outAdj = gen.adjList, inAdj(vcnt), bothAdj(vcnt);
    for (int i = 0; i < vcnt; i++)
        for (int j : outAdj[i]) {
            inAdj[j].emplace_back(i);
            bothAdj[i].emplace_back(j);
            bothAdj[j].emplace_back(i);
        }

    checkAdj(g, lcnt, outAdj, DirectionType_OUT);
    checkAdj(g, lcnt, inAdj, DirectionType_IN);
    checkAdj(g, lcnt, bothAdj, DirectionType_BOTH);
}

TEST(test_storage, example) { test_storage_example(); }

TEST(test_storage, testGeneratedGraph) {
    test_generatedGraph(5, 10, 5);
    test_generatedGraph(500, 1000, 8);
    test_cursor(20, 10, 7);
}

TEST(test_storage, testPartitionGraph) {
    test_partitionGraph(5, 10, 5, 10);
    test_partitionGraph(15, 10, 5, 3);
    test_partitionGraph(500, 1000, 5, 10);
}
}  // namespace AGE
