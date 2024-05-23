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

#include <algorithm>
#include <set>

#include "base/graph_entity.h"
#include "base/item.h"
#include "base/row.h"
#include "base/type.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "server/standalone.h"
#include "src/execution/graph_tool.h"
#include "test/test_tool.h"

namespace AGE {

const char test_cypher_graphdir[] = AGE_TEST_DATA_DIR "/cypher_data";
const int vcnt = 1000;
const int ecnt = 2000;

TEST(test_cypher, query_runnable) {
    TestTool::prepareGraph(test_cypher_graphdir, vcnt, ecnt, 10);
    Standalone server(Config::GetInstance()->graph_name_, test_cypher_graphdir, 4, false);

    EXPECT_EQ(server.RunQuery("create index on :a(int)").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("create index on :a(string)").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("match (n:a {string:1}) return n").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("match (n) return count(n)").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("match (n)-[]->(m) return count(m)").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("match (n)-[:a]->(m), (n)-[:b]->(a) return count(n)").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("match (n)-->({id:1}) return count(n)").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("match (n)-[:a]->(m) with n, count(m) as cm return n, cm").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("match (n:a {string: \"1\"})-[]->(m) where n.int = 1 return count(n)").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("match (n)-[]->(m) with n where n.int > 5 return n").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("match (n)-[]->(m) with n as nn return nn, count(m)").IsSuccess(), false);
    EXPECT_EQ(server.RunQuery("match (n)--(m {id: \"75\"})--() with m, count(n)").IsSuccess(), false);
    EXPECT_EQ(server.RunQuery("match (n)--(m)--() with m.id, count(n) as cn return cn").IsSuccess(), false);
    EXPECT_EQ(server.RunQuery("match (n)-[]->(m) with n as nn where n.int > 5 return nn").IsSuccess(), false);
    EXPECT_EQ(server.RunQuery("match (n)-[]->(m) with n as nn, count(m) as cm where n.int > 5 return nn").IsSuccess(),
              false);
    EXPECT_EQ(server.RunQuery("match (n)-[]->(m) with n as nn where n.int > 5 return nn").IsSuccess(), false);
    EXPECT_EQ(server.RunQuery("match (n)--(m)").IsSuccess(), false);
    EXPECT_EQ(server.RunQuery("match (n {id : \"177\"}) return m").IsSuccess(), false);
    EXPECT_EQ(server.RunQuery("match (n)-[]->(m) where n.int < 5 and n.int < 10 return m").IsSuccess(), true);

    // Barrier projection and aggregations
    EXPECT_EQ(server.RunQuery("match (n)-[]->(m) return n.int order by n.int").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("match (n)-[]->(m) return n.int order by n.int desc").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("match (n)-[]->(m) return n.int order by n.float").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("match (n)-[]->(m) return n.int order by m.float").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("match (n)-[]->(m) with n.int as nint return nint order by m.float").IsSuccess(), false);
    EXPECT_EQ(server.RunQuery("match (n)-[]->(m) return n.int limit 10").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("match (n)-[]->(m) return distinct n.int").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("match (n)-[]->(m) return count(n.randInt), m limit 10").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("match (n)-[]->(m) return count(n.randInt), m order by count(n.randInt)").IsSuccess(),
              true);
    EXPECT_EQ(server.RunQuery("match (n)-[]->(m) return count(distinct n.randInt)").IsSuccess(), true);

    // Subqueries
    EXPECT_EQ(server.RunQuery("match (n)-[]->(m) where n.int < 3 or (m)-[]->(n) return count(n)").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("match (n)-[]->(m) where not (m)-[]->(n) return count(n)").IsSuccess(), true);
    EXPECT_EQ(server.RunQuery("match (k)-[:a]->(n)-[*1..2]->(m) where k.int < m.int return m.int").IsSuccess(), true);
    EXPECT_EQ(
        server.RunQuery("match (n)-[]->(m) optional match (m)-[]->(k) where m.int < 300 return n, m, k").IsSuccess(),
        true);
    EXPECT_EQ(server.RunQuery("optional match (m)-[]->(n) where m.int > 500 return m, n").IsSuccess(), false);
}

TEST(test_cypher, query_correctness) {
    auto generator = TestTool::prepareGraph(test_cypher_graphdir, vcnt, ecnt, 5);
    Standalone server(Config::GetInstance()->graph_name_, test_cypher_graphdir, 4, false);
    Graph* graph = &server.getGraph();
    {
        auto r = server.RunQuery("match (n)-->() return count(n)");
        EXPECT_TRUE(r.IsSuccess());
        EXPECT_EQ(r.data[0][0], Item(T_INTEGER, ecnt));
    }
    {
        auto r = server.RunQuery("match (n)-->(m) with n, m, count(n) as cnt match (m)-->(q) return count(q);");
        EXPECT_TRUE(r.IsSuccess());
        vector<Item> cur = graph->getAllVtx(), nxt;
        for (Item& item : cur) graph->getNeighbor(item, ALL_LABEL, DirectionType_OUT, nxt);
        std::swap(nxt, cur);
        nxt.clear();
        for (Item& item : cur) graph->getNeighbor(item, ALL_LABEL, DirectionType_OUT, nxt);
        EXPECT_EQ(nxt.size(), static_cast<size_t>(r.data[0][0].integerVal));
    }
    {
        // generator.PrintAdj();
        auto r =
            server.RunQuery("match (n)-[:a]->(m:b) where m.bool = true with n, m match (n)-->(p) return n, count(p)");
        EXPECT_EQ(r.IsSuccess(), true);

        LabelId labelA = graph->getLabelId("a"), labelB = graph->getLabelId("b");
        PropId propBool = graph->getPropId("bool");

        map<Vertex, vector<Item>> answer;
        for (const Item& item : graph->getAllVtx()) answer[item.vertex].push_back(item);
        // (n)-[:a]->(m:b) where n.bool = true
        for (auto& [_, vec] : answer) {
            // (n)-[:a]->(m:b)
            vector<Item> nxt;
            for (const Item& item : vec) graph->getNeighbor(item, labelA, DirectionType_OUT, nxt, labelB);
            // where m.bool = true
            nxt.erase(std::remove_if(nxt.begin(), nxt.end(),
                                     [graph, propBool](const Item& item) {
                                         return graph->getProp(item, propBool).boolVal != Item(T_BOOL, true);
                                     }),
                      nxt.end());
            vec.swap(nxt);
        }
        // (n)-->(p)
        for (auto& [_, vec] : answer) {
            vector<Item> nxt;
            for (size_t i = 0; i < vec.size(); i++)
                graph->getNeighbor(Item(T_VERTEX, _), ALL_LABEL, DirectionType_OUT, nxt);
            vec.swap(nxt);
        }

        // answer count
        size_t answerCnt = 0;
        for (auto& [_, vec] : answer) answerCnt += vec.size() != 0;

        // Check answer
        EXPECT_EQ(r.data.size(), answerCnt);
        for (Row& row : r.data) {
            const Item &varN = row[0], &varCnt = row[1];
            i64 answerCnt = answer.at(varN.vertex).size();
            // LOG(INFO) << "result: n: " << varN.DebugString() << ", cnt: " << varCnt.DebugString();
            // LOG(INFO) << "answer: n: " << varN.DebugString() << ", cnt: " << answerCnt;
            EXPECT_EQ(answerCnt, varCnt.integerVal);
        }
    }
}

TEST(test_cypher, barrier_project_correctness) {
    auto generator = TestTool::prepareGraph(test_cypher_graphdir, vcnt, ecnt, 5);
    Standalone server(Config::GetInstance()->graph_name_, test_cypher_graphdir, 4, false);
    {
        auto r = server.RunQuery("match (n)-[]->(m) return n.int limit 10");
        EXPECT_TRUE(r.IsSuccess());
        EXPECT_EQ(r.data.size(), 10ull);
    }
    {
        auto r = server.RunQuery("match (n)-[]->(m) return n.int, m.float order by m.float asc");
        EXPECT_TRUE(r.IsSuccess());
        for (u32 i = 0; i < r.data.size(); i++) {
            // LOG(INFO) << r.data[i].DebugString() << endl;
            if (i) {
                // LOG(INFO) << r.data[i-1][1].DebugString() << ", " << r.data[i][1].DebugString();
                EXPECT_LE(r.data[i - 1][1], r.data[i][1]);
            }
        }
    }
    {
        auto r = server.RunQuery("match (n)-[]->(m) return distinct n.randInt");
        EXPECT_TRUE(r.IsSuccess());
        std::set<Item> checkSet;
        for (const Row& r : r.data) {
            // LOG(INFO) << r[0].DebugString() << endl;
            EXPECT_FALSE(checkSet.count(r[0]));
            checkSet.insert(r[0]);
        }
    }
    {
        auto r = server.RunQuery("match (n)-[]->(m) return avg(n.randInt), m.float limit 3");
        EXPECT_TRUE(r.IsSuccess());
        EXPECT_EQ(r.data.size(), 3ull);
    }
    {
        auto r = server.RunQuery("match (n)-[]->(m) return count(n.randInt) order by count(n.randInt) desc");
        EXPECT_TRUE(r.IsSuccess());
        for (u32 i = 0; i < r.data.size(); i++) {
            if (i) {
                EXPECT_GE(r.data[i - 1][0], r.data[i][0]);
            }
        }
    }
    {
        auto r1 = server.RunQuery("match (n)-[]->(m) return sum(distinct n.randInt)");
        auto r2 = server.RunQuery("match (n)-[]->(m) return sum(n.randInt)");
        EXPECT_TRUE(r1.IsSuccess() && r2.IsSuccess());
        EXPECT_LE(r1.data.size(), r2.data.size());
    }
}

TEST(test_cypher, branch_filter_correctness) {
    auto generator = TestTool::prepareGraph(test_cypher_graphdir, vcnt, ecnt, 5);
    Standalone server(Config::GetInstance()->graph_name_, test_cypher_graphdir, 4, false);
    Graph* graph = &server.getGraph();
    {
        auto r = server.RunQuery("match (n)-[]->(m) where n.int < 3 or (m)-[]->(n) return n, m, n.int order by n.int");
        EXPECT_TRUE(r.IsSuccess());

        map<Item, vector<Item>> answer;
        vector<pair<Item, Item>> answerVec;
        PropId intId = graph->getPropId("int");
        // Vertex scan n
        for (const Item& item : graph->getAllVtx()) {
            answer[item].push_back(item);
            CHECK_EQ(answer[item].size(), (size_t)1);
        }
        // (n)-[]->(m)
        for (auto& [src, vec] : answer) {
            vector<Item> nxt;
            graph->getNeighbor(src, ALL_LABEL, DirectionType_OUT, nxt, ALL_LABEL);
            vec.swap(nxt);
        }
        // where n.int < 3 or (m)-[]->(n)
        for (auto& [_, vec] : answer) {
            const Item& src = _;
            vec.erase(std::remove_if(vec.begin(), vec.end(),
                                     [graph, src, intId](const Item& dst) {
                                         vector<Item> tmp;
                                         if (graph->getProp(src, intId) < Item(T_INTEGER, 3)) return false;
                                         graph->getNeighbor(dst, ALL_LABEL, DirectionType_OUT, tmp, ALL_LABEL);
                                         for (const Item& nxt : tmp)
                                             if (nxt == src) return false;
                                         return true;
                                     }),
                      vec.end());
            for (Item& dst : vec) answerVec.emplace_back(std::make_pair(src, dst));
        }
        // ORDER BY n.int
        std::sort(answerVec.begin(), answerVec.end(),
                  [graph, intId](const pair<Item, Item>& a, const pair<Item, Item>& b) {
                      return graph->getProp(a.first, intId) < graph->getProp(b.first, intId);
                  });

        u32 answerCnt = answerVec.size();
        EXPECT_EQ(answerCnt, r.data.size());
        for (u32 i = 0; i < answerCnt; i++) {
            EXPECT_EQ(answerVec[i].first, r.data[i][0]);
        }
    }
    {
        auto r = server.RunQuery("match (n)-[]->(m) where not (m)-[]->(n) return n, m order by m.float");
        EXPECT_TRUE(r.IsSuccess());

        map<Item, vector<Item>> answer;
        vector<pair<Item, Item>> answerVec;
        PropId floatId = graph->getPropId("float");
        for (const Item& item : graph->getAllVtx()) {
            answer[item].push_back(item);
            CHECK_EQ(answer[item].size(), (size_t)1);
        }
        for (auto& [src, vec] : answer) {
            vector<Item> nxt;
            graph->getNeighbor(src, ALL_LABEL, DirectionType_OUT, nxt, ALL_LABEL);
            vec.swap(nxt);
        }
        // where not (m)-[]->(n)
        for (auto& [_, vec] : answer) {
            const Item& src = _;
            vec.erase(std::remove_if(vec.begin(), vec.end(),
                                     [graph, src](const Item& dst) {
                                         vector<Item> tmp;
                                         graph->getNeighbor(dst, ALL_LABEL, DirectionType_OUT, tmp, ALL_LABEL);
                                         for (const Item& nxt : tmp)
                                             if (nxt == src) return true;
                                         return false;
                                     }),
                      vec.end());
            for (Item& dst : vec) answerVec.emplace_back(std::make_pair(src, dst));
        }
        // ORDER BY m.float
        std::sort(answerVec.begin(), answerVec.end(),
                  [graph, floatId](const pair<Item, Item>& a, const pair<Item, Item>& b) {
                      return graph->getProp(a.second, floatId) < graph->getProp(b.second, floatId);
                  });

        u32 answerCnt = answerVec.size();
        EXPECT_EQ(answerCnt, r.data.size());
        for (u32 i = 0; i < answerCnt; i++) {
            EXPECT_EQ(answerVec[i].second, r.data[i][1]);
        }
    }
    {
        auto r = server.RunQuery("match (n)-[]->(m) where n.int < 3 or not (m)-[]->(n) return n, m order by n.int");
        EXPECT_TRUE(r.IsSuccess());

        map<Item, vector<Item>> answer;
        vector<pair<Item, Item>> answerVec;
        PropId intId = graph->getPropId("int");
        for (const Item& item : graph->getAllVtx()) {
            answer[item].push_back(item);
            CHECK_EQ(answer[item].size(), (size_t)1);
        }
        for (auto& [src, vec] : answer) {
            vector<Item> nxt;
            graph->getNeighbor(src, ALL_LABEL, DirectionType_OUT, nxt, ALL_LABEL);
            vec.swap(nxt);
        }
        // where NOT (m)-[]->(n) or n.int < 3
        for (auto& [_, vec] : answer) {
            const Item& src = _;
            vec.erase(std::remove_if(vec.begin(), vec.end(),
                                     [graph, src, intId](const Item& dst) {
                                         if (graph->getProp(src, intId) < Item(T_INTEGER, 3)) return false;
                                         vector<Item> tmp;
                                         graph->getNeighbor(dst, ALL_LABEL, DirectionType_OUT, tmp, ALL_LABEL);
                                         for (const Item& nxt : tmp)
                                             if (nxt == src) return true;
                                         return false;
                                     }),
                      vec.end());
            for (Item& dst : vec) answerVec.emplace_back(std::make_pair(src, dst));
        }
        // ORDER BY n.int
        std::sort(answerVec.begin(), answerVec.end(),
                  [graph, intId](const pair<Item, Item>& a, const pair<Item, Item>& b) {
                      return graph->getProp(a.first, intId) < graph->getProp(b.first, intId);
                  });

        u32 answerCnt = answerVec.size();
        EXPECT_EQ(answerCnt, r.data.size());
        for (u32 i = 0; i < answerCnt; i++) {
            EXPECT_EQ(answerVec[i].first, r.data[i][0]);
        }
    }
}

TEST(test_cypher, loop_correctness) {
    auto generator = TestTool::prepareGraph(test_cypher_graphdir, vcnt, ecnt, 5);
    Standalone server(Config::GetInstance()->graph_name_, test_cypher_graphdir, 4, false);
    Graph* graph = &server.getGraph();
    {
        auto r = server.RunQuery("match (n)-[*1..2]->(m) where n.int < m.int return n, m.int order by m.int");
        EXPECT_TRUE(r.IsSuccess());

        map<Item, vector<Item>> answer;
        vector<pair<Item, Item>> answerVec;
        PropId intId = graph->getPropId("int");
        for (const Item& item : graph->getAllVtx()) {
            answer[item].push_back(item);
            CHECK_EQ(answer[item].size(), (size_t)1);
        }
        // 1-hop (n)-[]->(m) and 2-hop (m)-[]->(m)
        for (u32 loopNum = 1; loopNum <= 2; loopNum++) {
            for (auto& [_, vec] : answer) {
                vector<Item> nxt;
                for (const Item& n : vec) {
                    vector<Item> tmp;
                    graph->getNeighbor(n, ALL_LABEL, DirectionType_OUT, tmp, ALL_LABEL);
                    Tool::VecMoveAppend(tmp, nxt);
                }
                vec.swap(nxt);
            }
            // where n.int < m.int
            for (auto& [_, vec] : answer) {
                const Item& n = _;
                vector<Item> candidates = vec;
                candidates.erase(std::remove_if(candidates.begin(), candidates.end(),
                                                [graph, n, intId](const Item& m) {
                                                    return graph->getProp(n, intId) >= graph->getProp(m, intId);
                                                }),
                                 candidates.end());
                for (Item& m : candidates) answerVec.emplace_back(std::make_pair(n, graph->getProp(m, intId)));
            }
        }
        // ORDER BY m.int
        std::sort(answerVec.begin(), answerVec.end(),
                  [](const pair<Item, Item>& a, const pair<Item, Item>& b) { return a.second < b.second; });

        u32 answerCnt = answerVec.size();
        EXPECT_EQ(answerCnt, r.data.size());
        for (u32 i = 0; i < answerCnt; i++) {
            const Row& row = r.data[i];
            EXPECT_EQ(row[1], answerVec[i].second);
            // LOG(INFO) << row.DebugString();
        }
    }
    {
        auto r = server.RunQuery("match (n)-[*2..2]->(m) where n.int < m.int return n, m order by n.int");
        EXPECT_TRUE(r.IsSuccess());

        map<Item, vector<Item>> answer;
        vector<pair<Item, Item>> answerVec;
        PropId intId = graph->getPropId("int");
        for (const Item& item : graph->getAllVtx()) {
            answer[item].push_back(item);
            CHECK_EQ(answer[item].size(), (size_t)1);
        }
        for (u32 loopNum = 1; loopNum <= 2; loopNum++) {
            for (auto& [_, vec] : answer) {
                vector<Item> nxt;
                for (const Item& n : vec) {
                    vector<Item> tmp;
                    graph->getNeighbor(n, ALL_LABEL, DirectionType_OUT, tmp, ALL_LABEL);
                    Tool::VecMoveAppend(tmp, nxt);
                }
                vec.swap(nxt);
            }
        }
        // where n.int < m.int
        for (auto& [_, vec] : answer) {
            const Item& n = _;
            vector<Item> candidates = vec;
            candidates.erase(std::remove_if(candidates.begin(), candidates.end(),
                                            [graph, n, intId](const Item& m) {
                                                return graph->getProp(n, intId) >= graph->getProp(m, intId);
                                            }),
                             candidates.end());
            for (Item& m : candidates) answerVec.emplace_back(std::make_pair(n, m));
        }
        // ORDER BY n.int
        std::sort(answerVec.begin(), answerVec.end(),
                  [graph, intId](const pair<Item, Item>& a, const pair<Item, Item>& b) {
                      return graph->getProp(a.first, intId) < graph->getProp(b.first, intId);
                  });

        u32 answerCnt = answerVec.size();
        EXPECT_EQ(answerCnt, r.data.size());
        for (u32 i = 0; i < answerCnt; i++) {
            const Row& row = r.data[i];
            EXPECT_EQ(row[0], answerVec[i].first);
            // LOG(INFO) << row.DebugString();
        }
    }
}

TEST(test_cypher, optional_match_correctness) {
    auto generator = TestTool::prepareGraph(test_cypher_graphdir, vcnt, ecnt, 5);
    Standalone server(Config::GetInstance()->graph_name_, test_cypher_graphdir, 4, false);
    Graph* graph = &server.getGraph();
    {
        auto r = server.RunQuery(
            "match (n)-[]->(m) optional match (m)-[]->(k) where not (k)-[]->(n) return n, k.int order by k.int");
        EXPECT_TRUE(r.IsSuccess());

        map<Item, vector<Row>> answer;
        vector<Row> answerVec;
        PropId intId = graph->getPropId("int");
        for (const Item& item : graph->getAllVtx()) {
            answer[item].push_back(Row());
            answer[item].back().emplace_back(item);
            CHECK_EQ(answer[item].size(), (size_t)1);
        }
        // 1-hop (n)-[]->(m)
        for (auto& [_, vec] : answer) {
            vector<Item> nxt;
            Item n = vec[0][0];
            graph->getNeighbor(n, ALL_LABEL, DirectionType_OUT, nxt, ALL_LABEL);
            vec.clear();
            for (Item& m : nxt) {
                vec.emplace_back(Row());
                vec.back().emplace_back(n);
                vec.back().emplace_back(m);
            }
        }
        // optional (m)-[]->(k) where k.int < 300
        for (auto& [_, vec] : answer) {
            for (const Row& row : vec) {
                vector<Item> tmp;
                Item n = row[0], m = row[1];
                graph->getNeighbor(m, ALL_LABEL, DirectionType_OUT, tmp, ALL_LABEL);
                tmp.erase(std::remove_if(tmp.begin(), tmp.end(),
                                         [graph, n](const Item k) {
                                             vector<Item> tmp;
                                             graph->getNeighbor(k, ALL_LABEL, DirectionType_OUT, tmp, ALL_LABEL);
                                             return std::find(tmp.begin(), tmp.end(), n) != tmp.end();
                                         }),
                          tmp.end());
                if (!tmp.size()) {
                    answerVec.emplace_back(row);
                    answerVec.back().emplace_back(Item(T_NULL));
                } else {
                    for (const Item& k : tmp) {
                        answerVec.emplace_back(row);
                        answerVec.back().emplace_back(graph->getProp(k, intId));
                    }
                }
            }
        }
        // ORDER BY k.int
        std::sort(answerVec.begin(), answerVec.end(),
                  [](const Row& a, const Row& b) { return Item::Compare(a[2], b[2]) < 0 ? true : false; });

        u32 answerCnt = answerVec.size();
        EXPECT_EQ(answerCnt, r.data.size());
        for (u32 i = 0; i < answerCnt; i++) {
            EXPECT_EQ(Item::Compare(r.data[i][1], answerVec[i][2]), 0);
        }
    }
}
}  // namespace AGE
