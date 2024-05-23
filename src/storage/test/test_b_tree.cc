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
#include <map>
#include <string>
#include <utility>
#include <vector>
#include "base/expression.h"
#include "base/type.h"
#include "gtest/gtest.h"
#include "storage/b_tree.h"
#include "test/test_tool.h"

namespace AGE {
using std::multimap;
using std::pair;
using std::sort;
using std::string;
using std::vector;

void checkBTreeResult(BTree *bt, multimap<Item, GEntity> *mulMap, Item key, u64 *btTime = nullptr,
                      u64 *mapTime = nullptr) {
    // printf("checkBTreeResult(%s)\n", key.DebugString().c_str());
    vector<GEntity> result, answer;

    // warm cache to compare perf.
    /*
    for (auto [beg, end] = bt->equal_range(key); beg != end; ++beg) result.push_back(beg->second);
    for (auto [beg, end] = bt->equal_range(key); beg != end; ++beg) answer.push_back(beg->second);
    result.clear();
    answer.clear();
    */

    u64 btBeg = TestTool::getTimeNs();
    // for (auto [beg, end] = bt->equal_range(key); beg != end; ++beg) result.push_back((*beg).second);
    for (auto [beg, end] = bt->equal_range(key); beg != end; ++beg) result.push_back(beg->second);
    if (btTime != nullptr) *btTime += TestTool::getTimeNs() - btBeg;

    u64 mapBeg = TestTool::getTimeNs();
    for (auto [beg, end] = mulMap->equal_range(key); beg != end; ++beg) answer.push_back(beg->second);
    if (mapTime != nullptr) *mapTime += TestTool::getTimeNs() - mapBeg;

    sort(result.begin(), result.end());
    sort(answer.begin(), answer.end());

    EXPECT_EQ(answer.size(), result.size());
    for (u32 i = 0; i < answer.size(); i++) {
        EXPECT_EQ(result[i], answer[i]);
    }
}

void testBTree(ItemType type, int n, int m) {
    ASSERT_TRUE(type == T_INTEGER || type == T_STRING);
    BTree bt(type);
    multimap<Item, GEntity> mulMap;
    pair<Item, GEntity> *insertData = new pair<Item, GEntity>[n];
    vector<u32> queryIndex;
    vector<Item> queryKey;
    u64 btITime = 0, mapITime = 0, btQTime = 0, mapQTime = 0, seedGenTime = 0;

    // Generate random seed for insert.
    seedGenTime = TestTool::getTimeNs();
    for (int i = 0; i < n; i++) {
        GEntity val(TestTool::Rand() % 10, 1);
        if (type == T_INTEGER) {
            int key = TestTool::Rand();
            insertData[i] = make_pair(Item(type, key), val);
        } else if (type == T_STRING) {
            string key = TestTool::RandStr(16);
            insertData[i] = make_pair(Item(type, key), val);
        }
    }

    // Generate random seed for query.
    for (int i = 0; i < m; i++) {
        int randIndex = TestTool::Rand() % n;
        queryIndex.emplace_back(randIndex);

        if (type == T_INTEGER) {
            int randKey = TestTool::Rand();
            queryKey.emplace_back(Item(T_INTEGER, randKey));
        } else if (type == T_STRING) {
            string randKey = TestTool::RandStr();
            queryKey.emplace_back(Item(T_STRING, randKey));
        }
    }
    seedGenTime = TestTool::getTimeNs() - seedGenTime;

    // Insert b-tree.
    btITime = TestTool::getTimeNs();
    for (int i = 0; i < n; i++) bt.emplace(insertData[i].first, insertData[i].second);
    btITime = TestTool::getTimeNs() - btITime;
    // printf("%s", bt.DebugString().c_str());

    // Insert multi-map.
    mapITime = TestTool::getTimeNs();
    for (int i = 0; i < n; i++) mulMap.emplace(insertData[i].first, insertData[i].second);
    mapITime = TestTool::getTimeNs() - mapITime;

    // Check with exist-key.
    for (int i = 0; i < m; i++) checkBTreeResult(&bt, &mulMap, insertData[queryIndex[i]].first, &btQTime, &mapQTime);

    // Check with random-key;
    for (int i = 0; i < m; i++) checkBTreeResult(&bt, &mulMap, queryKey[i], &btQTime, &mapQTime);

    /*
    printf("Seed generate time cost: %.6f ms\n", seedGenTime / 1e6);
    printf("AGE::BTree\tinsert time cost: %.6f ms\nstd::MultiMap\tinsert time cost: %.6f ms\n", btITime / 1e6,
           mapITime / 1e6);
    printf("AGE::BTree\tquery  time cost: %.6f ms\nstd::MultiMap\tquery  time cost: %.6f ms\n", btQTime / 1e6,
           mapQTime / 1e6);
    */

    delete[] insertData;
}

TEST(test_b_tree, test_int_key) { testBTree(T_INTEGER, 1000, 1000); }
TEST(test_b_tree, test_str_key) { testBTree(T_STRING, 100, 100); }

}  // namespace AGE
