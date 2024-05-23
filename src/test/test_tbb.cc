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

#include <set>
#include <thread>
#include "gtest/gtest.h"
#include "tbb/concurrent_hash_map.h"
#include "test/test_tool.h"

using TEST_hmap = tbb::concurrent_hash_map<int, int>;
using TEST_hmap_ac = TEST_hmap::accessor;
using std::set;
using std::thread;
using std::vector;

void _test_readwrite_hmap(TEST_hmap* m, int x, int t) {
    for (int i = 0; i < t; i++) {
        {
            TEST_hmap_ac ac;
            m->insert(ac, x);
            ac->second = x;
        }
        {
            TEST_hmap_ac ac;
            bool ret = m->insert(ac, x);
            EXPECT_FALSE(ret);
            EXPECT_EQ(ac->second, x);
        }
    }
}

void test_hmap(int n, int t) {
    vector<int> arr(n);
    for (int i = 0; i < n; i++) arr.push_back(TestTool::Rand());

    TEST_hmap m;

    // test return value.
    TEST_hmap_ac ac;
    bool ret = m.insert(ac, 1);
    EXPECT_TRUE(ret);

    vector<thread> ths;
    for (int i = 0; i < n; i++) {
        ths.emplace_back(&_test_readwrite_hmap, &m, arr[i], t);
    }
    for (thread& th : ths) {
        th.join();
    }
}

TEST(test_tbb, test_conccurent_hash_map) {
    test_hmap(8, 20);
    test_hmap(16, 20);
}
