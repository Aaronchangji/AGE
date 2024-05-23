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
#include <unordered_set>
#include <utility>
#include <vector>
#include "./test_tool.h"
#include "base/expression.h"
#include "base/type.h"
#include "gtest/gtest.h"
#include "storage/red_black_tree.h"

namespace AGE {
using std::sort;
using std::string;
using std::unordered_set;
using std::vector;
struct ItemHash {
    size_t operator()(const Item vec) const {
        std::vector<char> bytes;
        if (vec.type == T_STRING) {
            for (u32 j = 0; vec.stringVal[j] != '\0'; j++) bytes.push_back(vec.stringVal[j]);
        } else {
            for (const char& byte : vec.bytes) bytes.push_back(byte);
        }
        return Math::MurmurHash64_x64(&bytes[0], bytes.size());
    }
};

struct Equal {
    size_t operator()(const Item lhs, const Item rhs) const { return (lhs == rhs); }
};

/**
 * @brief Check the correctness of RBTree result. This is done by: checking the size of the tree and compare the result
 * with a unordered_set. It is expected that the size of Tree should be the same with that of STL set and each record in
 * the set should also exists in the Tree.
 * @param rbt the Red Black Tree
 * @param set the unordered_set
 * @param rbtCheckTime the variable used to test the RBTree's performance of searching elements
 * @param setCheckTime the variable used to test the unordered_set's performance of searching elements
 */
void checkRBTreeResult(RedBlackTree* rbt, unordered_set<Item, ItemHash, Equal>* set, u64& rbtCheckTime,
                       u64& setCheckTime) {
    vector<Item> result;
    for (auto item : *set) result.emplace_back(item);
    sort(result.begin(), result.end());

    size_t sizeOfTree = rbt->getTraverseSize();
    EXPECT_EQ(sizeOfTree, set->size());

    bool rbtResultMatch = true;
    u64 rbtBeg = TestTool::getTimeNs();
    for (size_t i = 0; i < set->size(); i++) rbtResultMatch = rbt->count(result[i]);
    rbtCheckTime = TestTool::getTimeNs() - rbtBeg;
    EXPECT_TRUE(rbtResultMatch);

    bool setResultMatch = true;
    u64 setBeg = TestTool::getTimeNs();
    for (size_t i = 0; i < set->size(); i++) setResultMatch = set->count(result[i]);
    setCheckTime = TestTool::getTimeNs() - setBeg;
    EXPECT_TRUE(setResultMatch);
}
/**
 * @brief Test the correctness of inserting null value into the Red Black Tree. It is epxected that only 1 UNKNOWN
 * record could be inserted into it.
 *
 * @param initSize the initial size of Red Black Tree consisting of Item(T_INTEGER)
 * @param numOfRecord the number of null value that is needed to insert.
 */
void testRBTreeNULLItem(int initSize, int numOfRecord) {
    RedBlackTree rbt;
    ASSERT_TRUE(initSize >= 0 && numOfRecord >= 0);
    for (int i = 0; i < initSize; i++) {
        Item val(T_INTEGER, i);
        rbt.insert(val);
    }
    EXPECT_EQ((uint64_t)initSize, rbt.getTraverseSize()) << "Problem with Tree Size before null value\n";
    Item nullItem(T_NULL);
    for (int i = 0; i < numOfRecord; i++) {
        rbt.insert(nullItem);
    }
    EXPECT_EQ((uint64_t)initSize + 1, rbt.getTraverseSize()) << "Problem with Tree Size after null value\n";
}
/**
 * @brief Initialize the vector object for a given type. This is a helper function and
 * will not be tested
 *
 * @param vec the vector that is going be be initialized
 * @param type the Item type that is going to be stored inside
 * @param numOfItem number of Item needed to insert the vector
 * @param numOfNull number of Null Item needed to stored inside the vector
 * @param numOfUnknown numbe of Unknown Item needed to stored inside the vector
 */
void setVector(vector<Item>& vec, ItemType type, int numOfItem, int numOfNull, int numOfUnknown) {
    ASSERT_TRUE(type == T_INTEGER || type == T_STRING);
    for (int i = 0; i < numOfItem; i++) {
        if (type == T_INTEGER) {
            Item val(type, TestTool::Rand());
            vec[i] = std::move(val);
        } else if (type == T_STRING) {
            Item val(type, TestTool::RandStr(16));
            vec[i] = std::move(val);
        }
    }
    Item nullVal(T_NULL);
    Item unknownVal(T_UNKNOWN);
    for (int i = 0; i < numOfNull; i++) vec.emplace_back(nullVal);
    for (int i = 0; i < numOfUnknown; i++) vec.emplace_back(unknownVal);
}

void performanceTest(ItemType type, int numOfRecords, int numOfNull, int numOfUnknown, int numOfTimes) {
    ASSERT_TRUE(type == T_INTEGER || type == T_STRING);
    RedBlackTree rbt;
    unordered_set<Item, ItemHash, Equal> aSet;
    auto rd = std::random_device{};
    auto rng = std::default_random_engine{rd()};
    vector<Item> insertData(numOfRecords);
    setVector(insertData, type, numOfRecords, numOfNull, numOfUnknown);

    u64 rbtIAVG = 0, setIAVG = 0, rbtCheckAVG = 0, setCheckAVG = 0;
    for (int curTime = 0; curTime < numOfTimes; curTime++) {
        u64 rbtBeg, setBeg;

        std::shuffle(std::begin(insertData), std::end(insertData), rng);
        rbtBeg = TestTool::getTimeNs();
        for (int i = 0; i < numOfRecords; i++) rbt.insert(insertData[i]);
        rbtIAVG += (TestTool::getTimeNs() - rbtBeg);

        setBeg = TestTool::getTimeNs();
        for (int i = 0; i < numOfRecords; i++) aSet.insert(insertData[i]);
        setIAVG += (TestTool::getTimeNs() - setBeg);

        std::shuffle(std::begin(insertData), std::end(insertData), rng);
        rbtBeg = TestTool::getTimeNs();
        for (int i = 0; i < numOfRecords; i++) rbt.count(insertData[i]);
        rbtCheckAVG += (TestTool::getTimeNs() - rbtBeg);

        setBeg = TestTool::getTimeNs();
        for (int i = 0; i < numOfRecords; i++) aSet.count(insertData[i]);
        setCheckAVG += (TestTool::getTimeNs() - setBeg);

        rbt.freeSpace();
        aSet.clear();
        setVector(insertData, type, numOfRecords, numOfNull, numOfUnknown);
    }
    rbtIAVG = rbtIAVG / (double)(numOfTimes);
    setIAVG = setIAVG / (double)(numOfTimes);
    rbtCheckAVG = rbtCheckAVG / (double)(numOfTimes);
    setCheckAVG = setCheckAVG / (double)(numOfTimes);

    string testType = type == T_INTEGER ? "Integer" : "String";
    printf(
        "Testing %d records with RBT and unordered_set and comparing their performance for %d times.\n Tested key type "
        ": %s\n",
        numOfRecords, numOfTimes, testType.c_str());

    printf(
        "AGE::RedBlackTree\t avg insert time cost: %.6f ms\nstd::unordered_set\t avg insert time cost: %.6f ms\n",
        rbtIAVG / 1e6, setIAVG / 1e6);
    printf(
        "AGE::RedBlackTree\t avg check  time cost: %.6f ms\nstd::unordered_set\t avg check  time cost: %.6f ms\n",
        rbtCheckAVG / 1e6, setCheckAVG / 1e6);
    rbt.freeSpace();
}
/**
 * @brief This test is specific for item type T_INTEGER and T_STRING
 *
 * @param type the type of Item that is going to be tested
 * @param n the number of records that is going to be tested
 * @param m
 */
void testRBTree(ItemType type, int n) {
    ASSERT_TRUE(type == T_INTEGER || type == T_STRING);
    RedBlackTree rbt;
    unordered_set<Item, ItemHash, Equal> aSet;
    vector<Item> insertData(n);
    u64 rbtITime = 0, setITime = 0, rbtCheckTime = 0, setCheckTime = 0, seedGenTime = 0;

    seedGenTime = TestTool::getTimeNs();
    setVector(insertData, type, n, 0, 0);
    seedGenTime = TestTool::getTimeNs() - seedGenTime;

    rbtITime = TestTool::getTimeNs();
    for (int i = 0; i < n; i++) rbt.insert(insertData[i]);
    rbtITime = TestTool::getTimeNs() - rbtITime;

    setITime = TestTool::getTimeNs();
    for (int i = 0; i < n; i++) aSet.insert(insertData[i]);
    setITime = TestTool::getTimeNs() - setITime;

    checkRBTreeResult(&rbt, &aSet, rbtCheckTime, setCheckTime);

    string testType = type == T_INTEGER ? "Integer" : "String";
    printf("Testing %d records with RBT and unordered_set. Tested key type:%s", n, testType.c_str());
    printf("Seed generate time cost: %.6f ms\n", seedGenTime / 1e6);
    printf("AGE::RedBlackTree\tinsert time cost: %.6f ms\nstd::unordered_set\tinsert time cost: %.6f ms\n",
           rbtITime / 1e6, setITime / 1e6);
    printf("AGE::RedBlackTree\tcheck  time cost: %.6f ms\nstd::unordered_set\tcheck  time cost: %.6f ms\n",
           rbtCheckTime / 1e6, setCheckTime / 1e6);
    rbt.freeSpace();
}

TEST(test_red_black_tree, test_int_key) { testRBTree(T_INTEGER, 1000); }
TEST(test_red_black_tree, test_str_key) { testRBTree(T_STRING, 100); }
TEST(test_red_black_tree, test_null_key) { testRBTreeNULLItem(100, 10); }
TEST(test_red_black_tree, test_performance) { performanceTest(T_STRING, 1000, 100, 10, 100); }
TEST(test_red_black_tree, test_performance_more_NULL) { performanceTest(T_INTEGER, 500, 150, 10, 100); }

}  // namespace AGE
