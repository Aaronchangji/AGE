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

#include <stdlib.h>
#include <time.h>
#include <string>
#include "base/type.h"
#include "gtest/gtest.h"
#include "test/test_tool.h"

using std::string;

namespace AGE {
TEST(test_Item, test_compare) {
    EXPECT_EQ(Item(T_UNKNOWN) && Item(T_BOOL, false), Item(T_BOOL, false));
    EXPECT_EQ(Item(T_NULL) && Item(T_BOOL, false), Item(T_BOOL, false));
    EXPECT_EQ(Item(T_UNKNOWN) || Item(T_BOOL, true), Item(T_BOOL, true));
    EXPECT_EQ(Item(T_NULL) || Item(T_BOOL, true), Item(T_BOOL, true));
}

TEST(test_Item, test_constructor) {
    int n = 100, max_len = 500;
    for (int i = 0; i < n; i++) {
        // string.
        string s = TestTool::RandStr(max_len);
        Item item1(T_STRING, s);
        Item item2(T_STRING, s.c_str());
        EXPECT_EQ(0, strcmp(item1.stringVal, s.c_str()));
        EXPECT_EQ(0, strcmp(item2.stringVal, s.c_str()));

        // double.
        double f = TestTool::RandF();
        Item item3(T_FLOAT, f);
        EXPECT_EQ(f, item3.floatVal);

        // integer.
        int x = TestTool::Rand();
        Item item4(T_INTEGER, x);
        EXPECT_EQ(x, item4.integerVal);
    }
}

TEST(test_Item, test_copy) {
    int n = 100, max_len = 2500;
    for (int i = 0; i < n; i++) {
        // string.
        string s = TestTool::RandStr(max_len);
        Item item1(T_STRING, s);

        Item item2 = item1;
        EXPECT_EQ(0, strcmp(item1.stringVal, item2.stringVal));
        EXPECT_NE(item1.stringVal, item2.stringVal);

        Item item3;
        item3.copy(item1);
        EXPECT_EQ(0, strcmp(item1.stringVal, item3.stringVal));
        EXPECT_NE(item1.stringVal, item3.stringVal);

        // double.
        double f = TestTool::RandF();
        Item item4(T_FLOAT, f);

        Item item5 = item4;
        EXPECT_EQ(item4.floatVal, item5.floatVal);

        Item item6;
        item6.copy(item4);
        EXPECT_EQ(item4.floatVal, item6.floatVal);

        // integer.
        int x = TestTool::Rand();
        Item item7(T_INTEGER, x);

        Item item8 = item7;
        EXPECT_EQ(item7.integerVal, item8.integerVal);

        Item item9;
        item9.copy(item7);
        EXPECT_EQ(item7.integerVal, item9.integerVal);
    }
}

TEST(test_Item, test_move) {
    int n = 200, max_len = 200;
    for (int i = 0; i < n; i++) {
        std::string s = TestTool::RandStr(max_len);
        Item item1(T_STRING, s);
        char *ptr = item1.stringVal;

        Item item2 = std::move(item1);
        EXPECT_EQ(item1.stringVal, nullptr);
        EXPECT_EQ(item2.type, T_STRING);
        EXPECT_EQ(item2.stringVal, ptr);
        EXPECT_EQ(0, strcmp(s.c_str(), item2.stringVal));

        Item item3(T_VERTEX, 10, 1);
        Item item4 = std::move(item3);
        EXPECT_EQ(item3.stringVal, nullptr);
        EXPECT_EQ(item4.type, T_VERTEX);
        EXPECT_EQ(item4.vertex.id, 10u);
        EXPECT_EQ(item4.vertex.label, 1u);
    }
}
}  // namespace AGE
