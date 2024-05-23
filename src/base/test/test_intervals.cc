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
#include "base/expression.h"
#include "base/interval.h"
#include "base/intervals.h"
#include "gtest/gtest.h"
#include "test/test_tool.h"

namespace AGE {

using std::set;

#define _OPEN Boundary::OPEN
#define _CLOSE Boundary::CLOSE
#define _define_boundary_with_suffix_(type, suffix)                                                                   \
    type inf##suffix, int1Close##suffix(Item(T_INTEGER, 1), _CLOSE), int12Close##suffix(Item(T_INTEGER, 12), _CLOSE), \
        int123Close##suffix(Item(T_INTEGER, 123), _OPEN), int1234Close##suffix(Item(T_INTEGER, 1234), _CLOSE),        \
                                                                                                                      \
        int1Open##suffix(Item(T_INTEGER, 1), _OPEN), int12Open##suffix(Item(T_INTEGER, 12), _OPEN),                   \
        int123Open##suffix(Item(T_INTEGER, 123), _OPEN), int1234Open##suffix(Item(T_INTEGER, 1234), _OPEN),           \
                                                                                                                      \
        float5Close##suffix(Item(T_FLOAT, 5.0), _CLOSE), float56Close##suffix(Item(T_FLOAT, 56.0), _CLOSE),           \
        float567Close##suffix(Item(T_FLOAT, 567.0), _CLOSE), float5678Close##suffix(Item(T_FLOAT, 5678.0), _CLOSE),   \
                                                                                                                      \
        float5Open##suffix(Item(T_FLOAT, 5.0), _OPEN), float56Open##suffix(Item(T_FLOAT, 56.0), _OPEN),               \
        float567Open##suffix(Item(T_FLOAT, 567.0), _OPEN), float5678Open##suffix(Item(T_FLOAT, 5678.0), _OPEN),       \
                                                                                                                      \
        strACEClose##suffix(Item(T_STRING, "ACE"), _CLOSE), strBadClose##suffix(Item(T_STRING, "Bad"), _CLOSE),       \
        strCatClose##suffix(Item(T_STRING, "C a, t"), _CLOSE), strDuckClose##suffix(Item(T_STRING, "Du_ck"), _CLOSE), \
                                                                                                                      \
        strACEOpen##suffix(Item(T_STRING, "ACE"), _OPEN), strBadOpen##suffix(Item(T_STRING, "Bad"), _OPEN),           \
        strCatOpen##suffix(Item(T_STRING, "C a, t"), _OPEN), strDuckOpen##suffix(Item(T_STRING, "Du_ck"), _OPEN)

#define _define_boundary_(type) _define_boundary_with_suffix_(type, )

void TestBoundaryPositive() {
    {
        _define_boundary_(LBoundary);
        // == and its derivative opeartor
        EXPECT_TRUE(inf == inf);
        EXPECT_FALSE(inf == int1Close);
        EXPECT_TRUE(int1Close != int1Open);

        // < and its derivative operator
        EXPECT_TRUE(int1Close > inf);
        EXPECT_TRUE(inf <= inf);
        EXPECT_FALSE(strDuckClose <= strACEOpen);

        // & |
        EXPECT_EQ(inf & int1Close, int1Close);
        EXPECT_EQ(inf & strACEOpen, strACEOpen);
        EXPECT_EQ(int1Close & int1Open, int1Open);
        EXPECT_EQ(float56Open | float5Close, float5Close);
        EXPECT_EQ(strACEClose | strACEOpen, strACEClose);

        // Contain
        EXPECT_FALSE(inf.Contain(Item(T_NULL)));
        EXPECT_TRUE(inf.Contain(Item(T_STRING, "123")));
        EXPECT_TRUE(float567Open.Contain(Item(T_FLOAT, 568)));
        EXPECT_FALSE(int123Open.Contain(Item(T_INTEGER, 123)));
        EXPECT_TRUE(strACEClose.Contain(Item(T_STRING, "ACE")));
    }

    {
        _define_boundary_(RBoundary);
        // == and its derivative opeartor
        EXPECT_FALSE(inf != inf);
        EXPECT_TRUE(inf != strDuckClose);
        EXPECT_FALSE(strACEClose == strACEOpen);

        // < and its derivative operator
        EXPECT_TRUE(int1Open >= int1Open);
        EXPECT_FALSE(float56Open <= float5Close);
        EXPECT_TRUE(int1Close < inf);

        // & |
        EXPECT_EQ(inf & float5Close, float5Close);
        EXPECT_EQ(inf | strDuckClose, inf);
        EXPECT_EQ(int1Close & int1234Open, int1Close);
        EXPECT_EQ(strACEClose | strDuckClose, strDuckClose);
        EXPECT_EQ(strACEOpen | strACEClose, strACEClose);

        // Contain
        EXPECT_FALSE(inf.Contain(Item(T_UNKNOWN)));
        EXPECT_TRUE(inf.Contain(Item(T_STRING, "123")));
        EXPECT_TRUE(float567Open.Contain(Item(T_FLOAT, 566)));
        EXPECT_FALSE(strBadOpen.Contain(Item(T_STRING, "Bad")));
        EXPECT_TRUE(strACEClose.Contain(Item(T_STRING, "ACE")));
    }
}

void TestBoundaryNegative() {
    {
        _define_boundary_(LBoundary);
        // == and its derivative opeartor
        EXPECT_DEATH(int1Close == float56Open, "");
        EXPECT_DEATH(strACEClose != int1Close, "");

        // < and its derivative operator
        EXPECT_DEATH(int1234Open < strACEClose, "");
        EXPECT_DEATH(float56Open >= strACEOpen, "");

        // & |
        EXPECT_DEATH(int1Close & float5Close, "");
        EXPECT_DEATH(strACEClose | int1234Open, "");
    }

    {
        _define_boundary_(RBoundary);
        // == and its derivative opeartor
        EXPECT_DEATH(int1234Open == strACEOpen, "");
        EXPECT_DEATH(float5Close != int1Close, "");

        // < and its derivative operator
        EXPECT_DEATH(strDuckClose > int1Open, "");
        EXPECT_DEATH(float56Open <= int1234Open, "");

        // & |
        EXPECT_DEATH(float56Open & strDuckClose, "");
        EXPECT_DEATH(strACEOpen | int1Close, "");
    }
}

void TestIntervalPositive() {
    _define_boundary_with_suffix_(LBoundary, _L);
    _define_boundary_with_suffix_(RBoundary, _R);

    // Constructor
    EXPECT_EQ(Interval().GetLeftBoundary(), inf_L);
    EXPECT_EQ(Interval().GetRightBoundary(), inf_R);
    EXPECT_TRUE(Interval().IsEmpty());

    // [ , )
    //      &
    //         [ , )
    EXPECT_TRUE((Interval(int1Close_L, int12Open_R) & Interval(int123Close_L, int1234Open_R)).IsEmpty());

    // ( , ]
    //     &
    //   [ , ]
    EXPECT_EQ((Interval(float5Open_L, float567Close_R) & Interval(float56Close_L, float5678Close_R)),
              Interval(float56Close_L, float567Close_R));

    // empty
    //      &
    //       [,]
    EXPECT_TRUE((Interval() & Interval(strBadClose_L, strBadClose_R)).IsEmpty());

    // [-inf, +inf]
    //      &
    // [-inf, +inf]
    EXPECT_EQ(Interval(inf_L, inf_R) & Interval(inf_L, inf_R), Interval(inf_L, inf_R));

    // [-inf, +inf]
    //      &
    //    ( , ]
    EXPECT_EQ(Interval(inf_L, inf_R) & Interval(int1Open_L, int12Close_R), Interval(int1Open_L, int12Close_R));

    // ( , )
    //      |
    //        [, )
    EXPECT_FALSE((Interval(strACEOpen_L, strBadClose_R) | Interval(strCatClose_L, strDuckOpen_R)).first);

    // [ , ]
    //     |
    //    ( , )
    {
        auto [success, result] = Interval(int1Close_L, int123Close_R) | Interval(int12Open_L, inf_R);
        EXPECT_TRUE(success);
        EXPECT_EQ(result, Interval(int1Close_L, inf_R));
    }

    // ( , ]
    //    |
    //     empty
    EXPECT_FALSE((Interval(float5Open_L, float567Close_R) | Interval()).first);

    // [ , )
    //     |
    //     [ , )
    {
        auto [success, result] = Interval(strACEClose_L, strBadOpen_R) | Interval(strBadClose_L, strCatOpen_R);
        EXPECT_TRUE(success);
        EXPECT_EQ(result, Interval(strACEClose_L, strCatOpen_R));
    }

    // [-inf, +inf]
    //      |
    // [-inf, +inf]
    {
        auto [success, result] = Interval(inf_L, inf_R) | Interval(inf_L, inf_R);
        EXPECT_TRUE(success);
        EXPECT_EQ(result, Interval(inf_L, inf_R));
    }

    // [-inf, +inf]
    //      |
    //    [ , )
    {
        auto [success, result] = Interval(inf_L, inf_R) | Interval(float56Close_L, float5678Open_R);
        EXPECT_TRUE(success);
        EXPECT_EQ(result, Interval(inf_L, inf_R));
    }

    // Contain
    EXPECT_TRUE(Interval(inf_L, inf_R).Contain(Item(T_INTEGER, 5)));
    EXPECT_FALSE(Interval(strACEClose_L, inf_R).Contain(Item(T_UNKNOWN)));
    EXPECT_FALSE(Interval(inf_L, float5678Open_R).Contain(Item(T_NULL)));
    EXPECT_TRUE(Interval(strACEClose_L, strACEClose_R).Contain(Item(T_STRING, "ACE")));
    EXPECT_FALSE(Interval(float56Open_L, float567Close_R).Contain(Item(T_FLOAT, 45.0)));
}

void TestIntervalNegative() {
    _define_boundary_with_suffix_(LBoundary, _L);
    _define_boundary_with_suffix_(RBoundary, _R);

    // LBoundary & RBoundary type not match
    EXPECT_DEATH(Interval(int1234Close_L, float567Close_R), "");
    EXPECT_DEATH(Interval(strACEClose_L, float5678Open_R), "");

    // Interval type not match
    EXPECT_DEATH(Interval(strACEOpen_L, strDuckClose_R) & Interval(int123Close_L, int1234Close_R), "");
    EXPECT_DEATH(Interval(float5678Open_L, inf_R) & Interval(int1234Close_L, inf_R), "");
    EXPECT_DEATH(Interval(float5Close_L, float567Open_R) | Interval(int123Close_L, int1234Close_R), "");
    EXPECT_DEATH(Interval(inf_L, int1234Close_R) | Interval(strACEOpen_L, inf_R), "");
}

void TestExprToIntervals(ExpType type, const Item &value) {
    Intervals intv = TestTool::ExprToIntervals(type, value);
    const set<Interval> &iSet = intv.GetIntervals();

    // Check result
    LBoundary infL = LBoundary(), closeL = LBoundary(value, _CLOSE), openL = LBoundary(value, _OPEN);
    RBoundary infR = RBoundary(), closeR = RBoundary(value, _CLOSE), openR = RBoundary(value, _OPEN);

    // NEQ
    if (type == EXP_NEQ) {
        EXPECT_EQ(iSet.size(), 2ull);
        EXPECT_EQ(*iSet.begin(), Interval(infL, openR));
        EXPECT_EQ(*iSet.rbegin(), Interval(openL, infR));
        return;
    }

    EXPECT_EQ(iSet.size(), 1ull);
    const Interval &result = *iSet.begin();
    if (type == EXP_EQ) {
        EXPECT_EQ(result, Interval(closeL, closeR));
    } else if (type == EXP_GT) {
        EXPECT_EQ(result, Interval(openL, infR));
    } else if (type == EXP_GE) {
        EXPECT_EQ(result, Interval(closeL, infR));
    } else if (type == EXP_LT) {
        EXPECT_EQ(result, Interval(infL, openR));
    } else if (type == EXP_LE) {
        EXPECT_EQ(result, Interval(infL, closeR));
    }
}

void TestIntervalsPositive() {
    LBoundary infL = LBoundary();
    RBoundary infR = RBoundary();

    // Constructor
    TestExprToIntervals(EXP_NEQ, Item(T_FLOAT, 56));
    TestExprToIntervals(EXP_EQ, Item(T_STRING, ""));
    TestExprToIntervals(EXP_GT, Item(T_STRING, "sh"));
    TestExprToIntervals(EXP_GE, Item(T_INTEGER, 998244353));
    TestExprToIntervals(EXP_LT, Item(T_INTEGER, 1231));
    TestExprToIntervals(EXP_LE, Item(T_STRING, "___FGMIOQA"));

    // Test Intervals modification
    Item int5(T_INTEGER, 5), int9(T_INTEGER, 9), int15(T_INTEGER, 15);
    Intervals intv = TestTool::ExprToIntervals(EXP_NEQ, int5);
    const set<Interval> &iSet = intv.GetIntervals();

    // ~
    intv = ~intv;
    EXPECT_EQ(iSet.size(), 1ull);
    EXPECT_EQ(*iSet.begin(), Interval(LBoundary(int5, _CLOSE), RBoundary(int5, _CLOSE)));

    // |
    intv |= TestTool::ExprToIntervals(EXP_GT, int9);
    EXPECT_EQ(iSet.size(), 2ull);
    EXPECT_EQ(*iSet.begin(), Interval(LBoundary(int5, _CLOSE), RBoundary(int5, _CLOSE)));
    EXPECT_EQ(*iSet.rbegin(), Interval(LBoundary(int9, _OPEN), infR));

    // |
    {
        Intervals temp = intv | TestTool::ExprToIntervals(EXP_LE, int15);
        const set<Interval> &tempSet = temp.GetIntervals();
        EXPECT_EQ(tempSet.size(), 1ull);
        EXPECT_EQ(*tempSet.begin(), Interval(infL, infR));
    }

    // &
    intv &= TestTool::ExprToIntervals(EXP_LE, int15);
    EXPECT_EQ(iSet.size(), 2ull);
    EXPECT_EQ(*iSet.begin(), Interval(LBoundary(int5, _CLOSE), RBoundary(int5, _CLOSE)));
    EXPECT_EQ(*iSet.rbegin(), Interval(LBoundary(int9, _OPEN), RBoundary(int15, _CLOSE)));

    // ~
    intv = ~intv;
    EXPECT_EQ(iSet.size(), 3ull);
    {
        auto itr = iSet.begin();
        EXPECT_EQ(*(itr++), Interval(infL, RBoundary(int5, _OPEN)));
        EXPECT_EQ(*(itr++), Interval(LBoundary(int5, _OPEN), RBoundary(int9, _CLOSE)));
        EXPECT_EQ(*(itr++), Interval(LBoundary(int15, _OPEN), infR));
    }

    // Check corner case with empty and complete
    Intervals empty, complete = ~empty;
    EXPECT_EQ(intv | complete, complete);
    EXPECT_EQ(intv & complete, intv);
    EXPECT_EQ(intv | empty, intv);
    EXPECT_EQ(intv & empty, empty);
}

void TestIntervalsNegative() {
    // Type mismatch
    Intervals intv1 = TestTool::ExprToIntervals(EXP_GT, Item(T_FLOAT, 5.0)),
              intv2 = TestTool::ExprToIntervals(EXP_NEQ, Item(T_STRING, "Joao"));
    EXPECT_DEATH(intv1 | intv2, "");
    EXPECT_DEATH(intv1 & intv2, "");
}

TEST(test_intervals, positive_test) {
    TestBoundaryPositive();
    TestIntervalPositive();
    TestIntervalsPositive();
}

TEST(test_intervals, negative_test) {
    TestBoundaryNegative();
    TestIntervalNegative();
    TestIntervalsNegative();
}

#undef _define_boundary_
#undef _define_bounda_with_suffix_
#undef _CLOSE
#undef _OPEN
}  // namespace AGE
