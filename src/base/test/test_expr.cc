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

#include "base/item.h"
#include "gtest/gtest.h"

#include "base/expression.h"
#include "base/type.h"

namespace AGE {

// Return expression of t(l,r);
Expression simpleExp(Item l, Item r, ExpType t) {
    Expression op(t);
    op.EmplaceChild(EXP_CONSTANT, l);
    op.EmplaceChild(EXP_CONSTANT, r);
    return op;
}

// Return the result of t(l, r). e.g. add(10, 20).
Item simpleExpResult(Item l, Item r, ExpType t) { return simpleExp(l, r, t).Eval(); }

// Return the result of t(lhs, rhs). e.g. or(10>20, 5<6)
Item complexExpResult(const Expression& lhs, const Expression& rhs, ExpType t) {
    Expression op(t);
    op.EmplaceChild(lhs);
    op.EmplaceChild(rhs);
    // printf("%s --> %s\n", op.DebugString().c_str(), op.Eval().DebugString().c_str());
    return op.Eval();
}

TEST(test_expr, test_copy_construct) {
    Expression exp(EXP_OR);
    exp.EmplaceChild(simpleExp(Item(T_INTEGER, 10), Item(T_INTEGER, 20), EXP_GT));
    exp.EmplaceChild(simpleExp(Item(T_INTEGER, 30), Item(T_INTEGER, 10), EXP_GE));
    Expression newExp(exp);
    EXPECT_EQ(exp, newExp);
    // printf("%s\n%s\n", newExp.DebugString().c_str(), exp.DebugString().c_str());

    newExp = Expression();
    EXPECT_TRUE(newExp.IsEmpty());
}

TEST(test_expr, test_move_construct) {
    Expression exp(EXP_AND);
    exp.EmplaceChild(simpleExp(Item(T_INTEGER, 10), Item(T_INTEGER, 20), EXP_GT));
    exp.EmplaceChild(simpleExp(Item(T_INTEGER, 30), Item(T_INTEGER, 10), EXP_GE));
    Expression newExp(std::move(exp));
    EXPECT_TRUE(exp.IsEmpty());
    // printf("%s\n%s\n", newExp.DebugString().c_str(), exp.DebugString().c_str());
}

TEST(test_expr, test_compare) {
    Expression a(EXP_EQ), b(EXP_EQ);
    a.EmplaceChild(Expression(EXP_VARIABLE, 4));
    a.EmplaceChild(Expression(EXP_CONSTANT, Item(T_INTEGER, 1)));
    b.EmplaceChild(Expression(EXP_CONSTANT, Item(T_STRING, "1")));
    b.EmplaceChild(Expression(EXP_VARIABLE, 3));
    EXPECT_TRUE(b < a);
    EXPECT_FALSE(a < b);
    EXPECT_EQ(a, a);
    EXPECT_EQ(b, b);
}

TEST(test_expr, test_simple_eval) {
    // 10 > 20 = true.
    EXPECT_FALSE(simpleExpResult(Item(T_INTEGER, 10), Item(T_INTEGER, 20), EXP_GT));
    // null == 20 = null.
    EXPECT_EQ(simpleExpResult(Item(T_NULL), Item(T_INTEGER, 20), EXP_EQ).type, T_NULL);
    // null == unkown = unknown.
    EXPECT_EQ(simpleExpResult(Item(T_NULL), Item(T_UNKNOWN), EXP_EQ).type, T_UNKNOWN);
    // null == null = null.
    EXPECT_EQ(simpleExpResult(Item(T_NULL), Item(T_NULL), EXP_EQ).type, T_NULL);
    // 0.5 > unkown = unknown.
    EXPECT_EQ(simpleExpResult(Item(T_FLOAT, 0.5), Item(T_UNKNOWN), EXP_GT).type, T_UNKNOWN);
    // "abc" == "abc" = true.
    EXPECT_TRUE(simpleExpResult(Item(T_STRING, "abc"), Item(T_STRING, "abc"), EXP_EQ));
    // 10 + 20 = 30.
    EXPECT_EQ(simpleExpResult(Item(T_INTEGER, 10), Item(T_INTEGER, 20), EXP_ADD), Item(T_INTEGER, 30));
    // 10 + null = null.
    EXPECT_EQ(simpleExpResult(Item(T_INTEGER, 10), Item(T_NULL), EXP_ADD).type, T_NULL);
    // 10 - 20 = -10.
    EXPECT_EQ(simpleExpResult(Item(T_INTEGER, 10), Item(T_INTEGER, 20), EXP_SUB), Item(T_INTEGER, -10));
    // 20 * 10 = 200.
    EXPECT_EQ(simpleExpResult(Item(T_INTEGER, 20), Item(T_INTEGER, 10), EXP_MUL), Item(T_INTEGER, 200));
}

TEST(test_expr, test_complex_eval) {
    Item res;
    // 10 > 20 or 30 > 10.
    res = AGE::complexExpResult(simpleExp(Item(T_INTEGER, 10), Item(T_INTEGER, 20), EXP_GT),
                                    simpleExp(Item(T_INTEGER, 30), Item(T_INTEGER, 10), EXP_GE), EXP_OR);
    EXPECT_TRUE(res);

    // 20 <= 20 and 30 > 10.
    res = AGE::complexExpResult(simpleExp(Item(T_INTEGER, 20), Item(T_INTEGER, 20), EXP_LE),
                                    simpleExp(Item(T_INTEGER, 30), Item(T_INTEGER, 10), EXP_GT), EXP_AND);
    EXPECT_TRUE(res);
}
}  // namespace AGE
