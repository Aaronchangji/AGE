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

#pragma once

#include <glog/logging.h>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "base/aggregation.h"
#include "base/item.h"
#include "base/type.h"
#include "storage/graph.h"

using std::string;
using std::vector;

namespace AGE {

// Expression node type
typedef enum : uint8_t {
    // Monocular operator.
    EXP_NOT,
    EXP_PROP,

    // Binocular operator.
    EXP_ADD,
    EXP_SUB,
    EXP_MUL,
    EXP_DIV,
    EXP_MOD,
    // EXP_XOR,
    EXP_EQ,
    EXP_NEQ,
    EXP_LT,
    EXP_LE,
    EXP_GT,
    EXP_GE,

    // Var-length child operator.
    EXP_AND,
    EXP_OR,
    EXP_AGGFUNC,

    // Operand.
    EXP_CONSTANT,
    EXP_VARIABLE,
    EXP_PATTERN_PATH
} ExpType;

inline bool ExpType_IsCondition(ExpType t) { return (t == EXP_AND) || (t == EXP_OR) || (t == EXP_NOT); }

inline bool ExpType_IsConstant(ExpType t) { return (t == EXP_CONSTANT) || (t == EXP_PATTERN_PATH); }

inline bool ExpType_IsPredicate(ExpType t) {
    return (t == EXP_EQ) || (t == EXP_NEQ) || (t == EXP_LT) || (t == EXP_LE) || (t == EXP_GT) || (t == EXP_GE);
}

inline std::string ExpType_DebugString(ExpType t) {
    switch (t) {
    case EXP_NOT:
        return "not";
    case EXP_PROP:
        return "prop";
    case EXP_ADD:
        return "add";
    case EXP_SUB:
        return "sub";
    case EXP_MUL:
        return "mul";
    case EXP_DIV:
        return "div";
    case EXP_MOD:
        return "mod";
    case EXP_EQ:
        return "eq";
    case EXP_NEQ:
        return "neq";
    case EXP_LT:
        return "lt";
    case EXP_LE:
        return "le";
    case EXP_GT:
        return "gt";
    case EXP_GE:
        return "ge";
    case EXP_AND:
        return "and";
    case EXP_OR:
        return "or";
    case EXP_CONSTANT:
        return "constant";
    case EXP_VARIABLE:
        return "variable";
    case EXP_AGGFUNC:
        return "aggFunc";
    case EXP_PATTERN_PATH:
        return "patternPath";
    default:
        return "unkown";
    }
}

#define VAR_KEY_LABEL LABEL_PID

// Expression tree node
// each instance of Expression represents a node in the expression tree
class Expression {
   public:
    explicit Expression(ExpType type = EXP_CONSTANT) : type(type), unionBytes(0), value(T_UNKNOWN) {}
    Expression(ExpType type, AggType aggType, bool isDistinct = false) : type(type), aggFunc(aggType, isDistinct) {
        assert(type == EXP_AGGFUNC);
    }
    Expression(ExpType type, int keyOrColIdx) : type(type), key(keyOrColIdx), value(T_UNKNOWN) {
        assert(type == EXP_PROP || type == EXP_VARIABLE);
    }
    Expression(ExpType type, const Item& item) : type(type), value(item) {
        assert(type == EXP_CONSTANT || type == EXP_PATTERN_PATH);
    }
    Expression(ExpType type, Item&& item) : type(type), value(std::move(item)) {
        assert(type == EXP_CONSTANT || type == EXP_PATTERN_PATH);
    }

    Expression& operator=(const Expression& rhs) {
        type = rhs.type;
        unionBytes = rhs.unionBytes;
        value = rhs.value;

        RemoveChildren();
        for (const Expression& expr : rhs.GetChildren()) EmplaceChild(expr);

        return *this;
    }

    Expression(const Expression& rhs) { *this = rhs; }
    Expression(Expression&& rhs) : type(rhs.type), unionBytes(rhs.unionBytes), value(std::move(rhs.value)) {
        child.swap(rhs.GetChildren());
        rhs.type = EXP_CONSTANT;
        rhs.SetValue(Item(T_UNKNOWN));
    }

    ~Expression() { child.clear(); }

    // NOT really meaningful: just for containers to distingush two DIFFERENT expressions
    bool operator<(const Expression& rhs) const {
        // printf("<: lhs exp:[%s]\n rhs exp:[%s]\n", DebugString().c_str(), rhs.DebugString().c_str());
        if (type == rhs.type) {
            switch (type) {
            case EXP_ADD:
            case EXP_SUB:
            case EXP_MUL:
            case EXP_DIV:
            case EXP_MOD:
            case EXP_EQ:
            case EXP_NEQ:
            case EXP_LT:
            case EXP_LE:
            case EXP_GT:
            case EXP_GE:
            case EXP_AND:
            case EXP_OR:
            case EXP_NOT:
            case EXP_CONSTANT: {
                int res = Item::Compare(value, rhs.value);
                if (res != 0) return res < 0;
                break;
            }
            case EXP_VARIABLE:
                if (colIdx != rhs.colIdx) return colIdx < rhs.colIdx;
                break;
            case EXP_PROP:
                if (key != rhs.key) return key < rhs.key;
                break;
            case EXP_AGGFUNC:
                if (aggFunc.type != rhs.aggFunc.type) return aggFunc.type < rhs.aggFunc.type;
                if (aggFunc.NeedDistinct() != rhs.aggFunc.NeedDistinct())
                    return aggFunc.NeedDistinct() < rhs.aggFunc.NeedDistinct();
                break;
            default:
                break;
            }
            const vector<Expression>& lhschild = GetChildren();
            const vector<Expression>& rhschild = rhs.GetChildren();
            if (lhschild.size() != rhschild.size()) return lhschild.size() < rhschild.size();
            for (u32 i = 0; i < lhschild.size(); i++) {
                if (lhschild[i] < rhschild[i]) return true;
                if (rhschild[i] < lhschild[i]) return false;
            }

            // lhs == rhs
            return false;
        } else {
            return type < rhs.type;
        }
    }

    bool operator==(const Expression& rhs) const {
        // printf("==: lhs exp:[%s]\n rhs exp:[%s]\n", DebugString().c_str(), rhs.DebugString().c_str());
        //  this >= rhs && this <= rhs
        if (!(*this < rhs) && !(rhs < *this)) return true;
        return false;
    }

    Item Eval(const Row* r = nullptr, const Graph* g = nullptr, const Item* curItem = nullptr) {
        for (Expression& exp : child) {
            exp.Eval(r, g, curItem);

            // Pattern paths cannot be evaluated
            if (exp.type == EXP_PATTERN_PATH) return Item(T_UNKNOWN);
        }
        return Calc(r, g, curItem);
    }

    Item Calc(const Row* r = nullptr, const Graph* g = nullptr, const Item* curItem = nullptr) {
#define _V0 (child[0].value)
#define _V1 (child[1].value)
        switch (type) {
        case EXP_ADD:
            return value = _V0 + _V1;
        case EXP_SUB:
            return value = _V0 - _V1;
        case EXP_MUL:
            return value = _V0 * _V1;
        case EXP_DIV:
            return value = _V0 / _V1;
        case EXP_MOD:
            return value = _V0 % _V1;
        case EXP_AND:
            value = _V0 && _V1;
            for (size_t i = 2; i < child.size(); i++) value = value && child[i].value;
            return value;
        case EXP_OR:
            value = _V0 || _V1;
            for (size_t i = 2; i < child.size(); i++) value = value || child[i].value;
            return value;
        case EXP_EQ:
            return value = _V0 == _V1;
        case EXP_NEQ:
            return value = _V0 != _V1;
        case EXP_LT:
            return value = _V0 < _V1;
        case EXP_LE:
            return value = _V0 <= _V1;
        case EXP_GT:
            return value = _V0 > _V1;
        case EXP_GE:
            return value = _V0 >= _V1;
        case EXP_NOT:
            return value = !_V0;
        case EXP_PROP:
            if (g == nullptr || _V0.type == Item(T_UNKNOWN)) {
                return value = Item(T_UNKNOWN);
            } else if (key == INVALID_PROP) {
                return value = Item(T_NULL);
            } else if (key == VAR_KEY_LABEL) {
                assert(_V0.type & (T_VERTEX | T_EDGE));
                LabelId label = _V0.type == T_VERTEX ? _V0.vertex.label : _V0.edge.label;
                return value = Item(T_INTEGER, static_cast<int>(label));
            } else {
                return value = g->getProp(_V0, key);
            }
        case EXP_CONSTANT:
        case EXP_PATTERN_PATH:
            return value;
        case EXP_VARIABLE:
            if (r == nullptr) return value = Item(T_UNKNOWN);
            if (colIdx == COLINDEX_COMPRESS)
                return curItem != nullptr ? value = *curItem : value = Item(T_UNKNOWN);
            else
                return value = r->at(colIdx);
        case EXP_AGGFUNC:
        default:
            return value = Item(T_UNKNOWN);
        }
#undef _V0
#undef _V1
    }

    bool ChildNumCheck() const {
        bool valid = true;
        for (const Expression& expr : GetChildren()) valid &= expr.ChildNumCheck();
        switch (type) {
        case EXP_ADD:
        case EXP_SUB:
        case EXP_MUL:
        case EXP_DIV:
        case EXP_MOD:
        case EXP_EQ:
        case EXP_NEQ:
        case EXP_LT:
        case EXP_LE:
        case EXP_GT:
        case EXP_GE:
            valid &= GetChildren().size() == 2ull;
            break;
        case EXP_AND:
        case EXP_OR:
            valid &= GetChildren().size() >= 2ull;
            break;
        case EXP_NOT:
        case EXP_PROP:
        case EXP_AGGFUNC:
            valid &= GetChildren().size() == 1ull;
            break;
        case EXP_CONSTANT:
        case EXP_VARIABLE:
        case EXP_PATTERN_PATH:
            valid &= GetChildren().size() == 0ull;
            break;
        default:
            valid = false;
        }
        CHECK(valid) << DebugString() << ": has invalid number of children";
        return valid;
    }

    bool IsEmpty() const { return type == EXP_CONSTANT && value.type == T_UNKNOWN && !child.size(); }
    bool IsPredicate() const { return ExpType_IsPredicate(type); }
    bool IsCondition() const { return ExpType_IsCondition(type); }
    bool IsConstant() const { return ExpType_IsConstant(type); }
    // Return true if this is like: a > 5; c = 10; d != "Joao"
    bool IsVarConstantCmpPredicate() const {
        if (!IsPredicate()) return false;
        assert(GetChildren().size() == 2ull);
        const Expression *lhs = &GetChild(0), *rhs = &GetChild(1);
        if (lhs->type != EXP_VARIABLE) std::swap(lhs, rhs);
        return lhs->type == EXP_VARIABLE && rhs->type == EXP_CONSTANT;
    }
    bool HasAggregate() const {
        if (type == EXP_AGGFUNC) return true;
        for (const Expression& exp : child)
            if (exp.HasAggregate()) return true;
        return false;
    }
    bool HasCompressCol() const {
        if (type == EXP_VARIABLE && colIdx == COLINDEX_COMPRESS) return true;
        for (const Expression& exp : child) {
            if (exp.HasCompressCol()) return true;
        }
        return false;
    }
    bool HasDistinct() const {
        if (type == EXP_AGGFUNC && aggFunc.NeedDistinct()) return true;
        for (const Expression& exp : child) {
            if (exp.HasDistinct()) return true;
        }
        return false;
    }

    u32 GetSize() const {
        u32 ret = 1;
        for (Expression exp : child) ret += exp.GetSize();
        return ret;
    }

    Item& GetValue() { return value; }
    const Item& GetValue() const { return value; }
    vector<Expression>& GetChildren() { return child; }
    const vector<Expression>& GetChildren() const { return child; }
    Expression& GetChild(u32 index) { return child[index]; }
    const Expression& GetChild(u32 index) const { return child[index]; }
    void SetValue(const Item& item) { value = item; }
    void SetValue(Item&& item) { value = std::move(item); }

    template <typename... Args>
    Expression& EmplaceChild(Args&&... args) {
        child.emplace_back(std::forward<Args>(args)...);
        return child.back();
    }

    Expression& RemoveChildren() {
        child.clear();
        return *this;
    }

    Expression CopyRoot() const {
        Expression ret = *this;
        return ret.RemoveChildren();
    }

    std::string DebugString(int depth = 0) const {
        std::string ret = "\n";
        DebugString_(ret, depth);
        return ret;
    }

    ExpType type;
    union {
        // For EXP_VARIABLE and EXP_PROP in planning.
        int varId;
        // For EXP_VARIABLE in execution.
        ColIndex colIdx;
        // For EXP_PROP.
        int key;
        // For EXP_AGGFUNC.
        AggFunc aggFunc;
        // Bytes
        u64 unionBytes;
    };

   private:
    // Store intermediate result or constant value
    Item value;
    // Store child Expression node of this node
    std::vector<Expression> child;

    void DebugString_(string& ret, u32 shift) const {
        for (u32 i = 0; i < shift; i++) ret += "    ";
        ret += ExpType_DebugString(type);
        switch (type) {
        case EXP_CONSTANT:
            ret += " " + value.DebugString();
            break;
        case EXP_VARIABLE:
            ret += " " + std::to_string(colIdx);
            break;
        case EXP_PROP:
            ret += " " + std::to_string(key);
            break;
        case EXP_PATTERN_PATH:
            ret += " " + std::to_string(value.integerVal);
            break;
        case EXP_AGGFUNC:
            ret += aggFunc.DebugString();
            break;
        default:
            break;
        }
        ret += "\n";

        for (Expression exp : child) {
            exp.DebugString_(ret, shift + 1);
        }
    }
};
}  // namespace AGE
