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
// limitations under the License.

#pragma once

#include <algorithm>
#include <string>
#include <utility>
#include "base/item.h"
#include "base/serializer.h"
#include "base/type.h"

namespace AGE {

using std::make_pair;
using std::pair;
using std::string;

#define _define_derivative_comparison_operator_(type)                  \
    bool operator!=(const type &rhs) const { return !(*this == rhs); } \
    bool operator>(const type &rhs) const { return rhs < *this; }      \
    bool operator<=(const type &rhs) const { return !(rhs < *this); }  \
    bool operator>=(const type &rhs) const { return !(*this < rhs); }

#define _define_derivative_assign_operator_(type)                     \
    type &operator&=(const type &rhs) { return *this = *this & rhs; } \
    type &operator|=(const type &rhs) { return *this = *this | rhs; }

// Notice that the main reason we split Boundary into LBoundary & RBoundary
//  is that infinite means different (-inf or +inf) in L or R
class Boundary {
   public:
    static constexpr bool CLOSE = true;
    static constexpr bool OPEN = false;
    ItemType Type() const { return value.type; }
    const Item &Value() const { return value; }
    bool IsClosed() const { return isClosed; }
    void ToString(string *s) const {
        value.ToString(s);
        Serializer::appendBool(s, isClosed);
    }

    void FromString(const string &s, size_t &pos) {
        value = Item::CreateFromString(s, pos);
        isClosed = Serializer::readBool(s, pos);
    }

    static Boundary CreateFromString(const string &s, size_t &pos) {
        Boundary ret;
        ret.FromString(s, pos);
        return ret;
    }

   protected:
    Boundary(const Item &value, bool isClosed) : value(value), isClosed(isClosed) {}
    Boundary(Item &&value, bool isClosed) : value(std::move(value)), isClosed(isClosed) {}
    // No input args means infinite boundary
    Boundary() : value(T_UNKNOWN), isClosed(false) {}
    static void TypeAssert(const Boundary &lhs, const Boundary &rhs) {
        assert(lhs.Type() == rhs.Type() || ((lhs.Type() | rhs.Type()) & T_UNKNOWN));
    }

   private:
    // value.type = T_UNKNOWN --> negative infinite
    // value.type = T_NULL --> invalid value
    Item value;

    // isClosed = true --> Boundary contains value
    // isClosed = false --> Boundary do not contains value
    bool isClosed;
};

class LBoundary : public Boundary {
   public:
    LBoundary(const Item &value, bool isClosed) : Boundary(value, isClosed) {}
    LBoundary(Item &&value, bool isClosed) : Boundary(std::move(value), isClosed) {}
    LBoundary() : Boundary() {}

    using Boundary::operator=;

    bool Contain(const Item &item) const {
        // Invalid item
        if (item.type == T_UNKNOWN || item.type == T_NULL) return false;

        // Infinite
        if (Type() == T_UNKNOWN) return true;

        // Ensure type is matched
        assert(Type() == item.type);

        return (item > Value()) || (item == Value() && IsClosed());
    }

    // Intersect & Union
    LBoundary operator&(const LBoundary &rhs) const { return *this >= rhs ? *this : rhs; }
    LBoundary operator|(const LBoundary &rhs) const { return *this <= rhs ? *this : rhs; }
    _define_derivative_assign_operator_(LBoundary);

    // Comparison
    bool operator==(const LBoundary &rhs) const {
        TypeAssert(*this, rhs);

        // Boundary equals in 2 situations:
        //  1. value is arithmetically equal and closeness is equal
        //  2. Both boundary are infinite
        return (Value() == rhs.Value() && IsClosed() == rhs.IsClosed()) || ((Type() & rhs.Type()) == T_UNKNOWN);
    }
    bool operator<(const LBoundary &rhs) const {
        TypeAssert(*this, rhs);

        // Both are infinite
        if ((Type() & rhs.Type()) == T_UNKNOWN) return false;

        // One is infinte
        if ((Type() | rhs.Type()) & T_UNKNOWN) return Type() == T_UNKNOWN;

        assert(Type() == rhs.Type());
        return Value() < rhs.Value() || (Value() == rhs.Value() && IsClosed() && !rhs.IsClosed());
    }
    _define_derivative_comparison_operator_(LBoundary);

    string DebugString() const {
        string ret = "";

        // brackets
        ret += IsClosed() ? "[" : "(";

        // value
        if (Type() == T_UNKNOWN)
            ret += "{-infinite}";
        else
            ret += Value().DebugString();

        return ret;
    }
};

class RBoundary : public Boundary {
   public:
    RBoundary(const Item &value, bool isClosed) : Boundary(value, isClosed) {}
    RBoundary(Item &&value, bool isClosed) : Boundary(std::move(value), isClosed) {}
    RBoundary() : Boundary() {}

    using Boundary::operator=;

    bool Contain(const Item &item) const {
        // Invalid item
        if (item.type == T_UNKNOWN || item.type == T_NULL) return false;

        // Infinite
        if (Type() == T_UNKNOWN) return true;

        // Ensure type is matched
        assert(Type() == item.type);

        return (item < Value()) || (item == Value() && IsClosed());
    }

    // Intersect & Union
    RBoundary operator&(const RBoundary &rhs) const { return *this <= rhs ? *this : rhs; }
    RBoundary operator|(const RBoundary &rhs) const { return *this >= rhs ? *this : rhs; }
    _define_derivative_assign_operator_(RBoundary);

    // Comparison
    bool operator==(const RBoundary &rhs) const {
        TypeAssert(*this, rhs);

        // Boundary equals in 2 situations:
        //  1. value is arithmetically equal and closeness is equal
        //  2. Both boundary are infinite
        return (Value() == rhs.Value() && IsClosed() == rhs.IsClosed()) || ((Type() & rhs.Type()) == T_UNKNOWN);
    }
    bool operator<(const RBoundary &rhs) const {
        TypeAssert(*this, rhs);

        // Both are infinite
        if ((Type() & rhs.Type()) == T_UNKNOWN) return false;

        // One is infinte
        if ((Type() | rhs.Type()) & T_UNKNOWN) return rhs.Type() == T_UNKNOWN;

        assert(Type() == rhs.Type());
        return Value() < rhs.Value() || (Value() == rhs.Value() && !IsClosed() && rhs.IsClosed());
    }

    _define_derivative_comparison_operator_(RBoundary);

    string DebugString() const {
        string ret = "";

        // value
        if (Type() == T_UNKNOWN)
            ret += "{+infinite}";
        else
            ret += Value().DebugString();

        // brackets
        ret += IsClosed() ? "]" : ")";
        return ret;
    }
};

class Interval {
   private:
    bool empty;

    LBoundary l;
    RBoundary r;

   public:
    Interval() : empty(true) {}
    Interval(const LBoundary &l, const RBoundary &r) : l(l), r(r) {
        TypeAssert();
        UpdateEmpty();
    }
    Interval(LBoundary &&l, RBoundary &&r) : l(std::move(l)), r(std::move(r)) {
        TypeAssert();
        UpdateEmpty();
    }

    Interval(const Interval &rhs) : empty(rhs.empty), l(rhs.l), r(rhs.r) { TypeAssert(); }
    Interval(Interval &&rhs) : empty(rhs.empty), l(std::move(rhs.l)), r(std::move(rhs.r)) { TypeAssert(); }

    Interval &operator=(const Interval &rhs) {
        l = rhs.l;
        r = rhs.r;
        empty = rhs.empty;
        TypeAssert();
        return *this;
    }

    Interval &operator=(Interval &&rhs) {
        l = std::move(rhs.l);
        r = std::move(rhs.r);
        empty = rhs.empty;
        TypeAssert();
        return *this;
    }

    const LBoundary &GetLeftBoundary() const { return l; }
    const RBoundary &GetRightBoundary() const { return r; }

    bool operator==(const Interval &rhs) const {
        if (IsEmpty() || rhs.IsEmpty()) return false;
        return l == rhs.l && r == rhs.r;
    }
    bool operator<(const Interval &rhs) const {
        // First compare R boundary
        if (r != rhs.r) return r < rhs.r;
        return l < rhs.l;
    }
    _define_derivative_comparison_operator_(Interval);

    void ToString(string *s) const {
        Serializer::appendBool(s, empty);
        l.ToString(s);
        r.ToString(s);
    }

    void FromString(const string &s, size_t &pos) {
        empty = Serializer::readBool(s, pos);
        l.FromString(s, pos);
        r.FromString(s, pos);
        TypeAssert();
    }

    static Interval CreateFromString(const string &s, size_t &pos) {
        Interval interval;
        interval.FromString(s, pos);
        return interval;
    }

    // - L & R both infinite --> Return T_UNKNOWN
    // - L or R is inifite --> Return type other than T_UNKNWON among L R
    // - L & R bot not infinite --> Return their equal type (ensure by TypeAssert())
    ItemType Type() const {
        TypeAssert();
        u16 type = l.Type() | r.Type();
        return static_cast<ItemType>((type == T_UNKNOWN) ? T_UNKNOWN : (type & T_UNKNOWN) ? (type ^ T_UNKNOWN) : type);
    }

    bool IsEmpty() const { return empty; }
    std::string DebugString() const { return IsEmpty() ? "(empty)" : l.DebugString() + ", " + r.DebugString(); }

    // Return true if item is in this Interval / false if item is not in this Interval
    bool Contain(const Item &item) const {
        if (IsEmpty()) return false;
        return l.Contain(item) && r.Contain(item);
    }

    // Return true if *this & rhs is not empty
    bool HasIntersect(const Interval &rhs) const { return !(*this & rhs).IsEmpty(); }

    Interval operator&(const Interval &rhs) const {
        if (IsEmpty() || rhs.IsEmpty()) return Interval();
        return Interval(this->l & rhs.l, this->r & rhs.r);
    }

    Interval &operator&=(const Interval &rhs) { return *this = *this & rhs; }

    // If *this and rhs can be union to single Interval:
    //      return {true, union result}
    // else:
    //      return {false, empty Interval}
    pair<bool, Interval> operator|(const Interval &rhs) const {
        // LOG(INFO) << "operator|(" << this->DebugString() << ", " << rhs.DebugString() << ")";
        if (IsEmpty()) return make_pair(false, rhs);
        if (rhs.IsEmpty()) return make_pair(false, *this);

        // Successful union in 2 situations:
        //  - has intersect
        //  - [0, 2] | (2, 5)
        if (HasIntersect(rhs) || IsOutTangentTo(rhs)) {
            return make_pair(true, Interval(l | rhs.l, r | rhs.r));
        }

        return make_pair(false, Interval());
    }

    // If *this and rhs can be union to single Interval:
    //      - change *this to union result
    //      - return true
    // else:
    //      - do not change *this
    //      - return false
    bool operator|=(const Interval &rhs) {
        auto [success, result] = *this | rhs;
        if (success) *this = result;
        return success;
    }

   private:
    void TypeAssert() const { assert((l.Type() | r.Type()) & T_UNKNOWN || l.Type() == r.Type()); }

    // Update empty
    void UpdateEmpty() {
        // Interval check
        // Empty if:
        //  - l > r
        //  - l == r && ![,]
        empty = l.Value() > r.Value() || (l.Value() == r.Value() && (!(l.IsClosed() && r.IsClosed())));
    }

    // Out tangent for interval is like (2, 7) and [7, 9]
    bool IsOutTangentTo(const Interval &rhs) const {
        if (IsEmpty() || rhs.IsEmpty()) return false;
        if (l.Value() == rhs.r.Value() && (l.IsClosed() ^ rhs.r.IsClosed())) return true;
        if (r.Value() == rhs.l.Value() && (r.IsClosed() ^ rhs.l.IsClosed())) return true;
        return false;
    }
};

#undef _define_derivative_comparison_operator_
#undef _define_derivative_assign_operator_

}  // namespace AGE
