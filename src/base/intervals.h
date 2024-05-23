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

#include <base/interval.h>
#include <base/item.h>
#include <set>
#include <string>
#include <utility>

namespace AGE {

using std::set;
using std::string;
class Expression;

// The collection represented by an Intervals is the union of all Interval in {set<int> intervals}
class Intervals {
   private:
    // - We guarantee that "all the Interval's in {set<int> intervals} do not intersect with each other"
    //      and this property as "Interval Independence"
    // - If intervals.empty(), means empty Intervals that contains nothing
    set<Interval> intervals;

    // To avoid invalid {intervals}, only use these two constructors in this class
    explicit Intervals(const set<Interval> &intervals) : intervals(intervals) {}
    explicit Intervals(set<Interval> &&intervals) : intervals(std::move(intervals)) {}

   public:
    string DebugString() const {
        string ret = "(";
        for (const Interval &interval : intervals) {
            ret += interval.DebugString() + ", ";
        }
        ret += ")";
        return ret;
    }

    // Default empty
    Intervals() {}

    ItemType Type() const {
        assert(TypeCheck());
        return intervals.size() ? intervals.begin()->Type() : T_UNKNOWN;
    }

    void ToString(string *s) const {
        Serializer::appendU32(s, intervals.size());
        for (const Interval &interval : intervals) interval.ToString(s);
    }

    static Intervals CreateFromString(const string &s, size_t &pos) {
        Intervals ret;
        ret.FromString(s, pos);
        return ret;
    }

    void FromString(const string &s, size_t &pos) {
        intervals.clear();
        u32 sz = Serializer::readU32(s, pos);
        while (sz--) {
            intervals.emplace(Interval::CreateFromString(s, pos));
        }
    }

    explicit Intervals(const Expression &expr);

    const set<Interval> &GetIntervals() const { return intervals; }

    bool operator==(const Intervals &rhs) const {
        if (intervals.size() != rhs.intervals.size()) return false;
        for (auto itrX = intervals.begin(), itrY = rhs.intervals.begin(); itrX != intervals.begin(); itrX++, itrY++) {
            if (*itrX != *itrY) return false;
        }
        return true;
    }

    // Complement
    Intervals operator~() const {
        set<Interval> newIntervals;

        // Empty
        if (intervals.size() == 0) {
            newIntervals.emplace(Interval(LBoundary(), RBoundary()));
            return Intervals(std::move(newIntervals));
        }

        // Universe
        if (Type() == T_UNKNOWN) {
            return Intervals();
        }

        auto prev = intervals.begin(), cur = std::next(intervals.begin());
        const LBoundary &leftMostLB = prev->GetLeftBoundary();
        if (leftMostLB.Type() != T_UNKNOWN) {
            LBoundary l(Item(T_UNKNOWN), false);
            RBoundary r(leftMostLB.Value(), leftMostLB.IsClosed() ^ true);
            newIntervals.emplace(std::move(l), std::move(r));
        }

        for (; cur != intervals.end(); ++cur, ++prev) {
            const RBoundary &prevRB = prev->GetRightBoundary();
            const LBoundary &curLB = cur->GetLeftBoundary();
            LBoundary l(prevRB.Value(), prevRB.IsClosed() ^ true);
            RBoundary r(curLB.Value(), curLB.IsClosed() ^ true);
            newIntervals.emplace(std::move(l), std::move(r));
        }

        const RBoundary &rightMostRB = prev->GetRightBoundary();
        if (rightMostRB.Type() != T_UNKNOWN) {
            LBoundary l(rightMostRB.Value(), rightMostRB.IsClosed() ^ true);
            RBoundary r(Item(T_UNKNOWN), false);
            newIntervals.emplace(std::move(l), std::move(r));
        }

        return Intervals(std::move(newIntervals));
    }

    // Modify this to the intersection of two Intervals by:
    //  1. Create a new empty set<Interval> called {newIntervals}.
    //  2. Iterate over all Interval pairs(A, B) where A from this Intervals, B from rhs Intervals. For each pair, do:
    //     - If A is intersect with B, add their intersection into {newIntervals}
    //  3. Swap {this->intervals} and {newIntervals}
    // Since we guarantee the "Interval Independence" and the orderliness by {set<Interval> intervals},
    //  we could implement the process mentioned above by two pointers(i.e. {itrX} & {itrY}) method
    Intervals &operator&=(const Intervals &rhs) { return *this = *this & rhs; }
    Intervals operator&(const Intervals &rhs) const {
        TypeAssert(*this, rhs);

        set<Interval> newIntervals;
        auto itrX = intervals.begin(), itrY = rhs.intervals.begin();
        while (itrX != intervals.end() && itrY != rhs.intervals.end()) {
            Interval intersection = *itrX & *itrY;

            // *itrX and *itrY have valid intersection
            if (!intersection.IsEmpty()) newIntervals.insert(intersection);

            // We keep the Interval whose right boundary is larger
            // and forward the pointer of the other one to continue
            itrX->GetRightBoundary() > itrY->GetRightBoundary() ? itrY++ : itrX++;
        }

        return Intervals(newIntervals);
    }

    // Modify this to the union of two Intervals by:
    //  1. Create a new empty set<Interval> called {newIntervals}.
    //  2. Firstly we insert the Interval with smallest left boundary among {this->intervals} and {rhs.intervals} into
    //  {newIntervals}
    //  3. For every time we have an interval pair(A, B) such that A is the smallest Interval in {this->intervals}
    //      and B is the smallest Interval in {rhs.intervals}, do this until {this->intervals} or {rhs.intervals} is
    //      empty:
    //      - Try to union the largest Interval C in {newInterval} with A or B
    //          - If it succeeds:
    //             - Replace C by the union result
    //             - Delete successfully union input A or B
    //             - Update A, B to new smallest Interval in their original {intervals}
    //          - If it fails:
    //             - Insert the Interval with smaller left boundary among A & B into {newIntervals}
    //  4. Insert remaining Interval in {this->intervals} & {rhs.intervals} into {newIntervals}
    //  5. Swap {this->intervals} and {newIntervals}
    // Since we guarantee the "Interval Independence" and the orderliness by {set<Interval> intervals},
    //  we could implement the process mentioned above by two pointers(i.e. {itrX} & {itrY}) method
    Intervals &operator|=(const Intervals &rhs) { return *this = *this | rhs; }
    Intervals operator|(const Intervals &rhs) const {
        TypeAssert(*this, rhs);

        // Resolve empty corner case
        if (rhs.intervals.size() == 0) return *this;
        if (intervals.size() == 0) return rhs;

        set<Interval> newIntervals;
        auto itrX = intervals.begin(), itrY = rhs.intervals.begin();
        newIntervals.insert(itrX->GetLeftBoundary() < itrY->GetLeftBoundary() ? *(itrX++) : *(itrY++));
        while (itrX != intervals.end() || itrY != rhs.intervals.end()) {
            Interval cur = *newIntervals.rbegin();

            // Sucess to union
            bool success = false;

            // We greedily union A, B as much as possible in one iteration
            //  to avoid meaningless std::set::erase() & std::set::insert()
            while (itrX != intervals.end() && (cur |= *itrX)) itrX++, success |= true;
            while (itrY != rhs.intervals.end() && (cur |= *itrY)) itrY++, success |= true;

            if (success) {
                newIntervals.erase(std::prev(newIntervals.end()));
                newIntervals.insert(cur);
            } else if (itrX == intervals.end() && itrY == rhs.intervals.end()) {
                // *this and rhs is empty
                break;
            } else if (itrX == intervals.end()) {
                // *this is empty
                newIntervals.insert(*(itrY++));
            } else if (itrY == rhs.intervals.end()) {
                // rhs is empty
                newIntervals.insert(*(itrX++));
            } else {
                // None is empty
                newIntervals.insert(itrX->GetLeftBoundary() < itrY->GetLeftBoundary() ? *(itrX++) : *(itrY++));
            }
        }

        // Insert remaining Interval's
        // while(itrX != intervals.end()) newIntervals.insert(*(itrX++));
        // while(itrY != rhs.intervals.end()) newIntervals.insert(*(itrY++));

        return Intervals(newIntervals);
    }

   private:
    static void TypeAssert(const Intervals &lhs, const Intervals &rhs) {
        // LOG(INFO) << "TypeAssert(\n" << lhs.DebugString() << ",\n" << rhs.DebugString() << "\n)";
        assert(lhs.Type() == rhs.Type() || ((lhs.Type() | rhs.Type()) & T_UNKNOWN));
    }

    bool TypeCheck() const {
        ItemType type = T_UNKNOWN;
        for (const Interval &interval : intervals) {
            if (interval.Type() == T_UNKNOWN && intervals.size() != 1ull) return false;
            if (type == T_UNKNOWN) {
                type = interval.Type();
            } else if (interval.Type() != type) {
                return false;
            }
        }
        return true;
    }
};

}  // namespace AGE
