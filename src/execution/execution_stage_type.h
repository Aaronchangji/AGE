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
#include <vector>
#include <bitset>
#include <string>

#include "base/math.h"
#include "base/serializer.h"
#include "model/util.h"

namespace AGE {

using std::vector;
using std::string;
using MLModel::OpModelType;

class ExecutionStageType {
   public:
    ExecutionStageType() : stage_type_(0) {}
    ~ExecutionStageType() {}

    inline bool operator== (const ExecutionStageType& other) const {
        return stage_type_ == other.stage_type_;
    }

    void set_single(OpModelType type) {
        int idx = static_cast<int>(type);
        stage_type_.set(idx);
    }

    void set(vector<OpModelType> types) {
        for (auto& optype : types) {
            set_single(optype);
        }
    }

    bool is_empty() {
        return stage_type_.none();
    }

    uint8_t value() const { return stage_type_.to_ulong(); }

    string DebugString() const {
        string s = "Execution Stage Type:";
        s += " [";
        if (stage_type_.test(0)) s += "Expand, ";
        if (stage_type_.test(1)) s += "Filter, ";
        if (stage_type_.test(2)) s += "Property, ";
        if (stage_type_.test(3)) s += "LocalAgg, ";
        if (stage_type_.test(4)) s += "Project, ";
        if (stage_type_.test(5)) s += "GlobalAgg, ";
        if (stage_type_.test(6)) s += "FlowControl, ";
        if (stage_type_.test(7)) s += "Scan, ";
        s += "]";
        return s;
    }

    void ToString(string* s) const {
        Serializer::appendU32(s, stage_type_.to_ulong());
    }

    void FromString(const string& s, size_t& pos) {
        stage_type_ = std::bitset<8>(Serializer::readU32(s, pos));
    }

   private:
    // [Expand, Filter, Property, LocalAgg, Project, GlobalAgg, FlowControl, Scan]
    std::bitset<8> stage_type_;
};

struct ExecutionStageTypeHash {
    size_t operator()(const ExecutionStageType& k) const {
        uint8_t v = k.value();
        return Math::MurmurHash64_x64(&v, sizeof(v));
    }
};

struct ExecutionStageTypeCompare {
    static size_t hash(const ExecutionStageType& key) {
        uint8_t value = key.value();
        return Math::MurmurHash64_x64(&value, 1);
    }

    static bool equal(const ExecutionStageType& lhs, const ExecutionStageType& rhs) { return lhs == rhs; }
};

class InterExecutionStageType {
   public:
    InterExecutionStageType() {}
    InterExecutionStageType(ExecutionStageType parent, ExecutionStageType child) : parent_(parent), child_(child) {}

    void set_parent(ExecutionStageType parent) { parent_ = parent; }
    void set_child(ExecutionStageType child) { child_ = child; }

    inline bool operator== (const InterExecutionStageType& other) const {
        return parent_ == other.parent_ && child_ == other.child_;
    }

    void ToString(string* s) const {
        parent_.ToString(s);
        child_.ToString(s);
    }

    void FromString(const string& s, size_t& pos) {
        parent_.FromString(s, pos);
        child_.FromString(s, pos);
    }

    ExecutionStageType& parent() { return parent_; }
    ExecutionStageType& child() { return child_; }

    uint16_t value() const {
        uint16_t value = parent_.value();
        value = (value << 8) | child_.value();
        return value;
    }

    string DebugString() const {
        string s = "Inter Execution Stage Type: ";
        s += "Parent: " + parent_.DebugString() + " Child: " + child_.DebugString();
        return s;
    }

   private:
    ExecutionStageType parent_;
    ExecutionStageType child_;
};

struct InterExecutionStageTypeHash {
    size_t operator()(const InterExecutionStageType& k) const {
        uint16_t v = k.value();
        return Math::MurmurHash64_x64(&v, sizeof(v));
    }
};

struct InterExecutionStageTypeCompare {
    static size_t hash(const InterExecutionStageType& key) {
        uint16_t value = key.value();
        return Math::MurmurHash64_x64(&value, sizeof(uint16_t));
    }

    static bool equal(const InterExecutionStageType& lhs, const InterExecutionStageType& rhs) { return lhs == rhs; }
};

}  // namespace AGE