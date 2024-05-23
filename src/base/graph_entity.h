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

#include <cstdint>
#include <string>
#include "base/math.h"

namespace AGE {

// Ensure that _edge_bit_ >= _vertex_bit_.
constexpr int _prop_bit_ = 8, _label_bit_ = _prop_bit_, _vertex_bit_ = 40, _edge_bit_ = _vertex_bit_;
constexpr uint64_t _label_mask_ = (1 << _label_bit_) - 1;

typedef uint8_t LabelId;
typedef uint8_t PropId;
constexpr LabelId ALL_LABEL = 0;
constexpr LabelId INVALID_LABEL = -1;
constexpr PropId LABEL_PID = 0;
constexpr PropId INVALID_PROP = -1;

// Graph entity (Vertex or Edge)
struct GEntity {
    uint64_t id : _vertex_bit_;
    uint64_t label : _label_bit_;
    GEntity() : id(0), label(0) {}
    GEntity(uint64_t id, LabelId label) : id(id), label(label) {}
    explicit GEntity(const uint8_t* buf) {
        constexpr uint32_t vByte = (_label_bit_ + _vertex_bit_ + 7) / 8;
        uint64_t bytes = 0;
        for (uint32_t i = 0; i < vByte; i++) bytes |= (uint64_t)buf[i] << (i * 8);
        // printf("bytes: %lu\n", bytes);
        id = bytes >> _label_bit_;
        label = bytes & _label_mask_;
    }
    uint64_t toBytes() {
        uint64_t ret = (id << _label_bit_) | label;
        // printf("toBytes(%lu|%lu): %lu\n", id, label, ret);
        return ret;
    }
    std::string DebugString() const { return std::to_string(id) + "|" + std::to_string(label); }
    bool operator==(const GEntity& rhs) const { return id == rhs.id && label == rhs.label; }
    bool operator<(const GEntity& rhs) const {
        if (label != rhs.label) return label < rhs.label;
        return id < rhs.id;
    }
};

typedef GEntity Vertex;
typedef GEntity Edge;
static_assert(sizeof(GEntity) == 8, "Struct size error");

class VertexHash {
   public:
    size_t operator()(const Vertex& v) const { return Math::hash_u64(v.id); }
};

class VertexEqual {
   public:
    bool operator()(const Vertex& lhs, const Vertex& rhs) const { return lhs.id == rhs.id; }
};
}  // namespace AGE
