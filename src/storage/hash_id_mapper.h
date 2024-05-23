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

#include <vector>
#include "base/graph_entity.h"
#include "base/item.h"
#include "base/math.h"
#include "base/node.h"
#include "base/type.h"
#include "storage/abstract_id_mapper.h"

namespace AGE {
class HashIdMapper : public AbstractIdMapper {
   public:
    static constexpr u32 seed = 1e9 + 7;
    explicit HashIdMapper(const Nodes& cacheNodes) : worldSize(cacheNodes.getWorldSize()) {}
    ~HashIdMapper() {}

    // Vertex/Edge Id -> Rank(cache worker index)
    int GetRank(const Item& item) const override {
        // assert((item.type == T_EDGE || item.type == T_VERTEX) && "IdMapper::GetRank() only accept vertex/edge
        // input");
        if (item.type == T_NULL) {
            return INVALID_RANK;
        }
        CHECK(item.type == T_EDGE || item.type == T_VERTEX)
            << "IdMapper::GetRank() only accept vertex/edge input, but got " << ItemType_DebugString(item.type);
        uint64_t id = item.type == T_VERTEX ? item.vertex.id : item.edge.id;
        return Math::hash_u64(id, seed) % worldSize;
    }
    int GetRank(const GEntity& ge) const override {
        // printf("hash value: %lu, worldSize: %lu\n", Math::hash_u64(ge.id, seed), worldSize);
        return Math::hash_u64(ge.id, seed) % worldSize;
    }

   private:
    int worldSize, localRank;
};
}  // namespace AGE
