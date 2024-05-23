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

#include <string>
#include <utility>
#include <vector>
#include "storage/hash_table.h"
#include "storage/layout.h"
#include "storage/seq_table.h"

namespace AGE {
namespace load {
inline uint64_t EP_getKey(void *p) { return reinterpret_cast<load::EP *>(p)->e.id; }
inline size_t EP_valSize(void *p, MemPool *pool) {
    load::EP *ep = reinterpret_cast<load::EP *>(p);
    size_t size = 0;
    for (size_t i = ep->propPos; i < ep->propPos + ep->propCnt; i++) {
        size += load::propSize(&(pool->propPool[i]), pool) + sizeof(PropPtr);
    }
    return size;
}
inline std::pair<size_t, metadata_t> EP_writeVal(u8 *buf, void *p, MemPool *pool) {
    load::EP *ep = reinterpret_cast<load::EP *>(p);
    Prop *prop = &(pool->propPool[ep->propPos]);
    PropPtr *ptr = reinterpret_cast<PropPtr *>(buf);
    u8 *val = reinterpret_cast<u8 *>(ptr + ep->propCnt);
    size_t sz = 0;
    for (size_t i = 0; i < ep->propCnt; i++) {
        ptr[i].pid = prop[i].pid;
        ptr[i].index = sz;
        sz += writeProp(val + sz, prop + i, pool);
    }
    sz += ep->propCnt * sizeof(PropPtr);
    return std::make_pair(sz, metadata_t(ep->e.label, ep->propCnt));
}
};  // namespace load

class EPropStore {
   public:
    EPropStore(const std::string &fileName, const StrMap *strMap) : store(fileName), strMap(strMap) {}
    bool load() { return store.load(); }
    void dump() { store.dump(); }

    void load(load::EP *beg, load::EP *end, load::MemPool *pool) {
        assert(beg <= end);
        store.processData(beg, sizeof(load::EP), end - beg, pool, load::EP_getKey, load::EP_valSize, load::EP_writeVal);
    }

    void load(vector<load::EP> &ep, load::MemPool *pool) { load(ep.data(), ep.data() + ep.size(), pool); }

    Item getProp(Edge e, PropId prop) const {
        ItemType t = strMap->GetPropType(prop);

        // Unknown prop key.
        if (t == T_UNKNOWN) return Item(T_NULL);

        auto [propCnt, val] = store.getValWithPtrData(e.id);

        // Non-exist edge id.
        if (val == nullptr) return Item(T_NULL);

        PropPtr *pptr = reinterpret_cast<PropPtr *>(val);
        u8 *propVal = reinterpret_cast<u8 *>(pptr + propCnt);
        PropId pos = std::lower_bound(pptr, pptr + propCnt, PropPtr(prop, 0)) - pptr;
        if (pos < propCnt && pptr[pos].pid == prop) {
            switch (t) {
            case T_FLOAT:
                return Item(t, reinterpret_cast<double *>(propVal + pptr[pos].index)[0]);
            case T_INTEGER:
                return Item(t, reinterpret_cast<int64_t *>(propVal + pptr[pos].index)[0]);
            case T_BOOL:
                return Item(t, reinterpret_cast<bool *>(propVal + pptr[pos].index)[0]);
            case T_STRING:
                return Item(T_STRINGVIEW, reinterpret_cast<char *>(propVal + pptr[pos].index));
            default:
                assert(false);
            }
        }

        // Cannot find prop key in given edge.
        return Item(T_NULL);
    }

   private:
    HashTable store;
    const StrMap *strMap;
};
}  // namespace AGE
