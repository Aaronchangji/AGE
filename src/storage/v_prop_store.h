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

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <string>
#include <utility>
#include <vector>
#include "storage/hash_table.h"
#include "storage/layout.h"
#include "storage/seq_table.h"
#include "util/tool.h"

namespace AGE {

namespace load {

inline uint64_t VP_getKey(void *p) { return reinterpret_cast<load::VP *>(p)->v.id; }
inline size_t VP_valSize(void *p, MemPool *pool) {
    load::VP *vp = reinterpret_cast<load::VP *>(p);
    size_t size = 0;
    for (size_t i = vp->propPos; i < vp->propPos + vp->propCnt; i++) {
        size += load::propSize(&(pool->propPool[i]), pool) + sizeof(PropPtr);
    }
    return size;
}

// | prop_ptr | var_length prop |
inline std::pair<size_t, metadata_t> VP_writeVal(u8 *buf, void *p, MemPool *pool) {
    load::VP *vp = reinterpret_cast<load::VP *>(p);
    Prop *prop = &(pool->propPool[vp->propPos]);
    PropPtr *ptr = reinterpret_cast<PropPtr *>(buf);
    u8 *val = reinterpret_cast<u8 *>(ptr + vp->propCnt);
    size_t sz = 0;
    for (size_t i = 0; i < vp->propCnt; i++) {
        ptr[i].pid = prop[i].pid;
        ptr[i].index = sz;
        sz += writeProp(val + sz, prop + i, pool);
    }
    sz += vp->propCnt * sizeof(PropPtr);
    return std::make_pair(sz, metadata_t(vp->v.label, vp->propCnt));
}
};  // namespace load

// Vertex property storage
// TODO(ycli): multiple lp-store by label.
class VPropStore {
   public:
    VPropStore(const std::string &fileName, const StrMap *strMap)
        : store(fileName), vidFile(fileName + "_allVid"), strMap(strMap) {
        // cout << "file:" << fileName << ", vidFile:" << vidFile << endl;
    }

    bool load() {
        // Load allVid.
        FILE *vidf = fopen(vidFile.c_str(), "r");
        if (vidf == nullptr) return false;
        for (uint64_t label = 0; label < (1 << _label_bit_); label++) {
            size_t size;
            fread(&size, sizeof(size_t), 1, vidf);
            allVid[label].resize(size);
            fread(&allVid[label][0], sizeof(uint8_t), size, vidf);
        }
        fclose(vidf);

        return store.load();
    }

    void dump() {
        store.dump();

        // Dump allVid.
        FILE *vidf = fopen(vidFile.c_str(), "w");
        // printf("dump %s\n", vidFile.c_str());
        for (uint64_t label = 0; label < (1 << _label_bit_); label++) {
            size_t size = allVid[label].size();
            fwrite(&size, sizeof(size_t), 1, vidf);
            fwrite(&allVid[label][0], sizeof(uint8_t), size, vidf);
        }
        fclose(vidf);
    }

    // [beg, end)
    void load(load::VP *beg, load::VP *end, load::MemPool *pool) {
        assert(beg <= end);
        std::sort(beg, end, [](const load::VP &lhs, const load::VP &rhs) -> bool {
            if (lhs.v.label != rhs.v.label) return lhs.v.label < rhs.v.label;
            return lhs.v.id < rhs.v.id;
        });

        for (const load::VP *cur = beg; cur != end; cur++) writeVid(cur->v);
        store.processData(beg, sizeof(load::VP), end - beg, pool, load::VP_getKey, load::VP_valSize, load::VP_writeVal);
    }

    void load(vector<load::VP> &vp, load::MemPool *pool) { load(vp.data(), vp.data() + vp.size(), pool); }

    class VtxCursor {
       public:
        VtxCursor(VPropStore *vps, u32 lid, size_t pos, size_t cnt) : vps(vps), lid(lid), pos(pos), cnt(cnt) {}
        Vertex operator*() { return Vertex(vps->getVid(&(vps->allVid[lid][pos])), static_cast<LabelId>(lid)); }

        bool end() { return cnt == 0; }

        VtxCursor &operator++() {
            if (cnt == 0) return *this;
            constexpr size_t vs = (_vertex_bit_ + 7) / 8;
            pos += vs;
            cnt--;

            while (lid < (1 << _label_bit_) && pos >= vps->allVid[lid].size()) {
                lid++;
                pos -= vps->allVid[lid].size();
            }
            return *this;
        }

       private:
        VPropStore *vps;
        u32 lid;
        size_t pos, cnt;
    };

    VtxCursor getLabelVtxCursor(LabelId label) {
        constexpr size_t vs = (_vertex_bit_ + 7) / 8;
        return VtxCursor(this, label, 0, allVid[label].size() / vs);
    }

    void getLabelVtx(LabelId label, vector<Item> &vec, int max_num_item = 0) const {
        constexpr size_t vs = (_vertex_bit_ + 7) / 8;
        assert(allVid[label].size() % vs == 0);
        for (size_t i = 0; i < allVid[label].size(); i += vs) {
            vec.emplace_back(T_VERTEX, getVid(&allVid[label][i]), label);
            // printf("getLabelVtx: %s\n", vec.back().DebugString().c_str());
            if (max_num_item != 0) {
                if (i > max_num_item) { break; }
            }
        }
    }

    void getAllVtx(vector<Item> &vec) const {
        // uint64_t beg = Tool::getTime();
        for (uint64_t label = 0; label < (1 << _label_bit_); label++) {
            getLabelVtx(label, vec);
        }
        // printf("getAllVtx cost : %f ms\n", (Tool::getTime() - beg) / 1000.0);
    }

    Item getProp(Vertex v, PropId prop) const {
        // LOG(INFO) << strMap->DebugString();
        // LOG(INFO) << "VP::GetProp(" << v.DebugString() << ", " << (int)prop << ")";
        ItemType t = strMap->GetPropType(prop);

        // Unknown prop key.
        if (t == T_UNKNOWN) return Item(T_NULL);

        auto [propCnt, val] = store.getValWithPtrData(v.id);

        // Non-exist vertex id.
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

        // Cannot find prop key in given vertex.
        return Item(T_NULL);
    }

   private:
    HashTable store;
    std::string vidFile;
    const StrMap *strMap;
    vector<uint8_t> allVid[1 << _label_bit_];

    void writeVid(const Vertex &v) {
        // TODO(ycli): SIMD.
        constexpr size_t size = (_vertex_bit_ + 7) / 8;
        // printf("writeVid(%s)\n", v.DebugString().c_str());
        for (size_t i = 0; i < size; i++) {
            size_t shift = i * 8;
            uint8_t byte = (v.id >> shift) & 0xff;
            // printf("(id>>shift):%lu, %lu\n", v.id>>shift, 1ul*byte);
            allVid[v.label].emplace_back(byte);
        }
    }

    uint64_t getVid(const uint8_t *p) const {
        // TODO(ycli): SIMD;
        size_t size = (_vertex_bit_ + 7) / 8;
        uint64_t ret = 0;
        for (size_t i = 0; i < size; i++) {
            size_t shift = i * 8;
            ret |= (static_cast<uint64_t>(p[i]) << shift);
        }
        return ret;
    }
};
}  // namespace AGE
