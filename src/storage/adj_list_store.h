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
#include <cstdlib>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include "base/type.h"
#include "storage/hash_table.h"
#include "storage/layout.h"

using std::pair;
using std::string;
using std::vector;

namespace AGE {
namespace load {

// Adj List of 1 vertex during loading process
struct VAdj {
    Vertex v;
    load::AdjList out, in;
    VAdj(Vertex v, load::AdjList &&out, load::AdjList &&in) : v(v), out(std::move(out)), in(std::move(in)) {}
};

inline uint64_t VAdj_getKey(void *p) { return reinterpret_cast<VAdj *>(p)->v.id; }
// adjacency_list = |out_label_pos|in_label_pos|out_label_id|in_label_id|out_nbs|in_nbs|
inline size_t VAdj_valSize(void *p, MemPool *pool) {
    load::VAdj *v = reinterpret_cast<load::VAdj *>(p);

    std::set<LabelId> inLabelSet, outLabelSet;
    for (Neighbor &nb : v->in) {
        // printf("{%lu,%lu,%u}\n", nb.vid(), nb.eid(), nb.elid());
        inLabelSet.insert(nb.edge().label);
    }
    for (Neighbor &nb : v->out) {
        // printf("{%lu,%lu,%u}\n", nb.vid(), nb.eid(), nb.elid());
        outLabelSet.insert(nb.edge().label);
    }
    // printf("inLabelCnt: %lu, outLabelCnt: %lu\n", inLabelSet.size(), outLabelSet.size());
    size_t sz = (inLabelSet.size() + outLabelSet.size()) * (sizeof(LabelId) + sizeof(uint32_t)) +
                (v->in.size() + v->out.size()) * sizeof(Neighbor);
    return sz;
}

inline size_t VAdj_writeAdj(u8 *buf, const load::AdjList &out, const load::AdjList &in) {
    constexpr u32 vByte = (_vertex_bit_ + _label_bit_ + 7) / 8, eByte = (_edge_bit_ + _label_bit_ + 7) / 8;
    size_t vPos = 0, ePos = vByte * (out.size() + in.size());
    for (const Neighbor &nb : out) {
        u64 vBytes = nb.vtx().toBytes(), eBytes = nb.edge().toBytes();
        for (u32 i = 0; i < vByte; vBytes >>= 8, i++) buf[vPos++] = vBytes & 0xff;
        for (u32 i = 0; i < eByte; eBytes >>= 8, i++) buf[ePos++] = eBytes & 0xff;
    }
    for (const Neighbor &nb : in) {
        u64 vBytes = nb.vtx().toBytes(), eBytes = nb.edge().toBytes();
        for (u32 i = 0; i < vByte; vBytes >>= 8, i++) buf[vPos++] = vBytes & 0xff;
        for (u32 i = 0; i < eByte; eBytes >>= 8, i++) buf[ePos++] = eBytes & 0xff;
    }

    /*
    for (u32 i = 0; i < ePos; i += vByte) {
        Vertex v(buf + i);
        printf("write(%s)\n", v.DebugString().c_str());
    }
    */
    assert(vPos == vByte * (out.size() + in.size()) && ePos == (vByte + eByte) * (out.size() + in.size()));
    return ePos;
}

inline pair<size_t, metadata_t> VAdj_writeVal(u8 *buf, void *p, MemPool *pool) {
    load::VAdj *v = reinterpret_cast<load::VAdj *>(p);
    // printf("writeVal(%s)\n", v->v.DebugString().c_str());

    std::sort(v->in.begin(), v->in.end(), [](const Neighbor &lhs, const Neighbor &rhs) {
        if (lhs.edge().label != rhs.edge().label) return lhs.edge().label < rhs.edge().label;
        return lhs.vtx().id < rhs.vtx().id;
    });
    std::sort(v->out.begin(), v->out.end(), [](const Neighbor &lhs, const Neighbor &rhs) {
        if (lhs.edge().label != rhs.edge().label) return lhs.edge().label < rhs.edge().label;
        return lhs.vtx().id < rhs.vtx().id;
    });

    vector<uint32_t> inLabelPos, outLabelPos;
    vector<LabelId> inLabelId, outLabelId;
    // printf("load Adj(%lu): out[", v->v.id);
    for (size_t i = 0; i < v->out.size(); i++) {
        // Neighbor& nb = v->out[i];
        // printf("{%s|%s} ", nb.vtx().DebugString().c_str(), nb.edge().DebugString().c_str());
        LabelId nxt = i == v->out.size() - 1 ? 0 : v->out[i + 1].edge().label;
        if (nxt != v->out[i].edge().label) {
            outLabelPos.emplace_back(i + 1);
            outLabelId.emplace_back(v->out[i].edge().label);
        }
    }
    // printf("], in[");
    uint32_t outEdgeCnt = v->out.size();
    for (size_t i = 0; i < v->in.size(); i++) {
        // Neighbor& nb = v->in[i];
        // printf("{%s|%s} ", nb.vtx().DebugString().c_str(), nb.edge().DebugString().c_str());
        LabelId nxt = i == v->in.size() - 1 ? 0 : v->in[i + 1].edge().label;
        if (nxt != v->in[i].edge().label) {
            inLabelPos.emplace_back(i + 1 + outEdgeCnt);
            inLabelId.emplace_back(v->in[i].edge().label);
        }
    }
    // printf("]\n");

    // printf("Write:inLabelCnt:%lu, outLabelCnt:%lu\n",inLabelPos.size(),outLabelPos.size());
    size_t sz = 0;
    memcpy(buf + sz, outLabelPos.data(), outLabelPos.size() * sizeof(uint32_t));
    sz += outLabelPos.size() * sizeof(uint32_t);
    memcpy(buf + sz, inLabelPos.data(), inLabelPos.size() * sizeof(uint32_t));
    sz += inLabelPos.size() * sizeof(uint32_t);
    memcpy(buf + sz, outLabelId.data(), outLabelId.size() * sizeof(LabelId));
    sz += outLabelId.size() * sizeof(LabelId);
    memcpy(buf + sz, inLabelId.data(), inLabelId.size() * sizeof(LabelId));
    sz += inLabelId.size() * sizeof(LabelId);

    // printf("write out\n");
    sz += VAdj_writeAdj(buf + sz, v->out, v->in);

    /*
    memcpy(buf + sz, &v->out[0], v->out.size() * sizeof(Neighbor));
    sz += v->out.size() * sizeof(Neighbor);
    memcpy(buf + sz, &v->in[0], v->in.size() * sizeof(Neighbor));
    sz += v->in.size() * sizeof(Neighbor);
    */

    // Clear memory.
    load::AdjList().swap(v->in);
    load::AdjList().swap(v->out);
    return std::make_pair(sz, metadata_t(outLabelPos.size(), inLabelPos.size()));
}
};  // namespace load

// Adjacency list storage
class AdjListStore {
   public:
    explicit AdjListStore(const std::string &fileName) : hashTable(fileName) {}
    bool load() { return hashTable.load(); }
    void dump() { hashTable.dump(); }

    void load(load::VAdj *beg, load::VAdj *end, load::MemPool *pool) {
        assert(beg <= end);
        hashTable.processData(beg, sizeof(load::VAdj), end - beg, pool, load::VAdj_getKey, load::VAdj_valSize,
                              load::VAdj_writeVal);
    }

    void load(vector<load::VAdj> &vAdj, load::MemPool *pool) { load(vAdj.data(), vAdj.data() + vAdj.size(), pool); }

#define _ADJ_LIST_OUT_EDGE_CNT_(val, outLabelCnt) (outLabelCnt ? reinterpret_cast<uint32_t *>(val)[outLabelCnt - 1] : 0)
#define _ADJ_LIST_EDGE_CNT_(val, outLabelCnt, inLabelCnt) \
    ((inLabelCnt + outLabelCnt) ? reinterpret_cast<uint32_t *>(val)[inLabelCnt + outLabelCnt - 1] : 0)

    void printAdjList(Vertex v) {
        constexpr u32 vByte = (_vertex_bit_ + _label_bit_ + 7) / 8, eByte = (_edge_bit_ + _label_bit_ + 7) / 8;
        auto [data, val] = hashTable.getValWithData(v.id);
        printf("store Adj(%lu): out[", v.id);
        if (val == nullptr) {
            printf("]\n");
            return;
        }
        LabelId outLabelCnt = data.keyData, inLabelCnt = data.ptrData;
        uint32_t beg = 0, end1 = _ADJ_LIST_OUT_EDGE_CNT_(val, outLabelCnt),
                 end2 = _ADJ_LIST_EDGE_CNT_(val, outLabelCnt, inLabelCnt);
        /*
        Neighbor *nb =
            reinterpret_cast<Neighbor *>(val + (inLabelCnt + outLabelCnt) * (sizeof(uint32_t) + sizeof(LabelId)));
        */
        u8 *nb = val + (inLabelCnt + outLabelCnt) * (sizeof(uint32_t) + sizeof(LabelId)),
           *edge = nb + vByte * _ADJ_LIST_EDGE_CNT_(val, outLabelCnt, inLabelCnt);
        for (uint32_t i = beg; i < end1; i++) {
            printf("{%s,%s} ", Vertex(nb + i * vByte).DebugString().c_str(),
                   Edge(edge + i * eByte).DebugString().c_str());
            // printf("{%s|%s} ", nb[i].vtx().DebugString().c_str(), nb[i].edge().DebugString().c_str());
        }
        printf("], in[");
        for (uint32_t i = end1; i < end2; i++) {
            printf("{%s,%s} ", Vertex(nb + i * vByte).DebugString().c_str(),
                   Edge(edge + i * eByte).DebugString().c_str());
            // printf("{%s|%s} ", nb[i].vtx().DebugString().c_str(), nb[i].edge().DebugString().c_str());
        }
        printf("]\n");
    }

    u32 getEdgeCnt(Vertex v, LabelId label, DirectionType dir) {
        auto [data, val] = hashTable.getValWithData(v.id);
        if (val == nullptr) return 0;
        LabelId outLabelCnt = data.keyData, inLabelCnt = data.ptrData;

        u32 ret = 0;
        if (dir != DirectionType_OUT) {
            auto [beg, end] = getInAdjPos(v, label, outLabelCnt, inLabelCnt, val);
            ret += end - beg;
        }
        if (dir != DirectionType_IN) {
            auto [beg, end] = getOutAdjPos(v, label, outLabelCnt, inLabelCnt, val);
            ret += end - beg;
        }

        return ret;
    }

    pair<uint32_t, uint32_t> getOutAdjPos(Vertex v, LabelId label, LabelId outLabelCnt, LabelId inLabelCnt, u8 *val) {
        uint32_t beg, end;
        if (label == ALL_LABEL) {
            beg = 0;
            end = _ADJ_LIST_OUT_EDGE_CNT_(val, outLabelCnt);
        } else {
            uint32_t *outLabelPos = reinterpret_cast<uint32_t *>(val);
            LabelId *outLabelId = reinterpret_cast<LabelId *>(outLabelPos + outLabelCnt + inLabelCnt);
            LabelId pos = std::lower_bound(outLabelId, outLabelId + outLabelCnt, label) - outLabelId;
            if (pos < outLabelCnt && outLabelId[pos] == label) {
                beg = (pos == 0) ? 0 : outLabelPos[pos - 1];
                end = outLabelPos[pos];
            } else {
                beg = end = 0;
            }
        }
        return std::make_pair(beg, end);
    }

    pair<uint32_t, uint32_t> getInAdjPos(Vertex v, LabelId label, LabelId outLabelCnt, LabelId inLabelCnt, u8 *val) {
        uint32_t beg, end;
        if (label == ALL_LABEL) {
            beg = _ADJ_LIST_OUT_EDGE_CNT_(val, outLabelCnt);
            end = _ADJ_LIST_EDGE_CNT_(val, outLabelCnt, inLabelCnt);
        } else {
            uint32_t *inLabelPos = reinterpret_cast<uint32_t *>(val) + outLabelCnt;
            LabelId *inLabelId = reinterpret_cast<LabelId *>(inLabelPos + inLabelCnt) + outLabelCnt;
            LabelId pos = std::lower_bound(inLabelId, inLabelId + inLabelCnt, label) - inLabelId;
            if (pos < inLabelCnt && inLabelId[pos] == label) {
                beg = (pos == 0) ? _ADJ_LIST_OUT_EDGE_CNT_(val, outLabelCnt) : inLabelPos[pos - 1];
                end = inLabelPos[pos];
            } else {
                beg = end = 0;
            }
        }
        return std::make_pair(beg, end);
    }

    void getNeighbor(Vertex v, LabelId eLabel, DirectionType dir, vector<Item> &result, LabelId dstVLabel) {
        constexpr u32 vByte = (_vertex_bit_ + _label_bit_ + 7) / 8;
        // printAdjList(v);
        auto [data, val] = hashTable.getValWithData(v.id);
        if (val == nullptr || dstVLabel == INVALID_LABEL) return;

        LabelId outLabelCnt = data.keyData, inLabelCnt = data.ptrData;
        /*
        Neighbor *nb =
            reinterpret_cast<Neighbor *>(val + (inLabelCnt + outLabelCnt) * (sizeof(uint32_t) + sizeof(LabelId)));
        */
        u8 *nb = val + (inLabelCnt + outLabelCnt) * (sizeof(uint32_t) + sizeof(LabelId));
        if (dir != DirectionType_OUT) {
            auto [beg, end] = getInAdjPos(v, eLabel, outLabelCnt, inLabelCnt, val);
            if (dstVLabel == ALL_LABEL) {
                for (uint32_t i = beg; i < end; i++) result.emplace_back(T_VERTEX, static_cast<Vertex>(nb + i * vByte));
            } else {
                for (uint32_t i = beg; i < end; i++) {
                    Item item = Item(T_VERTEX, static_cast<Vertex>(nb + i * vByte));
                    if (item.vertex.label == dstVLabel) result.emplace_back(item);
                }
            }
        }
        if (dir != DirectionType_IN) {
            auto [beg, end] = getOutAdjPos(v, eLabel, outLabelCnt, inLabelCnt, val);
            if (dstVLabel == ALL_LABEL) {
                for (uint32_t i = beg; i < end; i++) result.emplace_back(T_VERTEX, static_cast<Vertex>(nb + i * vByte));
            } else {
                for (uint32_t i = beg; i < end; i++) {
                    Item item = Item(T_VERTEX, static_cast<Vertex>(nb + i * vByte));
                    if (item.vertex.label == dstVLabel) result.emplace_back(item);
                }
            }
        }
    }

   private:
    HashTable hashTable;
};
}  // namespace AGE
