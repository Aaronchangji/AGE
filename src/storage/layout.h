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
#include <algorithm>
#include <fstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "base/item.h"
#include "base/type.h"
#include "storage/str_map.h"

using std::string;
using std::unordered_map;
using std::vector;

namespace AGE {
#define _ADJ_LIST_FILE_(dir) (dir + "/adj_list")
#define _V_PROP_FILE_(dir) (dir + "/v_prop")
#define _E_PROP_FILE_(dir) (dir + "/e_prop")

// Slot metadate
struct metadata_t {
    uint8_t keyData;
    uint8_t ptrData;
    metadata_t(uint8_t keyData, uint8_t ptrData) : keyData(keyData), ptrData(ptrData) {}
};

// Hash table slot
struct Slot {
    uint32_t key_;
    uint32_t key__ : 8;
    uint32_t keyData : 8;
    uint32_t ptrData : 8;
    uint32_t ptr__ : 8;
    uint32_t ptr_;
    uint64_t key() const { return static_cast<uint64_t>(key_) << 8 | key__; }
    void setKey(uint64_t k) {
        key_ = (k & 0xffffffff00) >> 8;
        key__ = k & 0xff;
    }
    void setKey(uint64_t k, uint8_t kd) {
        setKey(k);
        keyData = kd;
    }
    uint64_t ptr() const { return static_cast<uint64_t>(ptr_) << 8 | ptr__; }
    void setPtr(uint64_t p) {
        ptr_ = (p & 0xffffffff00) >> 8;
        ptr__ = p & 0xff;
    }
    void setPtr(uint64_t p, uint8_t pd) {
        setPtr(p);
        ptrData = pd;
    }
};

static_assert(sizeof(Slot) == 12, "Slot should be 12-bytes");

// A neighbor in adj list, including 1 vertex & 1 edge
struct Neighbor {
    Slot slot;
    Vertex vtx() const { return Vertex(slot.key(), slot.keyData); }
    Edge edge() const { return Edge(slot.ptr(), slot.ptrData); }
    void setVtx(Vertex v) { slot.setKey(v.id, v.label); }
    void setEdge(Edge e) { slot.setPtr(e.id, e.label); }
    void setVtx(uint64_t k, LabelId lid) { slot.setKey(k, lid); }
    void setEdge(uint64_t k, LabelId lid) { slot.setPtr(k, lid); }
    Neighbor() {}
    Neighbor(Vertex vtx, Edge edge) {
        setVtx(vtx);
        setEdge(edge);
    }
    Neighbor(uint64_t vid_, LabelId vlid_, uint64_t eid_, LabelId elid_) {
        setVtx(vid_, vlid_);
        setEdge(eid_, elid_);
    }
};

// Property pointer to memory pool in load process
struct PropPtr {
    PropId pid : _prop_bit_;
    uint32_t index : 32 - _prop_bit_;
    PropPtr(PropId pid, uint32_t index) : pid(pid), index(index) {}
    bool operator<(const PropPtr& rhs) { return pid < rhs.pid; }
};

namespace load {
// Property unit in load process
struct Prop {
    PropId pid;
    ItemType type;
    union {
        double db;
        int64_t int_;
        size_t strPos;
        bool bool_;
    };
    Prop(PropId pid, bool _bool) : pid(pid), type(T_BOOL), bool_(_bool) {}
    Prop(PropId pid, double db) : pid(pid), type(T_FLOAT), db(db) {}
    Prop(PropId pid, int64_t _int) : pid(pid), type(T_INTEGER), int_(_int) {}
    Prop(PropId pid, string s, vector<char>& strBuffer) : pid(pid), type(T_STRING) {
        strPos = strBuffer.size();
        strBuffer.resize(strBuffer.size() + s.size() + 1);
        memcpy(&strBuffer[strPos], s.c_str(), s.size() + 1);
    }
};

// A wide continuous memory pool to storage
// variable-length items (e.g. property, string)
struct MemPool {
    vector<Prop> propPool;
    vector<char> strPool;
    void clear() {
        propPool.clear();
        strPool.clear();
    }
};

inline size_t propSize(const Prop* p, const MemPool* pool) {
    switch (p->type) {
    case T_INTEGER:
        return sizeof(int64_t);
    case T_FLOAT:
        return sizeof(double);
    case T_BOOL:
        return sizeof(bool);
    case T_STRING:
        return strlen(&(pool->strPool[p->strPos])) + 1;
    default:
        return 0;
    }
}

inline size_t writeProp(u8* buf, const Prop* p, const MemPool* pool) {
    switch (p->type) {
    case T_INTEGER:
        memcpy(buf, &(p->int_), sizeof(int64_t));
        return sizeof(int64_t);
    case T_FLOAT:
        memcpy(buf, &(p->db), sizeof(double));
        return sizeof(double);
    case T_BOOL:
        memcpy(buf, &(p->bool_), sizeof(bool));
        return sizeof(bool);
    case T_STRING:
        for (size_t i = 0;; i++) {
            buf[i] = pool->strPool[i + p->strPos];
            if (pool->strPool[i + p->strPos] == '\0') return (i + 1);
        }
    default:
        return 0;
    }
}

// Vertex property in load process
struct VP {
    Vertex v;
    size_t propPos : 64 - _prop_bit_;
    PropId propCnt : _prop_bit_;
    VP(Vertex v, size_t propPos, size_t propCnt) : v(v), propPos(propPos), propCnt(propCnt) {}
};

// Edge property in load process
struct EP {
    Edge e;
    size_t propPos : 64 - _prop_bit_;
    PropId propCnt : _prop_bit_;
    EP(Edge e, size_t propPos, size_t propCnt) : e(e), propPos(propPos), propCnt(propCnt) {}
};

// Adj List in load process
typedef vector<Neighbor> AdjList;
};  // namespace load
}  // namespace AGE
