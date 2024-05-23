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
#include <cstdint>
#include <string>
#include <vector>
#include "base/item.h"
#include "base/type.h"
#include "storage/adj_list_store.h"
#include "storage/e_prop_store.h"
#include "storage/index_store.h"
#include "storage/layout.h"
#include "storage/v_prop_store.h"
#include "util/tool.h"

using std::string;

namespace AGE {
// AGE graph
class Graph {
   public:
    explicit Graph(const string& dirName)
        : indexStore(new IndexStore()),
          vPropStore(_V_PROP_FILE_(dirName), &strMap),
          ePropStore(_E_PROP_FILE_(dirName), &strMap),
          adjListStore(_ADJ_LIST_FILE_(dirName)) {
        if (!strMap.load(_STR_MAP_FILE_(dirName))) {
            LOG(ERROR) << "Graph schema file wrong on reading: " << _STR_MAP_FILE_(dirName);
            exit(-1);
        }
        // LOG(INFO) << strMap.DebugString();
    }

    ~Graph() { delete indexStore; }

    bool load() {
        // if (!strMap.load(_STR_MAP_FILE_(dirName)) return false;
        LOG(INFO) << strMap.DebugString() << std::flush;
        if (!vPropStore.load()) return false;
        if (!ePropStore.load()) return false;
        if (!adjListStore.load()) return false;
        return true;
    }
    Item getProp(const Item& entity, PropId pid) const {
        // LOG(INFO) << strMap.DebugString();
        // LOG(INFO) << "GetProp(" << entity.DebugString() << ", " << (int)pid << ")";
        if (entity.type == T_VERTEX)
            return vPropStore.getProp(entity.vertex, pid);
        else if (entity.type == T_EDGE)
            return ePropStore.getProp(entity.edge, pid);
        // assert(false && "Graph::getProp() first param is not vertex or edge.");
        return Item(T_NULL);
    }

    u32 getNeighborCnt(const Item& entity, LabelId label, DirectionType dir) {
        // assert(entity.type == T_VERTEX && "Graph::getNeighborCnt() first param is not vertex or edge.");
        if (entity.type != T_VERTEX) return 0;
        return adjListStore.getEdgeCnt(entity.vertex, label, dir);
    }

    vector<Item> getNeighbor(const Item& entity, LabelId label, DirectionType dir, LabelId dstVLabel = ALL_LABEL) {
        // assert(entity.type == T_VERTEX && "Graph::getNeighbor() first param is not vertex.");
        if (entity.type == T_NULL)
            return vector<Item>{Item(T_NULL)};  // T_NULL should return null item, rather than empty result

        vector<Item> ret;
        if (entity.type != T_VERTEX) return ret;
        getNeighbor(entity, label, dir, ret, dstVLabel);
        return ret;
    }

    void getNeighbor(const Item& entity, LabelId label, DirectionType dir, vector<Item>& result,
                     LabelId dstVLabel = ALL_LABEL) {
        // assert(entity.type == T_VERTEX && "Graph::getNeighbor() first param is not vertex.");
        if (entity.type == T_NULL) {
            result.emplace_back(Item(T_NULL));
            return;
        } else if (entity.type != T_VERTEX) {
            CHECK(false) << "Graph::getNeighbor() first param is not vertex.";
        }
        adjListStore.getNeighbor(entity.vertex, label, dir, result, dstVLabel);
    }

    VPropStore::VtxCursor getLabelVtxCursor(LabelId label) { return vPropStore.getLabelVtxCursor(label); }
    void getAllVtx(std::vector<Item>& vec) { vPropStore.getAllVtx(vec); }
    std::vector<Item> getAllVtx() const {
        std::vector<Item> vec;
        vPropStore.getAllVtx(vec);
        return vec;
    }
    void getLabelVtx(LabelId label, std::vector<Item>& vec, int max_num_items = 0) { vPropStore.getLabelVtx(label, vec, max_num_items); }
    std::vector<Item> getLabelVtx(LabelId label) {
        std::vector<Item> vec;
        vPropStore.getLabelVtx(label, vec);
        return vec;
    }

    LabelId getLabelId(const string& s) const { return strMap.GetLabelId(s); }
    string getLabelStr(LabelId label) const { return strMap.GetLabelStr(label); }
    PropId getPropId(const string& s) const { return strMap.GetPropId(s); }
    string getPropStr(PropId prop) const { return strMap.GetPropStr(prop); }
    ItemType getPropType(PropId prop) const { return strMap.GetPropType(prop); }
    const StrMap* getStrMap() const { return &strMap; }
    IndexStore* GetIndexStore() { return indexStore; }

   private:
    IndexStore* indexStore;
    // const StrMap& strMap;
    StrMap strMap;
    VPropStore vPropStore;
    EPropStore ePropStore;
    AdjListStore adjListStore;
};
}  // namespace AGE
