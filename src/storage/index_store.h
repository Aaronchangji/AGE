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

#include <stdint.h>
#include <atomic>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>
#include "base/intervals.h"
#include "base/serializer.h"
#include "base/type.h"
#include "storage/b_tree.h"
#include "storage/graph.h"
#include "storage/layout.h"

namespace AGE {
using std::make_pair;
using std::map;
using std::pair;
using std::string;

// Index storage
class IndexStore {
   public:
    IndexStore() {}

    void SetIndex(LabelId lid, PropId pid, BTree* bt, vector<GEntity>&& nullVec) {
        std::lock_guard<std::shared_mutex> lock(GetMutex(lid, pid));
        Index& index = GetIndex(lid, pid);
        index.Set(bt, std::move(nullVec));
        index.Enable();
    }

    ItemType GetIndexType(LabelId lid, PropId pid) {
        shared_guard lock(GetMutex(lid, pid));
        return GetIndex(lid, pid).KeyType();
    }

    bool IsEnable(LabelId lid, PropId pid) {
        shared_guard lock(GetMutex(lid, pid));
        return GetIndex(lid, pid).IsEnable();
    }

    bool IsBuilt(LabelId lid, PropId pid) {
        shared_guard lock(GetMutex(lid, pid));
        return GetIndex(lid, pid).GetBTree() != nullptr;
    }

    void DisableIndex(LabelId lid, PropId pid) {
        std::lock_guard<std::shared_mutex> lock(GetMutex(lid, pid));
        GetIndex(lid, pid).Disable();
    }

    void EnableIndex(LabelId lid, PropId pid) {
        std::lock_guard<std::shared_mutex> lock(GetMutex(lid, pid));
        GetIndex(lid, pid).Enable();
    }

    vector<pair<BTree::iterator, BTree::iterator>> IndexQuery(LabelId labelId, PropId propId,
                                                              const Intervals& intervals) {
        shared_guard lock(GetMutex(labelId, propId));

        BTree* tree = GetIndex(labelId, propId).GetBTree();
        vector<pair<BTree::iterator, BTree::iterator>> ret;

        for (const Interval& interval : intervals.GetIntervals()) {
            assert(interval.Type() == tree->keyType() || interval.Type() == T_UNKNOWN);
            const LBoundary& L = interval.GetLeftBoundary();
            const RBoundary& R = interval.GetRightBoundary();

            // Equal
            if (L.Value() == R.Value() && L.IsClosed() && R.IsClosed()) {
                ret.emplace_back(tree->equal_range(L.Value()));
                continue;
            }

            BTree::iterator beg = L.Type() == T_UNKNOWN ? tree->begin()
                                  : L.IsClosed()        ? tree->lower_bound(L.Value())
                                                        : tree->upper_bound(L.Value());
            BTree::iterator end = R.Type() == T_UNKNOWN ? tree->end()
                                  : R.IsClosed()        ? tree->upper_bound(R.Value())
                                                        : tree->lower_bound(R.Value());
            ret.emplace_back(beg, end);
        }

        return ret;
    }

    string DebugString(StrMap* strMap = nullptr) {
        string ret = "Index status: {";
        for (auto& p : index) {
            auto [lid, pid] = p.first;
            Index& index = p.second;
            string keyStr = "\n(";
            if (strMap != nullptr) {
                keyStr += strMap->GetLabelStr(lid) + ", " + strMap->GetPropStr(pid);
            } else {
                keyStr += to_string(lid) + ", " + to_string(pid);
            }
            keyStr += ")";

            string valStr = index.IsEnable() ? "Enable" : "Disable";

            ret += keyStr + " : " + valStr;
        }
        ret += "\n}\n";
        return ret;
    }

   private:
    // TODO(ycli): volatile?
    class Index {
       private:
        bool isEnable;
        BTree* tree;
        vector<GEntity> nullVec;
        std::shared_mutex mu;

       public:
        Index() : isEnable(false), tree(nullptr), nullVec(0) {}
        ~Index() { delete tree; }

        BTree* GetBTree() { return tree; }

        void Set(BTree* bt, vector<GEntity>&& nullVec_) {
            tree = bt;
            nullVec = std::move(nullVec_);
        }

        bool IsEnable() { return isEnable; }
        void Enable() {
            assert(tree);
            isEnable = true;
        }

        void Disable() { isEnable = false; }
        ItemType KeyType() { return tree ? tree->keyType() : T_UNKNOWN; }
        std::shared_mutex& GetMutex() { return mu; }
    };

    class shared_guard {
       public:
        explicit shared_guard(std::shared_mutex& mutex) : mutex(mutex) { mutex.lock_shared(); }
        ~shared_guard() { mutex.unlock_shared(); }

       private:
        std::shared_mutex& mutex;
    };

    Index& GetIndex(LabelId lid, PropId pid) { return index[make_pair(lid, pid)]; }
    std::shared_mutex& GetMutex(LabelId lid, PropId pid) { return GetIndex(lid, pid).GetMutex(); }

    map<pair<LabelId, PropId>, Index> index;
};

}  // namespace AGE
