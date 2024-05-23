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
#include <functional>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>
#include "base/item.h"
#include "base/proj_util.h"
#include "base/row.h"
#include "base/type.h"

using std::vector;
namespace AGE {
/**
 * @brief This class store the Row objects while maintain their distinction.
 * Mainly used in barrier_projection_op
 */
class GlobalDistinctor {
   public:
    GlobalDistinctor() : storageSet(16) {}
    ~GlobalDistinctor() {}
    explicit GlobalDistinctor(int n, int limitNum = LimitUtil::WITHOUT_LIMIT) : storageSet(n), limitNum(limitNum) {}
    GlobalDistinctor(const GlobalDistinctor& aDistinctor) {
        for (auto contentRow : aDistinctor.storageSet) this->storageSet.insert(contentRow);
    }
    GlobalDistinctor(GlobalDistinctor&& aDistinctor) {
        for (auto contentRow : aDistinctor.storageSet) this->storageSet.insert(std::move(contentRow));
    }
    GlobalDistinctor& operator=(const GlobalDistinctor& aDistinctor) {
        for (auto contentRow : aDistinctor.storageSet) this->storageSet.insert(contentRow);
        return *this;
    }
    GlobalDistinctor& operator=(GlobalDistinctor&& aDistinctor) {
        for (auto contentRow : aDistinctor.storageSet) this->storageSet.insert(std::move(contentRow));
        return *this;
    }
    /**
     * @brief Check if a Row object is in the distinctor or not. If it is new, then a copy will be
     * inserted.
     *
     * @param aRow the Row object that is going to be tested.
     */
    bool insert(const Row& aRow) { return storageSet.insert(aRow).second; }
    /**
     * @brief Check if a Row object is in the distinctor or not. If it is new, then the obeject will be moved
     * and inserted.
     *
     * @param aRow the Row object that is going to be tested.
     */
    bool insert(Row&& aRow) { return storageSet.insert(std::move(aRow)).second; }

    /**
     * @brief This function is used to return the Row data inside the global distinctor in the form of vector<Row>
     *
     */
    vector<Row> getResult() {
        vector<Row> newData;
        LimitUtil limit(limitNum);

        for (auto itr : storageSet) {
            if (!limit.checkCnt()) break;
            newData.emplace_back(std::move(itr));
            if (!limit.updateCnt(newData.back())) break;
        }

        return newData;
    }

   private:
    std::unordered_set<Row, Row::HashFunc, Row::EqualFunc> storageSet;
    int limitNum;
};

}  // namespace AGE
