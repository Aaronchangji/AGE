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
#include "storage/red_black_tree.h"

/**
 * @brief For the current design, when there is at least one AggFunc need distinct_operation, each key will corresponds
 * to a LocalDistinctor. There is only one member for the object, which is an InsertOnlySet array, whose size is the
 * same as the number of distinct AggFunc. Each AggFunc's value is stored independently. eg: When there is a key with 3
 * distinct AggFunc, then there is one LocalDistinctor which stores 3 InsertOnlySet.
 */
namespace AGE {

class LocalDistinctor {
   public:
    /**
     * @brief Construct a new Local Distinctor object
     */
    LocalDistinctor() : set() {}
    // Trivial Destructor
    ~LocalDistinctor() {}
    /**
     * @brief Check if a value is inside the set.
     *
     * @param val the value that is going to be inserted.
     * @param index the index of the distinct aggFunc.
     * @return true if and only the value was not inside the set and inserted successfully
     * @return false otherwise
     */
    bool tryToInsert(const Item& val) { return set.insert(val); }

    /**
     * @brief Current implementation of InsertOnlySet is Red Black Tree.
     */
   private:
    // Not allowed to use copy constructor & assignment for now. LocalDistinctor currently only used in
    //  AggregationOp which will only execute on the parent Node for each query.
    LocalDistinctor(const LocalDistinctor&);
    LocalDistinctor& operator=(const LocalDistinctor&);

    InsertOnlySet set;
};

}  // namespace AGE
