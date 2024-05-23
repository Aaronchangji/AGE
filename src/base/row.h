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
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "base/item.h"
#include "base/serializer.h"
#include "base/type.h"
#include "util/config.h"
#include "util/tool.h"

namespace AGE {

using std::string;
using std::vector;

// ColIndex = -1 for compressed column of Row[compressBeginIdx, Row::size()).
// ColIndex >= 0 for Row[ColIndex].
using u32 = uint32_t;
using ColIndex = int;

constexpr ColIndex COLINDEX_COMPRESS = -1;
constexpr ColIndex COLINDEX_NONE = -2;
constexpr u32 COMPRESS_BEGIN_IDX_UNDECIDED = -1;
#define inNormalColumn(idx) (idx != COLINDEX_COMPRESS && idx != COLINDEX_NONE)

// Note: we need to ensure that the count() of every input row for a operator is non-zero,
//      which means if we get an input row with zero compress-column we assume it has no compress-column instead of
//      0-count row.

// Each row represents one result/intermediate result during our execution. The data in class::Row can be compressed for
// less memory utilization.
// [0, compressBeginIdx) for normal column, where every normal column has only 1 item.
// [compressBeginIdx, Row::size()) for compressed columns, where multiple items are compressed into one column.
class Row {
   public:
    // Parody of std::vector.
    Row(const Row& rhs) : r(rhs.r) { copyCnt(rhs); }
    Row& operator=(const Row& rhs) {
        r = rhs.r;
        copyCnt(rhs);
        return *this;
    }

    // Move will inherit cnt.
    Row(Row&& rhs) noexcept {
        rowCnt = rhs.count();
        r = std::move(rhs.r);
    }
    Row& operator=(Row&& rhs) noexcept {
        rowCnt = rhs.count();
        r = std::move(rhs.r);
        return *this;
    }

    Row() {}
    explicit Row(size_t n) : r(n) {
        if (n > 0) rowCnt = 1;
    }
    Row(const Row& rhs, size_t first, size_t last, size_t more = 0) : r(rhs.r.begin() + first, rhs.r.begin() + last) {
        if (more) resize(size() + more);
        copyCnt(rhs);
    }
    Row(const Row& rhs, const vector<int>& colIdx, size_t total) {
        CHECK(total >= colIdx.size());
        resize(total);
        int _cnt = 0;
        for (auto& idx : colIdx) {
            CHECK(idx >= 0 && idx < static_cast<int>(rhs.size()));
            r[_cnt++] = rhs[idx];
        }
        copyCnt(rhs);
    }
    // Row(vector<Item>::iterator first, vector<Item>::iterator last): r(first, last) {}

    vector<Item>::iterator begin() { return r.begin(); }
    vector<Item>::iterator end() { return r.end(); }
    vector<Item>::const_iterator begin() const { return r.begin(); }
    vector<Item>::const_iterator end() const { return r.end(); }
    void clear() { r.clear(); }
    size_t size() const { return r.size(); }
    bool empty() const { return r.empty(); }
    Item& operator[](size_t n) { return r[n]; }
    const Item& operator[](size_t n) const { return r[n]; }
    Item& at(size_t n) { return r.at(n); }
    Item& back() { return r.back(); }
    const Item& back() const { return r.back(); }
    const Item& at(size_t n) const { return r.at(n); }
    void resize(size_t n) {
        // Change cnt from 0 to 1 when size increased from 0
        size_t cnt = (empty() && n > 0) ? 1 : count();
        r.resize(n);
        setCount(cnt);
    }
    void push_back(const Item& item) { emplace_back(item); }
    void push_back(Item&& item) { emplace_back(item); }
    void pop_back() { r.pop_back(); }
    operator vector<Item>&() { return r; }

    template <class... Args>
    void emplace_back(Args&&... args) {
        // Notice that vector may copy or move to another place
        size_t cnt = count();
        r.emplace_back(std::forward<Args>(args)...);
        if (r.size() == 1)
            rowCnt = 1;
        else
            setCount(cnt);
    }
    size_t setCount(size_t cnt) {
        // if (size() == 0) Tool::printStackTrace();
        // assert(size() > 0);
        if (empty()) return cnt;
        if (cnt > ItemCountMax) {
            rowCnt = ItemCountMax;
            return cnt - ItemCountMax;
        } else {
            rowCnt = cnt;
            return 0;
        }
    }
    size_t count() const {
        if (r.empty()) return 0;
        return rowCnt;
    }
    void copyCnt(const Row& rhs) {
        if (!empty()) rowCnt = rhs.count();
    }

    string DebugString(bool print_cnt = true) const {
        string ret = "[Count: " + std::to_string(count()) + "] (";
        u32 output_counter = Config::GetInstance()->max_log_data_size_;
        for (const Item& item : r) {
            if (output_counter-- == 0) break;
            ret += item.DebugString(print_cnt) + ",";
        }
        ret.pop_back();
        ret += ")";
        return ret;
    }

    void ToString(string* s) const {
        Serializer::appendU32(s, r.size());
        Serializer::appendU64(s, count());
        for (const Item& item : r) item.ToString(s);
    }

    void FromString(const string& s, size_t& pos) {
        uint32_t size_ = Serializer::readU32(s, pos);
        uint64_t cnt = Serializer::readU64(s, pos);
        r.resize(size_);
        for (Item& item : r) {
            item.FromString(s, pos);
        }
        setCount(cnt);
    }

    static Row CreateFromString(const string& s, size_t& pos) {
        Row r;
        r.FromString(s, pos);
        return r;
    }

    inline void EmplaceWithCount(vector<Row>& dst, size_t count) const {
        while (count > ItemCountMax) {
            dst.emplace_back(*this);
            count = dst.back().setCount(count);
        }
        dst.emplace_back(std::move(*this));
        dst.back().setCount(count);
    }

    vector<Row> decompress(u32 compress_begin_index, ColIndex dst_col) const {
        vector<Row> ret;
        decompress(ret, compress_begin_index, dst_col);
        return ret;
    }
    void decompress(vector<Row>& dst, u32 compress_begin_index, ColIndex dst_col) const {
        // LOG(INFO) << compress_begin_index << ' ' << dst_col;
        // no compressed column hence no need to decompress
        if (r.size() <= compress_begin_index) {
            dst.emplace_back(*this);
            return;
        }

        Row new_r(*this, 0, compress_begin_index);
        if (dst_col == COLINDEX_COMPRESS) {
            // from compress_begin_index to compress_begin_index
            new_r.resize(compress_begin_index + 1);
            for (auto itr = r.begin() + compress_begin_index; itr != r.end(); ++itr) {
                new_r[compress_begin_index] = *itr;
                dst.emplace_back(new_r);
            }
        } else if (dst_col == COLINDEX_NONE) {
            // from compress_begin_index to rowCnt
            size_t new_count = this->count() * (r.size() - compress_begin_index);
            new_r.EmplaceWithCount(dst, new_count);
        } else {
            // from compress_begin_index to dst_col
            for (auto itr = r.begin() + compress_begin_index; itr != r.end(); ++itr) {
                new_r[dst_col] = *itr;
                dst.emplace_back(new_r);
            }
        }
    }

    static const bool Equal(const Row& lhs, const Row& rhs);
    static const u64 Hash(const Row& r);
    struct EqualFunc;
    struct HashFunc;

   private:
    std::vector<Item> r;
    // Here is the count for the whole Row. For compressed Items, they contains one their own count.
    // For the items in colNum, cnt is 1.
    size_t rowCnt;
};

/**
 * @brief Provide a comparator for 2 Row object, this is done by first checking the size of
 * 2 Row and then compare the content inside of them one by one. 2 Row object are deemed
 * as the same if and only if they have the same size and each of their counterpart items
 * are the same.
 */
inline const bool Row::Equal(const Row& lhs, const Row& rhs) {
    CHECK(lhs.size() == rhs.size());
    Item::VecEqualFunc func(lhs.size());
    return func(lhs.r.data(), rhs.r.data());
}
struct Row::EqualFunc {
    bool operator()(const Row& lhs, const Row& rhs) const { return Row::Equal(lhs, rhs); }
};

/**
 * @brief Provide a hash function for Row structure. This is done by keep appending the hash
 * value of each Item inside the Row to form a new hash value.
 */
inline const u64 Row::Hash(const Row& lhs) {
    Item::VecHashFunc func(lhs.size());
    return func(lhs.r.data());
}
struct Row::HashFunc {
    u64 operator()(const Row& lhs) const { return Row::Hash(lhs); }
};

inline void PrintRow(const Row& r) {
    for (const Item& item : r) {
        printf("%s,", item.DebugString().c_str());
    }
    printf("\n");
}
}  // namespace AGE
